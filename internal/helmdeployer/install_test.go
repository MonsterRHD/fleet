package helmdeployer

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"testing"
	"time"

	fleet "github.com/rancher/fleet/pkg/apis/fleet.cattle.io/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"helm.sh/helm/v4/pkg/action"
	"helm.sh/helm/v4/pkg/kube"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"

	"github.com/rancher/fleet/internal/manifest"
)

func TestValuesFrom(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)
	key := "values.yaml"
	newline := "\n"
	if runtime.GOOS == "windows" {
		newline = "\r\n"
	}

	configMapPayload := fmt.Sprintf("replication: \"true\"%sreplicas: \"2\"%sserviceType: NodePort", newline, newline)
	secretPayload := fmt.Sprintf("replication: \"false\"%sreplicas: \"3\"%sserviceType: NodePort%sfoo: bar", newline, newline, newline)
	totalValues := map[string]any{"beforeMerge": "value"}
	expected := map[string]any{
		"beforeMerge": "value",
		"replicas":    "2",
		"replication": "true",
		"serviceType": "NodePort",
		"foo":         "bar",
	}

	configMapName := "configmap-name"
	configMapNamespace := "configmap-namespace"
	configMapValues, err := valuesFromConfigMap(configMapName, configMapNamespace, key, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: configMapNamespace,
		},
		Data: map[string]string{
			key: configMapPayload,
		},
	})
	r.NoError(err)

	secretName := "secret-name"
	secretNamespace := "secret-namespace"
	secretValues, err := valuesFromSecret(secretName, secretNamespace, key, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: secretNamespace,
		},
		Data: map[string][]byte{
			key: []byte(secretPayload),
		},
	})
	r.NoError(err)

	totalValues = mergeValues(totalValues, secretValues)
	totalValues = mergeValues(totalValues, configMapValues)
	a.Equal(expected, totalValues)
}

func TestAtomicMapsToRollbackOnFailure(t *testing.T) {
	a := assert.New(t)
	h := &Helm{}

	tests := []struct {
		name     string
		atomic   bool
		expected bool
	}{
		{
			name:     "atomic true maps to RollbackOnFailure true",
			atomic:   true,
			expected: true,
		},
		{
			name:     "atomic false maps to RollbackOnFailure false",
			atomic:   false,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			installAction := &action.Install{}
			h.configureInstallAction(
				installAction,
				&action.Configuration{},
				"test-release",
				"test-namespace",
				time.Duration(0),
				fleet.BundleDeploymentOptions{
					Helm: &fleet.HelmOptions{
						Atomic: tt.atomic,
					},
				},
				nil,
				dryRunConfig{DryRun: false},
			)
			a.Equal(tt.expected, installAction.RollbackOnFailure, "Install: Atomic should map to RollbackOnFailure")

			upgradeAction := &action.Upgrade{}
			h.configureUpgradeAction(
				upgradeAction,
				"test-namespace",
				time.Duration(0),
				fleet.BundleDeploymentOptions{
					Helm: &fleet.HelmOptions{
						Atomic: tt.atomic,
					},
				},
				nil,
				dryRunConfig{DryRun: false},
			)
			a.Equal(tt.expected, upgradeAction.RollbackOnFailure, "Upgrade: Atomic should map to RollbackOnFailure")
		})
	}
}

func TestForceMapsToForceReplace(t *testing.T) {
	a := assert.New(t)
	h := &Helm{}

	tests := []struct {
		name     string
		force    bool
		expected bool
	}{
		{
			name:     "force true maps to ForceReplace true",
			force:    true,
			expected: true,
		},
		{
			name:     "force false maps to ForceReplace false",
			force:    false,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upgradeAction := &action.Upgrade{}
			h.configureUpgradeAction(
				upgradeAction,
				"test-namespace",
				time.Duration(0),
				fleet.BundleDeploymentOptions{
					Helm: &fleet.HelmOptions{
						Force: tt.force,
					},
				},
				nil,
				dryRunConfig{DryRun: false},
			)
			a.Equal(tt.expected, upgradeAction.ForceReplace, "Force should map to ForceReplace")
		})
	}
}

func TestDryRunStrategyMapping(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		name            string
		template        bool
		dryRun          bool
		dryRunOption    string
		expectedInstall action.DryRunStrategy
		expectedUpgrade action.DryRunStrategy
		description     string
	}{
		// Normal execution cases
		{
			name:            "no dry run and no template mode",
			template:        false,
			dryRun:          false,
			dryRunOption:    "",
			expectedInstall: action.DryRunNone,
			expectedUpgrade: action.DryRunNone,
			description:     "Normal execution without dry run",
		},
		{
			name:            "client dry run without template mode",
			template:        false,
			dryRun:          true,
			dryRunOption:    "",
			expectedInstall: action.DryRunClient,
			expectedUpgrade: action.DryRunClient,
			description:     "Client-side dry run for validation",
		},
		{
			name:            "server dry run without template mode",
			template:        false,
			dryRun:          true,
			dryRunOption:    "server",
			expectedInstall: action.DryRunServer,
			expectedUpgrade: action.DryRunServer,
			description:     "Server-side dry run to enable lookup functions",
		},
		// Template mode cases (always uses DryRunClient)
		{
			name:            "template mode without dry run",
			template:        true,
			dryRun:          false,
			dryRunOption:    "",
			expectedInstall: action.DryRunClient,
			expectedUpgrade: action.DryRunClient,
			description:     "Template mode always uses DryRunClient to prevent cluster interaction",
		},
		{
			name:            "template mode overrides server dry run",
			template:        true,
			dryRun:          true,
			dryRunOption:    "server",
			expectedInstall: action.DryRunClient,
			expectedUpgrade: action.DryRunClient,
			description:     "Template mode takes precedence over server dry run configuration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Helm{
				template: tt.template,
			}

			// Test Install action
			installAction := &action.Install{}
			h.configureInstallAction(
				installAction,
				&action.Configuration{},
				"test-release",
				"test-namespace",
				time.Duration(0),
				fleet.BundleDeploymentOptions{
					Helm: &fleet.HelmOptions{},
				},
				nil,
				dryRunConfig{
					DryRun:       tt.dryRun,
					DryRunOption: tt.dryRunOption,
				},
			)
			a.Equal(tt.expectedInstall, installAction.DryRunStrategy,
				"Install: %s - expected DryRunStrategy=%v, got %v",
				tt.description, tt.expectedInstall, installAction.DryRunStrategy)

			// Test Upgrade action
			upgradeAction := &action.Upgrade{}
			h.configureUpgradeAction(
				upgradeAction,
				"test-namespace",
				time.Duration(0),
				fleet.BundleDeploymentOptions{
					Helm: &fleet.HelmOptions{},
				},
				nil,
				dryRunConfig{
					DryRun:       tt.dryRun,
					DryRunOption: tt.dryRunOption,
				},
			)
			a.Equal(tt.expectedUpgrade, upgradeAction.DryRunStrategy,
				"Upgrade: %s - expected DryRunStrategy=%v, got %v",
				tt.description, tt.expectedUpgrade, upgradeAction.DryRunStrategy)
		})
	}
}

func TestWaitStrategyConfiguration(t *testing.T) {
	a := assert.New(t)
	h := &Helm{}

	tests := []struct {
		name            string
		timeout         time.Duration
		expectedInstall kube.WaitStrategy
		expectedUpgrade kube.WaitStrategy
	}{
		{
			name:            "no timeout uses HookOnlyStrategy",
			timeout:         0,
			expectedInstall: kube.HookOnlyStrategy,
			expectedUpgrade: kube.HookOnlyStrategy,
		},
		{
			name:            "with timeout uses StatusWatcherStrategy",
			timeout:         5 * time.Minute,
			expectedInstall: kube.StatusWatcherStrategy,
			expectedUpgrade: kube.StatusWatcherStrategy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			installAction := &action.Install{}
			h.configureInstallAction(
				installAction,
				&action.Configuration{},
				"test-release",
				"test-namespace",
				tt.timeout,
				fleet.BundleDeploymentOptions{
					Helm: &fleet.HelmOptions{},
				},
				nil,
				dryRunConfig{DryRun: false},
			)
			a.Equal(tt.expectedInstall, installAction.WaitStrategy, "Install: WaitStrategy should be set based on timeout")

			upgradeAction := &action.Upgrade{}
			h.configureUpgradeAction(
				upgradeAction,
				"test-namespace",
				tt.timeout,
				fleet.BundleDeploymentOptions{
					Helm: &fleet.HelmOptions{},
				},
				nil,
				dryRunConfig{DryRun: false},
			)
			a.Equal(tt.expectedUpgrade, upgradeAction.WaitStrategy, "Upgrade: WaitStrategy should be set based on timeout")
		})
	}
}

func TestServerSideApplyConfiguration(t *testing.T) {
	a := assert.New(t)
	h := &Helm{}

	t.Run("Install with TakeOwnership disables ServerSideApply", func(t *testing.T) {
		installAction := &action.Install{}
		h.configureInstallAction(
			installAction,
			&action.Configuration{},
			"test-release",
			"test-namespace",
			time.Duration(0),
			fleet.BundleDeploymentOptions{
				Helm: &fleet.HelmOptions{
					TakeOwnership: true,
				},
			},
			nil,
			dryRunConfig{DryRun: false},
		)
		a.False(installAction.ServerSideApply, "ServerSideApply should be false when TakeOwnership is true")
	})

	t.Run("Install without TakeOwnership keeps ServerSideApply enabled", func(t *testing.T) {
		installAction := &action.Install{}
		h.configureInstallAction(
			installAction,
			&action.Configuration{},
			"test-release",
			"test-namespace",
			time.Duration(0),
			fleet.BundleDeploymentOptions{
				Helm: &fleet.HelmOptions{
					TakeOwnership: false,
				},
			},
			nil,
			dryRunConfig{DryRun: false},
		)
		a.True(installAction.ServerSideApply, "ServerSideApply should be true when TakeOwnership and ForceReplace are false")
	})

	t.Run("Upgrade uses auto mode for ServerSideApply", func(t *testing.T) {
		upgradeAction := &action.Upgrade{}
		h.configureUpgradeAction(
			upgradeAction,
			"test-namespace",
			time.Duration(0),
			fleet.BundleDeploymentOptions{
				Helm: &fleet.HelmOptions{},
			},
			nil,
			dryRunConfig{DryRun: false},
		)
		a.Equal("auto", upgradeAction.ServerSideApply, "Upgrade should use 'auto' mode for ServerSideApply")
	})
}

func TestCorrectDriftForceOption(t *testing.T) {
	a := assert.New(t)
	h := &Helm{}

	t.Run("CorrectDrift.Force is combined with Helm.Force", func(t *testing.T) {
		tests := []struct {
			name          string
			helmForce     bool
			driftForce    bool
			expectedForce bool
		}{
			{
				name:          "both false",
				helmForce:     false,
				driftForce:    false,
				expectedForce: false,
			},
			{
				name:          "helm true, drift false",
				helmForce:     true,
				driftForce:    false,
				expectedForce: true,
			},
			{
				name:          "helm false, drift true",
				helmForce:     false,
				driftForce:    true,
				expectedForce: true,
			},
			{
				name:          "both true",
				helmForce:     true,
				driftForce:    true,
				expectedForce: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				upgradeAction := &action.Upgrade{}
				h.configureUpgradeAction(
					upgradeAction,
					"test-namespace",
					time.Duration(0),
					fleet.BundleDeploymentOptions{
						Helm: &fleet.HelmOptions{
							Force: tt.helmForce,
						},
						CorrectDrift: &fleet.CorrectDrift{
							Force: tt.driftForce,
						},
					},
					nil,
					dryRunConfig{DryRun: false},
				)
				a.Equal(tt.expectedForce, upgradeAction.ForceReplace, "ForceReplace should combine Helm.Force and CorrectDrift.Force")
			})
		}
	})
}

func TestIsInDownstreamResources(t *testing.T) {
	a := assert.New(t)

	opts := fleet.BundleDeploymentOptions{
		DownstreamResources: []fleet.DownstreamResource{
			{Kind: "ConfigMap", Name: "my-config"},
			{Kind: "Secret", Name: "some-secret"},
		},
	}

	// function returns only a boolean indicating membership for a kind

	found := isInDownstreamResources("my-config", "ConfigMap", opts)
	a.True(found, "expected to find my-config in DownstreamResources")

	found2 := isInDownstreamResources("not-present", "ConfigMap", opts)
	a.False(found2, "expected not to find not-present in DownstreamResources")

	found3 := isInDownstreamResources("my-config", "SomeOtherKind", opts)
	a.False(found3, "expected not to find my-config of kind SomeOtherKind in DownstreamResources")

	found4 := isInDownstreamResources("some-secret", "Secret", opts)
	a.True(found4, "expected to find some-secret in DownstreamResources")

	found5 := isInDownstreamResources("not-present", "Secret", opts)
	a.False(found5, "expected not to find not-present in DownstreamResources")

	// Test case-insensitive kind matching for the parameter
	found6 := isInDownstreamResources("my-config", "configmap", opts)
	a.True(found6, "expected to find my-config with lowercase kind 'configmap'")

	found7 := isInDownstreamResources("my-config", "CONFIGMAP", opts)
	a.True(found7, "expected to find my-config with uppercase kind 'CONFIGMAP'")

	found8 := isInDownstreamResources("some-secret", "secret", opts)
	a.True(found8, "expected to find some-secret with lowercase kind 'secret'")

	found9 := isInDownstreamResources("some-secret", "SECRET", opts)
	a.True(found9, "expected to find some-secret with uppercase kind 'SECRET'")

	found10 := isInDownstreamResources("my-config", "CoNfIgMaP", opts)
	a.True(found10, "expected to find my-config with mixed case kind 'CoNfIgMaP'")

	found11 := isInDownstreamResources("not-present", "configmap", opts)
	a.False(found11, "expected not to find not-present even with lowercase kind")

	// Test case-insensitive kind matching for the DownstreamResources Kind field
	optsLowercaseKind := fleet.BundleDeploymentOptions{
		DownstreamResources: []fleet.DownstreamResource{
			{Kind: "configmap", Name: "my-config-lower"},
			{Kind: "secret", Name: "some-secret-lower"},
		},
	}

	found12 := isInDownstreamResources("my-config-lower", "ConfigMap", optsLowercaseKind)
	a.True(found12, "expected to find my-config-lower when DownstreamResource has lowercase 'configmap'")

	found13 := isInDownstreamResources("my-config-lower", "CONFIGMAP", optsLowercaseKind)
	a.True(found13, "expected to find my-config-lower with uppercase parameter and lowercase DownstreamResource kind")

	found14 := isInDownstreamResources("some-secret-lower", "Secret", optsLowercaseKind)
	a.True(found14, "expected to find some-secret-lower when DownstreamResource has lowercase 'secret'")

	optsUppercaseKind := fleet.BundleDeploymentOptions{
		DownstreamResources: []fleet.DownstreamResource{
			{Kind: "CONFIGMAP", Name: "my-config-upper"},
			{Kind: "SECRET", Name: "some-secret-upper"},
		},
	}

	found15 := isInDownstreamResources("my-config-upper", "ConfigMap", optsUppercaseKind)
	a.True(found15, "expected to find my-config-upper when DownstreamResource has uppercase 'CONFIGMAP'")

	found16 := isInDownstreamResources("my-config-upper", "configmap", optsUppercaseKind)
	a.True(found16, "expected to find my-config-upper with lowercase parameter and uppercase DownstreamResource kind")

	found17 := isInDownstreamResources("some-secret-upper", "secret", optsUppercaseKind)
	a.True(found17, "expected to find some-secret-upper with lowercase parameter and uppercase DownstreamResource kind")

	optsMixedKind := fleet.BundleDeploymentOptions{
		DownstreamResources: []fleet.DownstreamResource{
			{Kind: "CoNfIgMaP", Name: "my-config-mixed"},
		},
	}

	found18 := isInDownstreamResources("my-config-mixed", "ConfigMap", optsMixedKind)
	a.True(found18, "expected to find my-config-mixed when DownstreamResource has mixed case 'CoNfIgMaP'")

	found19 := isInDownstreamResources("my-config-mixed", "configmap", optsMixedKind)
	a.True(found19, "expected to find my-config-mixed with lowercase parameter and mixed case DownstreamResource kind")
}

func TestValuesFromUsesDefaultNamespaceWhenResourceCopiedDownstream(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	// default namespace where the Helm release lives
	defaultNS := "helm-default"

	// Create a ConfigMap and Secret in the default namespace which should be picked
	// when the valuesFrom reference is part of DownstreamResources.
	cm := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm-down", Namespace: defaultNS},
		Data:       map[string]string{DefaultKey: "cmVal: cmDefault"},
	}

	sec := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "sec-down", Namespace: defaultNS},
		Data:       map[string][]byte{DefaultKey: []byte("secVal: secDefault")},
	}

	kubeClient := kubernetesfake.NewSimpleClientset(&cm, &sec)
	h := &Helm{template: false}

	opts := fleet.BundleDeploymentOptions{
		Helm: &fleet.HelmOptions{
			ValuesFrom: []fleet.ValuesFrom{
				{ConfigMapKeyRef: &fleet.ConfigMapKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "cm-down"}}},
				{SecretKeyRef: &fleet.SecretKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "sec-down"}, Namespace: "ignored-ns"}},
			},
		},
		// copied resources: meaning these exist in the defaultNamespace of the release
		DownstreamResources: []fleet.DownstreamResource{{Kind: "ConfigMap", Name: "cm-down"}, {Kind: "Secret", Name: "sec-down"}},
	}

	vals, err := h.getValues(context.TODO(), opts, defaultNS, kubeClient)
	r.NoError(err)

	// configmap and secret data should have been read from defaultNS
	a.Equal("cmDefault", vals["cmVal"])
	a.Equal("secDefault", vals["secVal"])
}

func TestValuesFromUsesProvidedNamespaceWhenNotCopiedDownstream(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	// namespaces
	defaultNS := "helm-default"
	providedNS := "explicit-ns"

	// ConfigMap present only in providedNS
	cmProvided := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm-provided", Namespace: providedNS},
		Data:       map[string]string{DefaultKey: "cmVal: cmProvided"},
	}

	kubeClient := kubernetesfake.NewSimpleClientset(&cmProvided)
	h := &Helm{template: false}

	opts := fleet.BundleDeploymentOptions{
		Helm: &fleet.HelmOptions{
			ValuesFrom: []fleet.ValuesFrom{{ConfigMapKeyRef: &fleet.ConfigMapKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "cm-provided"}, Namespace: providedNS}}},
		},
		// DownstreamResources does NOT list cm-provided — so the provided namespace should be used
		DownstreamResources: []fleet.DownstreamResource{{Kind: "ConfigMap", Name: "some-other"}},
	}

	vals, err := h.getValues(context.TODO(), opts, defaultNS, kubeClient)
	r.NoError(err)
	a.Equal("cmProvided", vals["cmVal"])
}

func TestInstallActionCorrectDriftForce(t *testing.T) {
	a := assert.New(t)
	h := &Helm{}
	testCfg := &action.Configuration{}

	tests := []struct {
		name                    string
		helmForce               bool
		driftForce              bool
		takeOwnership           bool
		expectedForceReplace    bool
		expectedServerSideApply bool
	}{
		{
			name:                    "both false, no ownership",
			helmForce:               false,
			driftForce:              false,
			takeOwnership:           false,
			expectedForceReplace:    false,
			expectedServerSideApply: true,
		},
		{
			name:                    "helm force true",
			helmForce:               true,
			driftForce:              false,
			takeOwnership:           false,
			expectedForceReplace:    true,
			expectedServerSideApply: false,
		},
		{
			name:                    "drift force true",
			helmForce:               false,
			driftForce:              true,
			takeOwnership:           false,
			expectedForceReplace:    true,
			expectedServerSideApply: false,
		},
		{
			name:                    "both forces true",
			helmForce:               true,
			driftForce:              true,
			takeOwnership:           false,
			expectedForceReplace:    true,
			expectedServerSideApply: false,
		},
		{
			name:                    "take ownership disables server-side apply",
			helmForce:               false,
			driftForce:              false,
			takeOwnership:           true,
			expectedForceReplace:    false,
			expectedServerSideApply: false,
		},
		{
			name:                    "take ownership with force",
			helmForce:               true,
			driftForce:              false,
			takeOwnership:           true,
			expectedForceReplace:    true,
			expectedServerSideApply: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			installAction := &action.Install{}
			opts := fleet.BundleDeploymentOptions{
				Helm: &fleet.HelmOptions{
					Force:         tt.helmForce,
					TakeOwnership: tt.takeOwnership,
				},
			}
			if tt.driftForce {
				opts.CorrectDrift = &fleet.CorrectDrift{
					Force: tt.driftForce,
				}
			}

			h.configureInstallAction(
				installAction,
				testCfg,
				"test-release",
				"test-namespace",
				time.Duration(0),
				opts,
				nil,
				dryRunConfig{DryRun: false},
			)

			a.Equal(tt.expectedForceReplace, installAction.ForceReplace,
				"ForceReplace should be set correctly based on Helm.Force and CorrectDrift.Force")
			a.Equal(tt.expectedServerSideApply, installAction.ServerSideApply,
				"ServerSideApply should be disabled when ForceReplace or TakeOwnership is true")
		})
	}
}

// TestTemplate_ToJsonFlowStyle is a regression test for the Helm v4 + kyaml
// issue where JSON output from a toJson template function is round-tripped
// through kyaml (annotateAndMerge), which converts it into YAML flow-style
// with unquoted keys, e.g. {apiVersion: v1, kind: ConfigMap}.
// k8s.io/apimachinery then mistakes the leading '{' for JSON and passes the
// document through unchanged, causing json.Unmarshal to fail.
// The user-visible error looks like:
//
//	error while running post render on files: invalid character 'a'
func TestTemplate_ToJsonFlowStyle(t *testing.T) {
	m := &manifest.Manifest{
		Resources: []fleet.BundleResource{
			{
				Name:    "test-chart/Chart.yaml",
				Content: "apiVersion: v2\nname: test-chart\nversion: 0.1.0\ntype: application\n",
			},
			{
				// Template that outputs a raw JSON document via toJson.
				// Helm v4's kyaml converts this to flow-style YAML with unquoted
				// keys before Fleet's post-renderer sees it.
				Name:    "test-chart/templates/configmap.yaml",
				Content: "{{- toJson (dict \"apiVersion\" \"v1\" \"kind\" \"ConfigMap\" \"metadata\" (dict \"name\" \"json-output\") \"data\" (dict \"key\" \"value\")) }}\n",
			},
		},
	}
	opts := fleet.BundleDeploymentOptions{
		DefaultNamespace: "default",
		Helm:             &fleet.HelmOptions{Chart: "test-chart"},
	}
	_, err := Template(context.Background(), "test-bundle", m, opts, "v1.25.0")
	require.NoError(t, err)
}

func TestTemplate_LeadingComment(t *testing.T) {
	m := &manifest.Manifest{
		Resources: []fleet.BundleResource{
			{
				Name:    "test-chart/Chart.yaml",
				Content: "apiVersion: v2\nname: test-chart\nversion: 0.1.0\ntype: application\n",
			},
			{
				Name:    "test-chart/templates/configmap.yaml",
				Content: "{{- if .Values.configmap.enabled }}\n# ---------< SOME LABEL >---------\nkind: ConfigMap\napiVersion: v1\nmetadata:\n  name: some-envvar\n  namespace: {{ .Release.Namespace }}\ndata:\n  EXAMPLE_KEY: \"example-value\"\n{{- end }}\n",
			},
		},
	}
	opts := fleet.BundleDeploymentOptions{
		DefaultNamespace: "default",
		Helm: &fleet.HelmOptions{
			Chart: "test-chart",
			Values: &fleet.GenericMap{Data: map[string]any{
				"configmap": map[string]any{"enabled": true},
			}},
		},
	}
	release, err := Template(context.Background(), "test-bundle", m, opts, "v1.25.0")
	require.NoError(t, err)
	require.Contains(t, release.Manifest, "kind: ConfigMap")
}

// TestGetValuesMultipleSourcesMergedInDeclarationOrder verifies that several
// valuesFrom sources are merged in their declaration order: keys from later
// sources override earlier ones, nested maps are deep merged and scalars or
// slices replace the earlier value. Within one valuesFrom entry the secret is
// still merged after the configmap (fleet <= 0.6.0 compatibility).
func TestGetValuesMultipleSourcesMergedInDeclarationOrder(t *testing.T) {
	const ns = "merge-ns"

	cmFirst := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm-first", Namespace: ns},
		Data: map[string]string{
			DefaultKey: "shared:\n  a: from-cm-first\n  b: cm-first-only\nscalar: cm-first\nlist:\n  - cm-first\nonlyFirst: first-only\n",
		},
	}
	secSecond := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "sec-second", Namespace: ns},
		Data: map[string][]byte{
			DefaultKey: []byte("shared:\n  a: from-sec-second\n  c: sec-second-only\nscalar: sec-second\nlist:\n  - sec-second\n"),
		},
	}
	cmThird := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm-third", Namespace: ns},
		Data: map[string]string{
			DefaultKey: "shared:\n  a: from-cm-third\nscalar: cm-third\nonlyThird: true\n",
		},
	}
	// One entry references both a configmap and a secret. The secret must win.
	cmEntry := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm-entry", Namespace: ns},
		Data:       map[string]string{DefaultKey: "entry: from-configmap\nfromConfigMap: from-configmap\n"},
	}
	secEntry := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "sec-entry", Namespace: ns},
		Data:       map[string][]byte{DefaultKey: []byte("entry: from-secret\nfromSecret: from-secret\n")},
	}

	kubeClient := kubernetesfake.NewSimpleClientset(cmFirst, secSecond, cmThird, cmEntry, secEntry)
	h := &Helm{}

	opts := fleet.BundleDeploymentOptions{
		Helm: &fleet.HelmOptions{
			Values: &fleet.GenericMap{Data: map[string]any{
				"base":   true,
				"scalar": "inline",
				"shared": map[string]any{"baseKey": "keep"},
			}},
			ValuesFrom: []fleet.ValuesFrom{
				{ConfigMapKeyRef: &fleet.ConfigMapKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "cm-first"}, Namespace: ns}},
				{SecretKeyRef: &fleet.SecretKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "sec-second"}, Namespace: ns}},
				{ConfigMapKeyRef: &fleet.ConfigMapKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "cm-third"}, Namespace: ns}},
				{
					ConfigMapKeyRef: &fleet.ConfigMapKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "cm-entry"}, Namespace: ns},
					SecretKeyRef:    &fleet.SecretKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "sec-entry"}, Namespace: ns},
				},
			},
		},
	}

	values, err := h.getValues(context.TODO(), opts, "default-ns", kubeClient)
	require.NoError(t, err)

	expected := map[string]any{
		"base":          true,
		"onlyFirst":     "first-only",
		"onlyThird":     true,
		"fromConfigMap": "from-configmap",
		"fromSecret":    "from-secret",
		// last source in declaration order wins
		"scalar": "cm-third",
		// the secret of the final entry overrides its configmap
		"entry": "from-secret",
		// slices are replaced, not concatenated
		"list": []any{"sec-second"},
		"shared": map[string]any{
			"baseKey": "keep",
			"b":       "cm-first-only",
			"c":       "sec-second-only",
			"a":       "from-cm-third",
		},
	}
	assert.Equal(t, expected, values)
}

// TestGetValuesMultipleSourcesMissingOrEmptyWaits verifies that with several
// valuesFrom sources, missing objects, missing keys and empty documents do not
// fail the deployment: they are all reported as pending, while the values
// resolved from the sources that are present (merged in declaration order) are
// retained. Once the sources recover, the next pass merges everything without
// touching the options, i.e. it keeps producing the same deployment revision.
func TestGetValuesMultipleSourcesMissingOrEmptyWaits(t *testing.T) {
	const ns = "wait-ns"

	cmPresent := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm-present", Namespace: ns},
		Data:       map[string]string{DefaultKey: "present: present-value\nshared:\n  a: present\n"},
	}
	secNoKey := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "sec-nokey", Namespace: ns},
		Data:       map[string][]byte{"other-key": []byte("unused")},
	}
	cmEmpty := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm-empty", Namespace: ns},
		Data:       map[string]string{DefaultKey: ""},
	}
	secBlank := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "sec-blank", Namespace: ns},
		Data:       map[string][]byte{DefaultKey: []byte("  \n")},
	}
	cmLater := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm-later", Namespace: ns},
		Data:       map[string]string{DefaultKey: "later: true"},
	}
	// "cm-missing" deliberately does not exist.

	kubeClient := kubernetesfake.NewSimpleClientset(cmPresent, secNoKey, cmEmpty, secBlank, cmLater)
	h := &Helm{}

	opts := fleet.BundleDeploymentOptions{
		Helm: &fleet.HelmOptions{
			Values: &fleet.GenericMap{Data: map[string]any{"inline": "kept"}},
			ValuesFrom: []fleet.ValuesFrom{
				{ConfigMapKeyRef: &fleet.ConfigMapKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "cm-present"}, Namespace: ns}},
				{ConfigMapKeyRef: &fleet.ConfigMapKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "cm-missing"}, Namespace: ns}},
				{SecretKeyRef: &fleet.SecretKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "sec-nokey"}, Namespace: ns}},
				{ConfigMapKeyRef: &fleet.ConfigMapKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "cm-empty"}, Namespace: ns}},
				{SecretKeyRef: &fleet.SecretKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "sec-blank"}, Namespace: ns}},
				{ConfigMapKeyRef: &fleet.ConfigMapKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "cm-later"}, Namespace: ns}},
			},
		},
	}

	values, err := h.getValues(context.TODO(), opts, "default-ns", kubeClient)
	var waitErr *WaitingForValuesFromError
	require.ErrorAs(t, err, &waitErr)

	// already resolved values are not cleared
	assert.Equal(t, map[string]any{
		"inline":  "kept",
		"present": "present-value",
		"shared":  map[string]any{"a": "present"},
		"later":   true,
	}, values)
	assert.Equal(t, values, waitErr.Values)

	require.Len(t, waitErr.Pending, 4)
	assert.Equal(t, PendingValuesFrom{Kind: "ConfigMap", Name: "cm-missing", Namespace: ns, Key: DefaultKey, Reason: PendingValuesFromNotFound}, waitErr.Pending[0])
	assert.Equal(t, PendingValuesFrom{Kind: "Secret", Name: "sec-nokey", Namespace: ns, Key: DefaultKey, Reason: PendingValuesFromKeyMissing}, waitErr.Pending[1])
	assert.Equal(t, PendingValuesFrom{Kind: "ConfigMap", Name: "cm-empty", Namespace: ns, Key: DefaultKey, Reason: PendingValuesFromEmpty}, waitErr.Pending[2])
	assert.Equal(t, PendingValuesFrom{Kind: "Secret", Name: "sec-blank", Namespace: ns, Key: DefaultKey, Reason: PendingValuesFromEmpty}, waitErr.Pending[3])
	assert.Len(t, waitErr.PendingDescriptions(), 4)
	assert.Contains(t, err.Error(), "configmap wait-ns/cm-missing not found")

	// Sources recover: the next reconcile pass merges all sources in order
	// with the same options, hence with the same deployment revision.
	_, err = kubeClient.CoreV1().ConfigMaps(ns).Create(context.TODO(), &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm-missing", Namespace: ns},
		Data:       map[string]string{DefaultKey: "missing: recovered\nshared:\n  a: recovered\n"},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	secNoKey.Data[DefaultKey] = []byte("nokey: recovered")
	_, err = kubeClient.CoreV1().Secrets(ns).Update(context.TODO(), secNoKey, metav1.UpdateOptions{})
	require.NoError(t, err)
	cmEmpty.Data[DefaultKey] = "empty: recovered"
	_, err = kubeClient.CoreV1().ConfigMaps(ns).Update(context.TODO(), cmEmpty, metav1.UpdateOptions{})
	require.NoError(t, err)
	secBlank.Data[DefaultKey] = []byte("blank: recovered")
	_, err = kubeClient.CoreV1().Secrets(ns).Update(context.TODO(), secBlank, metav1.UpdateOptions{})
	require.NoError(t, err)

	values, err = h.getValues(context.TODO(), opts, "default-ns", kubeClient)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"inline":  "kept",
		"present": "present-value",
		"missing": "recovered",
		"nokey":   "recovered",
		"empty":   "recovered",
		"blank":   "recovered",
		"later":   true,
		// cm-present is declared before cm-missing, so the recovered source
		// overrides it, in declaration order.
		"shared": map[string]any{"a": "recovered"},
	}, values)
}

// TestGetValuesSingleSourceKeepsLegacyBehavior ensures that bundles with no or
// a single valuesFrom source keep the historical hard-error behaviour when the
// source is missing or unusable: the new waiting semantics only applies to
// multi-source bundles.
func TestGetValuesSingleSourceKeepsLegacyBehavior(t *testing.T) {
	const ns = "legacy-ns"

	secNoKey := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "sec-nokey", Namespace: ns},
		Data:       map[string][]byte{"other-key": []byte("unused")},
	}
	cmEmpty := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm-empty", Namespace: ns},
		Data:       map[string]string{DefaultKey: ""},
	}
	cmBlank := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm-blank", Namespace: ns},
		Data:       map[string]string{DefaultKey: "   \n"},
	}

	kubeClient := kubernetesfake.NewSimpleClientset(secNoKey, cmEmpty, cmBlank)
	h := &Helm{}

	// referenced object does not exist
	opts := fleet.BundleDeploymentOptions{Helm: &fleet.HelmOptions{
		ValuesFrom: []fleet.ValuesFrom{
			{ConfigMapKeyRef: &fleet.ConfigMapKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "cm-missing"}, Namespace: ns}},
		},
	}}
	_, err := h.getValues(context.TODO(), opts, "default-ns", kubeClient)
	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err))
	assert.NotContains(t, err.Error(), "waiting for valuesFrom")

	// key missing from an existing secret: same message as before
	opts = fleet.BundleDeploymentOptions{Helm: &fleet.HelmOptions{
		ValuesFrom: []fleet.ValuesFrom{
			{SecretKeyRef: &fleet.SecretKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "sec-nokey"}, Namespace: ns}},
		},
	}}
	_, err = h.getValues(context.TODO(), opts, "default-ns", kubeClient)
	require.EqualError(t, err, "key values.yaml is missing from secret "+ns+"/sec-nokey, can't use it in valuesFrom")

	// empty document: EOF error as before
	opts = fleet.BundleDeploymentOptions{Helm: &fleet.HelmOptions{
		ValuesFrom: []fleet.ValuesFrom{
			{ConfigMapKeyRef: &fleet.ConfigMapKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "cm-empty"}, Namespace: ns}},
		},
	}}
	_, err = h.getValues(context.TODO(), opts, "default-ns", kubeClient)
	require.ErrorIs(t, err, io.EOF)

	// whitespace-only document: silently skipped as before
	opts = fleet.BundleDeploymentOptions{Helm: &fleet.HelmOptions{
		Values: &fleet.GenericMap{Data: map[string]any{"inline": "kept"}},
		ValuesFrom: []fleet.ValuesFrom{
			{ConfigMapKeyRef: &fleet.ConfigMapKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "cm-blank"}, Namespace: ns}},
		},
	}}
	values, err := h.getValues(context.TODO(), opts, "default-ns", kubeClient)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"inline": "kept"}, values)
}

// TestGetValuesMultipleSourcesMalformedIsHardError ensures that a source with
// malformed content fails the deployment even with several sources: only
// missing or empty sources are retriable.
func TestGetValuesMultipleSourcesMalformedIsHardError(t *testing.T) {
	const ns = "bad-ns"

	cmBad := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm-bad", Namespace: ns},
		Data:       map[string]string{DefaultKey: "a: [1, 2"},
	}
	cmGood := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm-good", Namespace: ns},
		Data:       map[string]string{DefaultKey: "good: yes"},
	}

	kubeClient := kubernetesfake.NewSimpleClientset(cmBad, cmGood)
	h := &Helm{}

	opts := fleet.BundleDeploymentOptions{Helm: &fleet.HelmOptions{
		ValuesFrom: []fleet.ValuesFrom{
			{ConfigMapKeyRef: &fleet.ConfigMapKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "cm-bad"}, Namespace: ns}},
			{ConfigMapKeyRef: &fleet.ConfigMapKeySelector{LocalObjectReference: fleet.LocalObjectReference{Name: "cm-good"}, Namespace: ns}},
		},
	}}

	_, err := h.getValues(context.TODO(), opts, "default-ns", kubeClient)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "waiting for valuesFrom")
}
