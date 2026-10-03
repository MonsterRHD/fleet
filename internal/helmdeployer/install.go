package helmdeployer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"strconv"
	"strings"
	"time"

	"helm.sh/helm/v4/pkg/action"
	chartv2 "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/chart/v2/loader"
	"helm.sh/helm/v4/pkg/kube"
	releasecommon "helm.sh/helm/v4/pkg/release/common"
	releasev1 "helm.sh/helm/v4/pkg/release/v1"
	"helm.sh/helm/v4/pkg/storage/driver"

	"github.com/rancher/fleet/internal/helmdeployer/render"
	"github.com/rancher/fleet/internal/manifest"
	fleet "github.com/rancher/fleet/pkg/apis/fleet.cattle.io/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type dryRunConfig struct {
	DryRun       bool
	DryRunOption string
}

// Deploy deploys an unpacked content resource with helm. bundleID is the name of the bundledeployment.
func (h *Helm) Deploy(ctx context.Context, bundleID string, manifest *manifest.Manifest, options fleet.BundleDeploymentOptions) (*releasev1.Release, error) {
	if options.Helm == nil {
		options.Helm = &fleet.HelmOptions{}
	}
	if options.Kustomize == nil {
		options.Kustomize = &fleet.KustomizeOptions{}
	}

	tar, err := render.HelmChart(bundleID, manifest, options)
	if err != nil {
		return nil, err
	}

	chart, err := loader.LoadArchive(tar)
	if err != nil {
		return nil, err
	}

	if chart.Metadata.Annotations == nil {
		chart.Metadata.Annotations = map[string]string{}
	}
	chart.Metadata.Annotations[ServiceAccountNameAnnotation] = options.ServiceAccount
	chart.Metadata.Annotations[BundleIDAnnotation] = bundleID
	chart.Metadata.Annotations[AgentNamespaceAnnotation] = h.agentNamespace
	chart.Metadata.Annotations[KeepResourcesAnnotation] = strconv.FormatBool(options.KeepResources)

	if manifest.Commit != "" {
		chart.Metadata.Annotations[CommitAnnotation] = manifest.Commit
	}

	release, err := h.install(ctx, bundleID, manifest, chart, options, getDryRunConfig(chart, true))
	if err != nil {
		// A server-side dry run, which is needed for charts using the lookup
		// function, cannot install the CRDs contained in the chart. Custom
		// resources defined by those CRDs then have no REST mapping yet and the
		// dry run fails. The install below installs crds/ first, hence it
		// validates the manifest without hitting this chicken and egg problem.
		if h.template || !isMissingOwnCRDsError(err, chart) {
			return nil, err
		}
		log.FromContext(ctx).WithName("helm-deployer").Info(
			"Skipping dry run validation, the CRDs defining resources from this chart are not installed yet",
			"error", err,
		)
	} else if h.template {
		return release, nil
	}

	return h.install(ctx, bundleID, manifest, chart, options, getDryRunConfig(chart, false))
}

// install runs helm install or upgrade and supports dry running the action. Will run helm rollback in case of a failed upgrade.
func (h *Helm) install(ctx context.Context, bundleID string, manifest *manifest.Manifest, chart *chartv2.Chart, options fleet.BundleDeploymentOptions, dryRunCfg dryRunConfig) (*releasev1.Release, error) {
	logger := log.FromContext(ctx).WithName("helm-deployer").WithName("install").WithValues("commit", manifest.Commit, "dryRun", dryRunCfg.DryRun)
	timeout, defaultNamespace, releaseName := h.getOpts(bundleID, options)

	cfg, err := h.getCfg(ctx, defaultNamespace, options.ServiceAccount)
	if err != nil {
		return nil, err
	}

	// kubeClient is nil in template mode; getValues treats a nil client as
	// "skip ValuesFrom lookup" and returns only the statically defined values.
	var kubeClient kubernetes.Interface
	if !h.template {
		kubeClient, err = kubeClientFromGetter(cfg.RESTClientGetter)
		if err != nil {
			return nil, err
		}
	}

	values, err := h.getValues(ctx, options, defaultNamespace, kubeClient)
	if err != nil {
		return nil, err
	}

	uninstall, err := h.mustUninstall(cfg, releaseName)
	if err != nil {
		return nil, err
	}

	if uninstall {
		logger.Info("Uninstalling helm release first")
		if err := h.delete(ctx, bundleID, options, dryRunCfg.DryRun); err != nil {
			return nil, err
		}
		if dryRunCfg.DryRun {
			// In dry run mode, we've validated that uninstall is needed but can't proceed
			// with install/upgrade since the old release conceptually still exists.
			// Returning (nil, nil) indicates successful dry run completion with no release object.
			return nil, nil
		}
	}

	install, err := h.mustInstall(cfg, releaseName)
	if err != nil {
		return nil, err
	}

	pr, err := h.createPostRenderer(cfg, bundleID, manifest, chart, options)
	if err != nil {
		return nil, err
	}

	if install {
		return h.runInstall(ctx, cfg, chart, values, releaseName, defaultNamespace, timeout, options, pr, dryRunCfg)
	}

	// Before running upgrade, check if we're upgrading from a pending-install with no previous version.
	// In this case, ensure any orphaned pending-install release is marked as failed so the upgrade can proceed cleanly.
	if err := h.ensureForceOnOrphanedPendingInstall(ctx, cfg, releaseName); err != nil {
		return nil, err
	}

	return h.runUpgrade(ctx, cfg, chart, values, releaseName, defaultNamespace, timeout, options, pr, dryRunCfg)
}

// createPostRenderer creates a post-renderer for Helm charts that handles label/annotation
// transformations and CRD deletion policies based on Fleet bundle deployment options.
func (h *Helm) createPostRenderer(cfg *action.Configuration, bundleID string, manifest *manifest.Manifest, chart *chartv2.Chart, options fleet.BundleDeploymentOptions) (*postRender, error) {
	pr := &postRender{
		labelPrefix: h.labelPrefix,
		labelSuffix: h.labelSuffix,
		bundleID:    bundleID,
		manifest:    manifest,
		opts:        options,
		chart:       chart,
	}

	if !h.useGlobalCfg {
		mapper, err := cfg.RESTClientGetter.ToRESTMapper()
		if err != nil {
			return nil, err
		}
		pr.mapper = mapper
	}

	return pr, nil
}

// runInstall executes a Helm install operation with the provided configuration and values.
// It creates an Install action, configures it, and runs the installation.
func (h *Helm) runInstall(
	ctx context.Context,
	cfg *action.Configuration,
	chart *chartv2.Chart,
	values map[string]any,
	releaseName string,
	namespace string,
	timeout time.Duration,
	options fleet.BundleDeploymentOptions,
	pr *postRender,
	dryRunCfg dryRunConfig,
) (*releasev1.Release, error) {
	logger := log.FromContext(ctx)
	u := action.NewInstall(cfg)

	h.configureInstallAction(u, cfg, releaseName, namespace, timeout, options, pr, dryRunCfg)

	if !dryRunCfg.DryRun {
		logger.Info("Installing helm release")
	}

	rel, err := u.Run(chart, values)
	if err != nil {
		return nil, err
	}

	return assertRelease(rel)
}

// configureDryRunStrategy sets the DryRunStrategy based on template mode and dryRunConfig.
// Template mode requires DryRunClient to render without cluster interaction.
// If DryRunOption is "server", use DryRunServer to allow lookup functions to query the cluster.
// Otherwise, use DryRunClient for client-only dry run or DryRunNone for actual execution.
func (h *Helm) configureDryRunStrategy(dryRunCfg dryRunConfig) action.DryRunStrategy {
	if h.template {
		return action.DryRunClient
	} else if dryRunCfg.DryRun {
		if dryRunCfg.DryRunOption == "server" {
			return action.DryRunServer
		}
		return action.DryRunClient
	}
	return action.DryRunNone
}

// configureInstallAction configures a Helm Install action with Fleet-specific options,
// including timeout, wait strategies, and dry-run configuration.
func (h *Helm) configureInstallAction(u *action.Install, cfg *action.Configuration, releaseName, namespace string, timeout time.Duration, options fleet.BundleDeploymentOptions, pr *postRender, dryRunCfg dryRunConfig) {
	if cfg.Capabilities != nil {
		if cfg.Capabilities.KubeVersion.Version != "" {
			u.KubeVersion = &cfg.Capabilities.KubeVersion
		}
		if cfg.Capabilities.APIVersions != nil {
			u.APIVersions = cfg.Capabilities.APIVersions
		}
	}
	u.TakeOwnership = options.Helm.TakeOwnership
	u.ForceReplace = options.Helm.Force || (options.CorrectDrift != nil && options.CorrectDrift.Force)
	// Disable server-side apply when taking ownership or forcing replacement to avoid conflicts.
	// When adopting existing resources or forcing replacement, we need three-way merge instead.
	if u.TakeOwnership || u.ForceReplace {
		u.ServerSideApply = false
	} else {
		// Explicitly enable server-side apply when neither TakeOwnership nor ForceReplace is set
		u.ServerSideApply = true
	}
	u.EnableDNS = !options.Helm.DisableDNS
	u.Replace = true
	u.RollbackOnFailure = options.Helm.Atomic
	u.ReleaseName = releaseName
	u.CreateNamespace = options.CreateNamespace == nil || *options.CreateNamespace
	u.Namespace = namespace
	u.Timeout = timeout
	u.DryRunStrategy = h.configureDryRunStrategy(dryRunCfg)
	u.SkipSchemaValidation = options.Helm.SkipSchemaValidation
	u.PostRenderer = pr
	u.WaitForJobs = options.Helm.WaitForJobs
	// When timeout is set, use StatusWatcherStrategy to wait for resources.
	// Otherwise use HookOnlyStrategy (the default, equivalent to not waiting).
	if u.Timeout > 0 {
		u.WaitStrategy = kube.StatusWatcherStrategy
	} else {
		u.WaitStrategy = kube.HookOnlyStrategy
	}
}

// runUpgrade executes a Helm upgrade operation with the provided configuration and values.
// It creates an Upgrade action, configures it, and runs the upgrade with automatic rollback
// retry logic if the upgrade is interrupted.
func (h *Helm) runUpgrade(
	ctx context.Context,
	cfg *action.Configuration,
	chart *chartv2.Chart,
	values map[string]any,
	releaseName string,
	namespace string,
	timeout time.Duration,
	options fleet.BundleDeploymentOptions,
	pr *postRender,
	dryRunCfg dryRunConfig,
) (*releasev1.Release, error) {
	logger := log.FromContext(ctx)
	u := action.NewUpgrade(cfg)

	h.configureUpgradeAction(u, namespace, timeout, options, pr, dryRunCfg)

	if !dryRunCfg.DryRun {
		logger.Info("Upgrading helm release")
	}

	rel, err := u.Run(releaseName, chart, values)
	if err != nil && err.Error() == HelmUpgradeInterruptedError {
		return h.retryUpgradeAfterRollback(ctx, cfg, u, releaseName, chart, values)
	}
	if err != nil {
		return nil, err
	}

	return assertRelease(rel)
}

// configureUpgradeAction configures a Helm Upgrade action with Fleet-specific options,
// including timeout, wait strategies, and drift correction settings.
func (h *Helm) configureUpgradeAction(u *action.Upgrade, namespace string, timeout time.Duration, options fleet.BundleDeploymentOptions, pr *postRender, dryRunCfg dryRunConfig) {
	u.TakeOwnership = true
	u.EnableDNS = !options.Helm.DisableDNS
	u.ForceReplace = options.Helm.Force
	if options.CorrectDrift != nil {
		u.ForceReplace = u.ForceReplace || options.CorrectDrift.Force
	}
	// When using ForceReplace, must disable ServerSideApply.
	// ForceReplace and ServerSideApply cannot be used together in Helm v4.
	// Set to "false" (not "auto") to explicitly disable server-side apply.
	// Otherwise use "auto" to respect the previous release's apply method.
	if u.ForceReplace {
		u.ServerSideApply = "false"
	} else {
		u.ServerSideApply = "auto"
	}
	u.RollbackOnFailure = options.Helm.Atomic
	u.MaxHistory = options.Helm.MaxHistory
	if u.MaxHistory == 0 {
		u.MaxHistory = MaxHelmHistory
	}
	u.Namespace = namespace
	u.Timeout = timeout
	u.DryRunStrategy = h.configureDryRunStrategy(dryRunCfg)
	u.SkipSchemaValidation = options.Helm.SkipSchemaValidation
	u.DisableOpenAPIValidation = h.template || dryRunCfg.DryRun
	u.PostRenderer = pr
	u.WaitForJobs = options.Helm.WaitForJobs
	// When timeout is set, use StatusWatcherStrategy to wait for resources.
	// Otherwise use HookOnlyStrategy (the default, equivalent to not waiting).
	if u.Timeout > 0 {
		u.WaitStrategy = kube.StatusWatcherStrategy
	} else {
		u.WaitStrategy = kube.HookOnlyStrategy
	}
}

// retryUpgradeAfterRollback handles the case where a Helm upgrade is interrupted and retries
// the upgrade after performing a rollback. This addresses the "another operation is in progress" error.
func (h *Helm) retryUpgradeAfterRollback(ctx context.Context, cfg *action.Configuration, u *action.Upgrade, releaseName string, chart *chartv2.Chart, values map[string]any) (*releasev1.Release, error) {
	logger := log.FromContext(ctx)
	logger.Info("Helm doing a rollback", "error", HelmUpgradeInterruptedError)

	// Check if there's a previous version to rollback to
	lastRelease, err := getLastRelease(cfg.Releases, releaseName)
	if err != nil {
		return nil, fmt.Errorf("cannot get last release for rollback: %w", err)
	}

	// Check if this is an orphaned release and patch if needed
	patched, err := handleOrphanedRelease(ctx, cfg, lastRelease, releaseName)
	if err != nil {
		return nil, err
	}

	if patched {
		// Retry the upgrade after patching status
		logger.Info("Retrying upgrade after patching release to failed")
		rel, err := u.Run(releaseName, chart, values)
		if err != nil {
			return nil, fmt.Errorf("upgrade failed after patching release status: %w", err)
		}
		return assertRelease(rel)
	}

	// Previous version exists, proceed with rollback
	r := action.NewRollback(cfg)
	r.ServerSideApply = "false"
	r.WaitStrategy = kube.HookOnlyStrategy
	err = r.Run(releaseName)
	if err != nil {
		return nil, err
	}

	logger.V(1).Info("Retrying upgrade after rollback")
	rel, err := u.Run(releaseName, chart, values)
	if err != nil {
		return nil, err
	}

	return assertRelease(rel)
}

// assertRelease converts a Helm release interface to a concrete *releasev1.Release type.
func assertRelease(rel any) (*releasev1.Release, error) {
	if v1Rel, ok := rel.(*releasev1.Release); ok {
		return v1Rel, nil
	}
	return nil, fmt.Errorf("unexpected release type: %T", rel)
}

func (h *Helm) mustUninstall(cfg *action.Configuration, releaseName string) (bool, error) {
	r, err := getLastRelease(cfg.Releases, releaseName)
	if err != nil {
		// If the release doesn't exist, there's nothing to uninstall
		if errors.Is(err, driver.ErrReleaseNotFound) || errors.Is(err, driver.ErrNoDeployedReleases) {
			return false, nil
		}
		return false, err
	}
	return r.Info.Status == releasecommon.StatusUninstalling, nil
}

// mustInstall checks if a fresh install is required by verifying if there is no deployed release.
// Returns true if no deployed release exists for the given release name.
func (h *Helm) mustInstall(cfg *action.Configuration, releaseName string) (bool, error) {
	_, err := cfg.Releases.Deployed(releaseName)
	if err != nil && errors.Is(err, driver.ErrNoDeployedReleases) {
		_, err := getLastRelease(cfg.Releases, releaseName)
		if err == nil {
			// There is a release, but not deployed (e.g., failed install/upgrade)
			return false, nil
		}
		return true, nil
	}
	return false, err
}

// ensureForceOnOrphanedPendingInstall checks if we're about to upgrade from a pending-install
// release that has no previous version. This handles the case where:
// 1. A release is stuck in pending-install status
// 2. No previous successful version exists (lost history or initial install failure)
// 3. Normal upgrade will fail with "another operation is in progress"
// In this scenario, we patch the release status to "failed" to allow the upgrade to proceed.
// This avoids an unnecessary upgrade attempt that would fail and require a retry.
func (h *Helm) ensureForceOnOrphanedPendingInstall(ctx context.Context, cfg *action.Configuration, releaseName string) error {
	// Get the last release to check its status
	lastRelease, err := getLastRelease(cfg.Releases, releaseName)
	if err != nil {
		// If we can't get the last release, proceed normally
		if errors.Is(err, driver.ErrReleaseNotFound) {
			return nil
		}
		return err
	}

	// Only handle pending-install status
	if lastRelease.Info.Status != releasecommon.StatusPendingInstall {
		return nil
	}

	// Check if a previous version exists and patch if needed
	_, err = handleOrphanedRelease(ctx, cfg, lastRelease, releaseName)
	return err
}

// handleOrphanedRelease checks if a release has no valid previous version to rollback to
// and patches its status to failed if needed. This handles cases where a release is stuck
// in a transient state (like pending-install) but has no previous version to rollback to.
// Returns true if the release was patched, false otherwise.
func handleOrphanedRelease(ctx context.Context, cfg *action.Configuration, lastRelease *releasev1.Release, releaseName string) (bool, error) {
	logger := log.FromContext(ctx)

	// Check if a previous version exists
	previousVersion := lastRelease.Version - 1
	if previousVersion < 1 {
		// Version 1 with no v0 - patch status to failed
		logger.Info("No previous version exists, patching release to failed",
			"releaseName", releaseName,
			"currentVersion", lastRelease.Version)

		if err := patchReleaseStatus(cfg.Releases, lastRelease, releasecommon.StatusFailed); err != nil {
			return false, fmt.Errorf("failed to patch release status: %w", err)
		}
		return true, nil
	}

	// Try to get the previous version
	_, err := cfg.Releases.Get(releaseName, previousVersion)
	if err != nil {
		if errors.Is(err, driver.ErrReleaseNotFound) {
			// Previous version doesn't exist - patch status to failed
			logger.Info("Previous version missing, patching release to failed",
				"releaseName", releaseName,
				"currentVersion", lastRelease.Version,
				"missingVersion", previousVersion)

			if err := patchReleaseStatus(cfg.Releases, lastRelease, releasecommon.StatusFailed); err != nil {
				return false, fmt.Errorf("failed to patch release status: %w", err)
			}
			return true, nil
		}
		return false, err
	}

	// Previous version exists, no patching needed
	return false, nil
}

// Reasons reported on PendingValuesFrom. They only describe the wait, the
// caller decides whether a given condition is retriable or an error.
const (
	// PendingValuesFromNotFound means the referenced ConfigMap or Secret object does not exist.
	PendingValuesFromNotFound = "notFound"
	// PendingValuesFromKeyMissing means the object exists, but not the referenced key.
	PendingValuesFromKeyMissing = "keyMissing"
	// PendingValuesFromEmpty means object and key exist, but the values document is empty.
	PendingValuesFromEmpty = "empty"
)

// PendingValuesFrom describes one valuesFrom source which could not be
// resolved yet, because its ConfigMap/Secret is missing, its key is missing or
// its values document is empty.
type PendingValuesFrom struct {
	// Kind is "ConfigMap" or "Secret".
	Kind string
	// Name and Namespace identify the source object. Namespace is the
	// effectively used namespace, defaulting to the Helm release namespace.
	Name      string
	Namespace string
	// Key is the effectively used key, defaulting to DefaultKey.
	Key    string
	Reason string
}

func (p PendingValuesFrom) String() string {
	switch p.Reason {
	case PendingValuesFromNotFound:
		return fmt.Sprintf("%s %s/%s not found", strings.ToLower(p.Kind), p.Namespace, p.Name)
	case PendingValuesFromKeyMissing:
		return fmt.Sprintf("%s %s/%s has no key %q", strings.ToLower(p.Kind), p.Namespace, p.Name, p.Key)
	default:
		return fmt.Sprintf("%s %s/%s has no values under key %q", strings.ToLower(p.Kind), p.Namespace, p.Name, p.Key)
	}
}

// WaitingForValuesFromError is returned while a bundle uses several
// valuesFrom sources and one or more of them are missing or empty. It is a
// temporary condition, not a deployment failure: the sources are meant to be
// merged in declaration order, so deploying without the missing ones would
// drop or fail to override their keys. The caller should keep the currently
// deployed release and the already resolved values untouched and retry, which
// applies the unchanged deployment revision once every source is available.
type WaitingForValuesFromError struct {
	// Pending lists every source that could not be resolved in this pass.
	Pending []PendingValuesFrom
	// Values holds the inline values merged with the sources that were
	// resolved so far, in declaration order. They are not deployed while the
	// wait lasts and are retained for logging and diagnostics only.
	Values map[string]any
}

func (e *WaitingForValuesFromError) Error() string {
	pending := make([]string, 0, len(e.Pending))
	for _, p := range e.Pending {
		pending = append(pending, p.String())
	}
	return "waiting for valuesFrom sources to become available: " + strings.Join(pending, "; ")
}

// PendingDescriptions returns one human readable description per pending
// source. Loggers reflect over the slice instead of calling Error, so they
// should use this rather than Error.
func (e *WaitingForValuesFromError) PendingDescriptions() []string {
	pending := make([]string, 0, len(e.Pending))
	for _, p := range e.Pending {
		pending = append(pending, p.String())
	}
	return pending
}

// valuesFromSource identifies one valuesFrom ConfigMap or Secret reference,
// after resolving it from a ValuesFrom entry.
type valuesFromSource struct {
	// kind is "ConfigMap" or "Secret".
	kind               string
	name               string
	namespace          string
	key                string
}

func (h *Helm) getValues(ctx context.Context, options fleet.BundleDeploymentOptions, defaultNamespace string, kubeClient kubernetes.Interface) (map[string]any, error) {
	if options.Helm == nil {
		return nil, nil
	}

	var values map[string]any
	if options.Helm.Values != nil {
		values = options.Helm.Values.Data
	}

	// avoid the possibility of returning a nil map
	if values == nil {
		values = map[string]any{}
	}
	// kubeClient is nil in template mode; skip the cluster lookups in that case.
	if kubeClient == nil {
		return values, nil
	}

	// With several valuesFrom sources, those are merged in declaration order,
	// with later keys overriding earlier ones. A source that is still missing
	// or empty is a temporary wait (see WaitingForValuesFromError), instead of
	// failing the deployment with partial values or discarding the values
	// already resolved. Bundles without valuesFrom, or with a single source,
	// keep the historical behaviour where an unresolved source fails the
	// deployment.
	multiSource := len(options.Helm.ValuesFrom) > 1
	var pending []PendingValuesFrom

	for _, valuesFrom := range options.Helm.ValuesFrom {
		// The secret is merged after the configmap within one entry to stay
		// compatible with fleet <= 0.6.0.
		refs := make([]valuesFromSource, 0, 2)
		if valuesFrom.ConfigMapKeyRef != nil {
			refs = append(refs, valuesFromSource{
				kind: "ConfigMap", name: valuesFrom.ConfigMapKeyRef.Name,
				namespace: valuesFrom.ConfigMapKeyRef.Namespace, key: valuesFrom.ConfigMapKeyRef.Key,
			})
		}
		if valuesFrom.SecretKeyRef != nil {
			refs = append(refs, valuesFromSource{
				kind: "Secret", name: valuesFrom.SecretKeyRef.Name,
				namespace: valuesFrom.SecretKeyRef.Namespace, key: valuesFrom.SecretKeyRef.Key,
			})
		}

		for _, ref := range refs {
			tempValues, pendingSource, err := h.resolveValuesFromSource(ctx, kubeClient, options, defaultNamespace, ref, multiSource)
			if err != nil {
				return nil, err
			}
			if pendingSource != nil {
				pending = append(pending, *pendingSource)
			}
			if tempValues != nil {
				values = mergeValues(values, tempValues)
			}
		}
	}

	if len(pending) > 0 {
		return values, &WaitingForValuesFromError{Pending: pending, Values: values}
	}

	return values, nil
}

// resolveValuesFromSource fetches and decodes one valuesFrom source. It
// returns the decoded values (nil for an empty document), a non-nil
// PendingValuesFrom when the source is missing, its key is missing or its
// document is empty and waitOnMissing is true (i.e. the bundle uses several
// sources), and a hard error otherwise. Malformed values and every error for a
// single source are returned as errors to keep the historical behaviour.
func (h *Helm) resolveValuesFromSource(
	ctx context.Context,
	kubeClient kubernetes.Interface,
	options fleet.BundleDeploymentOptions,
	defaultNamespace string,
	ref valuesFromSource,
	waitOnMissing bool,
) (map[string]any, *PendingValuesFrom, error) {
	namespace := ref.namespace
	if namespace == "" || isInDownstreamResources(ref.name, ref.kind, options) {
		// If the namespace is not set, or if the resource is part of the
		// copied resources, we assume it is in the default namespace of the
		// Helm release.
		namespace = defaultNamespace
	}
	key := ref.key
	if key == "" {
		key = DefaultKey
	}
	pending := func(reason string) *PendingValuesFrom {
		return &PendingValuesFrom{Kind: ref.kind, Name: ref.name, Namespace: namespace, Key: key, Reason: reason}
	}

	var (
		tempValues map[string]any
		err        error
	)
	switch ref.kind {
	case "ConfigMap":
		var configMap *corev1.ConfigMap
		configMap, err = kubeClient.CoreV1().ConfigMaps(namespace).Get(ctx, ref.name, metav1.GetOptions{})
		if err == nil {
			tempValues, err = valuesFromConfigMap(ref.name, namespace, key, configMap)
		}
	case "Secret":
		var secret *corev1.Secret
		secret, err = kubeClient.CoreV1().Secrets(namespace).Get(ctx, ref.name, metav1.GetOptions{})
		if err == nil {
			tempValues, err = valuesFromSecret(ref.name, namespace, key, secret)
		}
	default:
		return nil, nil, fmt.Errorf("unsupported valuesFrom kind %q", ref.kind)
	}

	if err != nil {
		if waitOnMissing && apierrors.IsNotFound(err) {
			return nil, pending(PendingValuesFromNotFound), nil
		}
		if p, ok := classifyValuesFromError(ref.kind, ref.name, namespace, key, waitOnMissing, err); ok {
			return nil, &p, nil
		}
		return nil, nil, err
	}

	if tempValues == nil && waitOnMissing {
		// Object and key exist, but the document contains no values.
		return nil, pending(PendingValuesFromEmpty), nil
	}

	return tempValues, nil, nil
}

// classifyValuesFromError turns a "source not ready yet" decode error into a
// PendingValuesFrom description when waitOnMissing is true (i.e. the bundle
// uses several valuesFrom sources). Malformed values are never retriable and
// fall through as a hard error, as do all errors for a single source, which
// keeps the historical error behaviour.
func classifyValuesFromError(kind, name, namespace, key string, waitOnMissing bool, err error) (PendingValuesFrom, bool) {
	if !waitOnMissing || err == nil {
		return PendingValuesFrom{}, false
	}

	var missingKey *valuesFromKeyMissingError
	if errors.As(err, &missingKey) {
		return PendingValuesFrom{Kind: kind, Name: name, Namespace: namespace, Key: key, Reason: PendingValuesFromKeyMissing}, true
	}
	if errors.Is(err, io.EOF) {
		// An empty values document decodes to io.EOF; treat it like a source
		// that has not been populated yet.
		return PendingValuesFrom{Kind: kind, Name: name, Namespace: namespace, Key: key, Reason: PendingValuesFromEmpty}, true
	}

	return PendingValuesFrom{}, false
}

// restConfigGetter produces a *rest.Config. Both
// genericclioptions.RESTClientGetter and action.RESTClientGetter satisfy it.
type restConfigGetter interface {
	ToRESTConfig() (*rest.Config, error)
}

func kubeClientFromGetter(getter restConfigGetter) (kubernetes.Interface, error) {
	restConfig, err := getter.ToRESTConfig()
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(restConfig)
}

// valuesFromKeyMissingError is returned when a referenced valuesFrom key is
// absent from its ConfigMap or Secret. For a bundle with several sources it is
// classified as a temporary wait; for a single source it keeps failing the
// deployment as before. Its Error text is kept stable for backwards
// compatibility.
type valuesFromKeyMissingError struct {
	kind      string
	name      string
	namespace string
	key       string
}

func (e *valuesFromKeyMissingError) Error() string {
	return fmt.Sprintf("key %s is missing from %s %s/%s, can't use it in valuesFrom", e.key, strings.ToLower(e.kind), e.namespace, e.name)
}

func valuesFromSecret(name, namespace, key string, secret *corev1.Secret) (map[string]any, error) {
	var m map[string]any
	if secret == nil {
		return m, nil
	}

	values, ok := secret.Data[key]
	if !ok {
		return nil, &valuesFromKeyMissingError{kind: "Secret", name: name, namespace: namespace, key: key}
	}
	if err := yaml.NewYAMLToJSONDecoder(bytes.NewBuffer(values)).Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

func valuesFromConfigMap(name, namespace, key string, configMap *corev1.ConfigMap) (map[string]any, error) {
	var m map[string]any
	if configMap == nil {
		return m, nil
	}

	values, ok := configMap.Data[key]
	if !ok {
		return nil, &valuesFromKeyMissingError{kind: "ConfigMap", name: name, namespace: namespace, key: key}
	}
	if err := yaml.NewYAMLToJSONDecoder(bytes.NewBufferString(values)).Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

func mergeMaps(base, other map[string]string) map[string]string {
	result := map[string]string{}
	maps.Copy(result, base)
	maps.Copy(result, other)
	return result
}

// mergeValues merges the src map into dest and returns dest. It is called once
// per valuesFrom source, in declaration order, so keys from a later source
// override keys from an earlier source (or from the inline helm values):
//   - when both sides hold a map for the same key, they are deep merged, again
//     with src winning on conflicts;
//   - any non-map value (scalars, slices, ...) from src replaces the value in
//     dest.
//
// dest is mutated. This is slightly adapted from:
// https://github.com/helm/helm/blob/2332b480c9cb70a0d8a85247992d6155fbe82416/cmd/helm/install.go#L359
func mergeValues(dest, src map[string]any) map[string]any {
	for k, v := range src {
		// If the key doesn't exist already, then just set the key to that value
		if _, exists := dest[k]; !exists {
			// new key
			dest[k] = v
			continue
		}
		nextMap, ok := v.(map[string]any)
		// If it isn't another map, overwrite the value
		if !ok {
			// new key is not a map, overwrite existing key as we prefer values over maps
			dest[k] = v
			continue
		}
		// Edge case: If the key exists in the destination, but isn't a map
		destMap, isMap := dest[k].(map[string]any)
		// If the source map has a map for this key, prefer it
		if !isMap {
			dest[k] = v
			continue
		}
		// If we got to this point, it is a map in both, so merge them
		dest[k] = mergeValues(destMap, nextMap)
	}
	return dest
}

// getDryRunConfig determines the dry-run configuration based on whether the chart
// uses the Helm "lookup" function.
// If the chart contains the "lookup" function, DryRunOption is set to "server"
// to allow the lookup function to interact with the Kubernetes API during a dry-run.
// Otherwise, DryRunOption remains empty, implying a client-side dry-run.
func getDryRunConfig(chart *chartv2.Chart, dryRun bool) dryRunConfig {
	cfg := dryRunConfig{DryRun: dryRun}
	if dryRun && hasLookupFunction(chart) {
		cfg.DryRunOption = "server"
	}

	return cfg
}

// isInDownstreamResources returns true when a resource with the
// provided name exists in the provided BundleDeploymentOptions.DownstreamResources slice.
// If not found, returns false.
func isInDownstreamResources(resourceName, kind string, options fleet.BundleDeploymentOptions) bool {
	kind = strings.ToLower(kind)

	for _, dr := range options.DownstreamResources {
		if dr.Name == resourceName && strings.ToLower(dr.Kind) == kind {
			return true
		}
	}
	return false
}
