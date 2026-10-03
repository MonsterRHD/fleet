package reconciler

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/rancher/fleet/internal/ocistorage"
	fleet "github.com/rancher/fleet/pkg/apis/fleet.cattle.io/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// fakeOCIDeleter records deleted artifacts and can return errors per id.
type fakeOCIDeleter struct {
	deleted []string
	errByID map[string]error
}

func (d *fakeOCIDeleter) DeleteManifest(_ context.Context, _ ocistorage.OCIOpts, id string) error {
	if err, ok := d.errByID[id]; ok {
		return err
	}
	d.deleted = append(d.deleted, id)
	return nil
}

func gcTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := fleet.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func gcBundle() *fleet.Bundle {
	return &fleet.Bundle{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-bundle",
			Namespace: "fleet-local",
			UID:       types.UID("bundle-uid"),
		},
		Spec: fleet.BundleSpec{ContentsID: "s-new"},
	}
}

func boolPtr(b bool) *bool { return &b }

func gcOCISecret(name string, owner *fleet.Bundle, includeReference bool) *corev1.Secret {
	data := map[string][]byte{}
	if includeReference {
		data[ocistorage.OCISecretReference] = []byte("registry.example.com/test")
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: owner.Namespace,
			Labels:    map[string]string{fleet.InternalSecretLabel: "true"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: fleet.SchemeGroupVersion.String(),
				Kind:       "Bundle",
				Name:       owner.Name,
				UID:        owner.UID,
				Controller: boolPtr(true),
			}},
		},
		Data: data,
		Type: corev1.SecretType(fleet.SecretTypeOCIStorage),
	}
}

func gcBundleDeployment(name, namespace, deploymentID, stagedID string) *fleet.BundleDeployment {
	return &fleet.BundleDeployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				fleet.BundleLabel:          "my-bundle",
				fleet.BundleNamespaceLabel: "fleet-local",
			},
		},
		Spec: fleet.BundleDeploymentSpec{
			DeploymentID:       deploymentID,
			StagedDeploymentID: stagedID,
		},
	}
}

func secretExists(t *testing.T, c client.Client, ns, name string) bool {
	t.Helper()
	err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &corev1.Secret{})
	if err == nil {
		return true
	}
	if apierrors.IsNotFound(err) {
		return false
	}
	t.Fatalf("unexpected error getting secret %s/%s: %v", ns, name, err)
	return false
}

func drainEvents(recorder *events.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-recorder.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func TestContentsIDFromDeploymentID(t *testing.T) {
	cases := map[string]string{
		"s-abc:deadbeef": "s-abc",
		"s-abc":          "s-abc",
		"":               "",
	}
	for in, want := range cases {
		if got := contentsIDFromDeploymentID(in); got != want {
			t.Fatalf("contentsIDFromDeploymentID(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCleanupUnreferencedOCIArtifacts_DeletesUnreferencedGeneration(t *testing.T) {
	bundle := gcBundle()
	current := gcOCISecret("s-new", bundle, true)
	stale := gcOCISecret("s-old", bundle, true)
	// User-provided OCI registry configuration secret, not owned by the bundle.
	userSecret := gcOCISecret("oci-registry-config", bundle, true)
	userSecret.OwnerReferences = nil
	// Secret of another bundle's generation, must be left alone.
	otherBundle := &fleet.Bundle{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: bundle.Namespace, UID: types.UID("other-uid")}}
	foreign := gcOCISecret("s-foreign", otherBundle, true)

	bd := gcBundleDeployment("bd", "cluster-ns", "s-new:hash1", "s-new:hash1")

	scheme := gcTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(bundle, current, stale, userSecret, foreign, bd).Build()

	deleter := &fakeOCIDeleter{}
	originalFactory := newOCIArtifactDeleter
	newOCIArtifactDeleter = func() ociArtifactDeleter { return deleter }
	t.Cleanup(func() { newOCIArtifactDeleter = originalFactory })

	r := &BundleReconciler{Client: c, Recorder: events.NewFakeRecorder(10)}
	if err := r.cleanupUnreferencedOCIArtifacts(context.Background(), bundle); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(deleter.deleted) != 1 || deleter.deleted[0] != "s-old" {
		t.Fatalf("expected only s-old to be deleted from the registry, got %v", deleter.deleted)
	}
	if secretExists(t, c, bundle.Namespace, "s-old") {
		t.Fatal("expected the stale generation secret to be deleted")
	}
	if !secretExists(t, c, bundle.Namespace, "s-new") {
		t.Fatal("expected the current generation secret to be retained")
	}
	if !secretExists(t, c, bundle.Namespace, "oci-registry-config") {
		t.Fatal("expected the user-provided registry secret to be retained")
	}
	if !secretExists(t, c, bundle.Namespace, "s-foreign") {
		t.Fatal("expected another bundle's generation secret to be retained")
	}
}

func TestCleanupUnreferencedOCIArtifacts_RetainsReferencedGeneration(t *testing.T) {
	cases := []struct {
		name         string
		deploymentID string
		stagedID     string
	}{
		{"current deployment still old", "s-old:hash0", "s-old:hash0"},
		{"staged deployment still old", "s-new:hash1", "s-old:hash0"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bundle := gcBundle()
			current := gcOCISecret("s-new", bundle, true)
			stale := gcOCISecret("s-old", bundle, true)
			bd := gcBundleDeployment("bd", "cluster-ns", tc.deploymentID, tc.stagedID)

			scheme := gcTestScheme(t)
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(bundle, current, stale, bd).Build()

			deleter := &fakeOCIDeleter{}
			originalFactory := newOCIArtifactDeleter
			newOCIArtifactDeleter = func() ociArtifactDeleter { return deleter }
			t.Cleanup(func() { newOCIArtifactDeleter = originalFactory })

			r := &BundleReconciler{Client: c, Recorder: events.NewFakeRecorder(10)}
			if err := r.cleanupUnreferencedOCIArtifacts(context.Background(), bundle); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(deleter.deleted) != 0 {
				t.Fatalf("expected no artifact deletion, got %v", deleter.deleted)
			}
			if !secretExists(t, c, bundle.Namespace, "s-old") {
				t.Fatal("expected the referenced generation secret to be retained")
			}
		})
	}
}

func TestCleanupUnreferencedOCIArtifacts_IgnoresDeletingBundleDeployment(t *testing.T) {
	bundle := gcBundle()
	current := gcOCISecret("s-new", bundle, true)
	stale := gcOCISecret("s-old", bundle, true)
	bd := gcBundleDeployment("bd", "cluster-ns", "s-old:hash0", "s-old:hash0")
	bd.Finalizers = []string{"fleet.cattle.io/bundle-deployment-finalizer"}
	now := metav1.Now()
	bd.DeletionTimestamp = &now

	scheme := gcTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(bundle, current, stale, bd).Build()

	deleter := &fakeOCIDeleter{}
	originalFactory := newOCIArtifactDeleter
	newOCIArtifactDeleter = func() ociArtifactDeleter { return deleter }
	t.Cleanup(func() { newOCIArtifactDeleter = originalFactory })

	r := &BundleReconciler{Client: c, Recorder: events.NewFakeRecorder(10)}
	if err := r.cleanupUnreferencedOCIArtifacts(context.Background(), bundle); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(deleter.deleted) != 1 || deleter.deleted[0] != "s-old" {
		t.Fatalf("expected s-old to be deleted, got %v", deleter.deleted)
	}
}

func TestCleanupUnreferencedOCIArtifacts_DeletionFailureWarnsAndRetains(t *testing.T) {
	bundle := gcBundle()
	current := gcOCISecret("s-new", bundle, true)
	stale := gcOCISecret("s-old", bundle, true)

	scheme := gcTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(bundle, current, stale).Build()

	deleter := &fakeOCIDeleter{errByID: map[string]error{"s-old": errors.New("permission denied")}}
	originalFactory := newOCIArtifactDeleter
	newOCIArtifactDeleter = func() ociArtifactDeleter { return deleter }
	t.Cleanup(func() { newOCIArtifactDeleter = originalFactory })

	recorder := events.NewFakeRecorder(10)
	r := &BundleReconciler{Client: c, Recorder: recorder}
	if err := r.cleanupUnreferencedOCIArtifacts(context.Background(), bundle); err != nil {
		t.Fatalf("deletion failures must not block garbage collection, got %v", err)
	}

	if !secretExists(t, c, bundle.Namespace, "s-old") {
		t.Fatal("expected the secret to be retained so deletion can be retried")
	}
	found := false
	for _, e := range drainEvents(recorder) {
		if strings.Contains(e, "FailedToDeleteOCIArtifact") && strings.Contains(e, "s-old") {
			found = true
		}
	}
	if !found {
		t.Fatal("expected a FailedToDeleteOCIArtifact warning event")
	}
}

func TestCleanupUnreferencedOCIArtifacts_AlreadyDeletedRegistryArtifact(t *testing.T) {
	bundle := gcBundle()
	current := gcOCISecret("s-new", bundle, true)
	stale := gcOCISecret("s-old", bundle, true)

	scheme := gcTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(bundle, current, stale).Build()

	deleter := &fakeOCIDeleter{errByID: map[string]error{"s-old": errors.New("artifact not found in registry")}}
	originalFactory := newOCIArtifactDeleter
	newOCIArtifactDeleter = func() ociArtifactDeleter { return deleter }
	t.Cleanup(func() { newOCIArtifactDeleter = originalFactory })

	recorder := events.NewFakeRecorder(10)
	r := &BundleReconciler{Client: c, Recorder: recorder}
	if err := r.cleanupUnreferencedOCIArtifacts(context.Background(), bundle); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if secretExists(t, c, bundle.Namespace, "s-old") {
		t.Fatal("expected the stale secret to be deleted when the artifact is already gone")
	}
	for _, e := range drainEvents(recorder) {
		if strings.Contains(e, "FailedToDeleteOCIArtifact") {
			t.Fatalf("did not expect a warning event for a not-found artifact: %s", e)
		}
	}
}

func TestCleanupUnreferencedOCIArtifacts_MalformedSecretWarnsAndRetains(t *testing.T) {
	bundle := gcBundle()
	current := gcOCISecret("s-new", bundle, true)
	stale := gcOCISecret("s-old", bundle, false) // missing reference key

	scheme := gcTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(bundle, current, stale).Build()

	deleter := &fakeOCIDeleter{}
	originalFactory := newOCIArtifactDeleter
	newOCIArtifactDeleter = func() ociArtifactDeleter { return deleter }
	t.Cleanup(func() { newOCIArtifactDeleter = originalFactory })

	recorder := events.NewFakeRecorder(10)
	r := &BundleReconciler{Client: c, Recorder: recorder}
	if err := r.cleanupUnreferencedOCIArtifacts(context.Background(), bundle); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(deleter.deleted) != 0 {
		t.Fatalf("expected no registry deletion, got %v", deleter.deleted)
	}
	if !secretExists(t, c, bundle.Namespace, "s-old") {
		t.Fatal("expected the malformed secret to be retained")
	}
	found := false
	for _, e := range drainEvents(recorder) {
		if strings.Contains(e, "FailedToDeleteOCIArtifact") && strings.Contains(e, "s-old") {
			found = true
		}
	}
	if !found {
		t.Fatal("expected a warning event for the malformed stale secret")
	}
}

func TestCleanupUnreferencedOCIArtifacts_NonOCIBundleIsNoop(t *testing.T) {
	bundle := gcBundle()
	bundle.Spec.ContentsID = ""

	scheme := gcTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(bundle).Build()

	deleter := &fakeOCIDeleter{}
	originalFactory := newOCIArtifactDeleter
	newOCIArtifactDeleter = func() ociArtifactDeleter { return deleter }
	t.Cleanup(func() { newOCIArtifactDeleter = originalFactory })

	r := &BundleReconciler{Client: c, Recorder: events.NewFakeRecorder(10)}
	if err := r.cleanupUnreferencedOCIArtifacts(context.Background(), bundle); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(deleter.deleted) != 0 {
		t.Fatalf("expected no deletion for a non-OCI bundle, got %v", deleter.deleted)
	}
}


