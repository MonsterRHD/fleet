package apply

import (
	"context"
	"errors"
	"testing"

	"github.com/rancher/fleet/internal/manifest"
	"github.com/rancher/fleet/internal/ocistorage"
	fleet "github.com/rancher/fleet/pkg/apis/fleet.cattle.io/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// stubOCIStore is a stand-in for the OCI registry client used by saveOCIBundle.
type stubOCIStore struct {
	pushedIDs []string
	err       error
}

func (s *stubOCIStore) PushManifest(_ context.Context, _ ocistorage.OCIOpts, id string, _ *manifest.Manifest) error {
	s.pushedIDs = append(s.pushedIDs, id)
	return s.err
}

func ociTestScheme(t *testing.T) *runtime.Scheme {
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

func ociGenerationSecret(id string, bundle *fleet.Bundle) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      id,
			Namespace: bundle.Namespace,
			Labels:    map[string]string{fleet.InternalSecretLabel: "true"},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         fleet.SchemeGroupVersion.String(),
					Kind:               "Bundle",
					Name:               bundle.Name,
					UID:                bundle.UID,
					BlockOwnerDeletion: new(bool),
					Controller:         new(bool),
				},
			},
		},
		Data: map[string][]byte{ocistorage.OCISecretReference: []byte("registry.example.com/test")},
		Type: corev1.SecretType(fleet.SecretTypeOCIStorage),
	}
}

func TestSaveOCIBundle_AtomicGenerationSwitch(t *testing.T) {
	scheme := ociTestScheme(t)

	existing := &fleet.Bundle{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "my-bundle",
			Namespace:  "fleet-local",
			UID:        types.UID("bundle-uid"),
			Generation: 1,
		},
		Spec: fleet.BundleSpec{
			ContentsID: "s-old",
		},
	}
	oldSecret := ociGenerationSecret("s-old", existing)

	desired := &fleet.Bundle{
		ObjectMeta: metav1.ObjectMeta{
			Name:      existing.Name,
			Namespace: existing.Namespace,
		},
		Spec: fleet.BundleSpec{
			Resources: []fleet.BundleResource{{Name: "cm.yaml", Content: "key: value\n"}},
		},
	}
	newManifestID, err := manifest.FromBundle(desired).ID()
	if err != nil {
		t.Fatal(err)
	}
	if newManifestID == "s-old" {
		t.Fatal("test setup expects a new contents id")
	}

	var contentsIDWhenSecretWritten string
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(existing, oldSecret).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if secret, ok := obj.(*corev1.Secret); ok && secret.Name == newManifestID {
					bundle := &fleet.Bundle{}
					if err := c.Get(ctx, client.ObjectKeyFromObject(existing), bundle); err != nil {
						return err
					}
					contentsIDWhenSecretWritten = bundle.Spec.ContentsID
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()

	store := &stubOCIStore{}
	newOCIManifestStore = func() ociManifestStore { return store }
	t.Cleanup(func() {
		newOCIManifestStore = func() ociManifestStore { return ocistorage.NewOCIWrapper() }
	})

	saved, err := saveOCIBundle(context.Background(), kubeClient, desired, ocistorage.OCIOpts{
		Reference: "registry.example.com/test",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// The new artifact was pushed.
	if len(store.pushedIDs) != 1 || store.pushedIDs[0] != newManifestID {
		t.Fatalf("unexpected pushed ids: %v", store.pushedIDs)
	}

	// The Bundle switches to the new generation.
	if saved.Spec.ContentsID != newManifestID {
		t.Fatalf("expected contents id %q, got %q", newManifestID, saved.Spec.ContentsID)
	}
	if len(saved.Spec.Resources) != 0 {
		t.Fatalf("expected resources to be cleared, got %d", len(saved.Spec.Resources))
	}

	// The access secret for the new generation existed before the Bundle switched.
	if contentsIDWhenSecretWritten != "s-old" {
		t.Fatalf("expected the new access secret to be written while the bundle still points at s-old, got %q", contentsIDWhenSecretWritten)
	}

	// The new access secret exists.
	if err := kubeClient.Get(context.Background(), types.NamespacedName{
		Name: newManifestID, Namespace: existing.Namespace,
	}, &corev1.Secret{}); err != nil {
		t.Fatalf("expected new OCI access secret, got %v", err)
	}

	// The previous generation is untouched, it is garbage-collected later.
	if err := kubeClient.Get(context.Background(), types.NamespacedName{
		Name: "s-old", Namespace: existing.Namespace,
	}, &corev1.Secret{}); err != nil {
		t.Fatalf("expected the previous generation secret to be retained, got %v", err)
	}
}

func TestSaveOCIBundle_PushFailureKeepsPreviousGeneration(t *testing.T) {
	scheme := ociTestScheme(t)

	existing := &fleet.Bundle{
		ObjectMeta: metav1.ObjectMeta{Name: "my-bundle", Namespace: "fleet-local", UID: types.UID("uid")},
		Spec:       fleet.BundleSpec{ContentsID: "s-old"},
	}
	oldSecret := ociGenerationSecret("s-old", existing)
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing, oldSecret).Build()

	store := &stubOCIStore{err: errors.New("registry down")}
	newOCIManifestStore = func() ociManifestStore { return store }
	t.Cleanup(func() {
		newOCIManifestStore = func() ociManifestStore { return ocistorage.NewOCIWrapper() }
	})

	desired := &fleet.Bundle{
		ObjectMeta: metav1.ObjectMeta{Name: existing.Name, Namespace: existing.Namespace},
		Spec: fleet.BundleSpec{
			Resources: []fleet.BundleResource{{Name: "cm.yaml", Content: "key: value\n"}},
		},
	}
	if _, err := saveOCIBundle(context.Background(), kubeClient, desired, ocistorage.OCIOpts{
		Reference: "registry.example.com/test",
	}); err == nil {
		t.Fatal("expected push error, got nil")
	}

	bundle := &fleet.Bundle{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(existing), bundle); err != nil {
		t.Fatal(err)
	}
	if bundle.Spec.ContentsID != "s-old" {
		t.Fatalf("expected bundle to stay on the previous generation, got %q", bundle.Spec.ContentsID)
	}

	secrets := &corev1.SecretList{}
	if err := kubeClient.List(context.Background(), secrets, client.InNamespace(existing.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(secrets.Items) != 1 || secrets.Items[0].Name != "s-old" {
		t.Fatalf("expected only the previous generation secret, got %d secrets", len(secrets.Items))
	}
}

func TestSaveOCIBundle_SecretWriteFailureKeepsPreviousGeneration(t *testing.T) {
	scheme := ociTestScheme(t)

	existing := &fleet.Bundle{
		ObjectMeta: metav1.ObjectMeta{Name: "my-bundle", Namespace: "fleet-local", UID: types.UID("uid")},
		Spec:       fleet.BundleSpec{ContentsID: "s-old"},
	}
	oldSecret := ociGenerationSecret("s-old", existing)

	desired := &fleet.Bundle{
		ObjectMeta: metav1.ObjectMeta{Name: existing.Name, Namespace: existing.Namespace},
		Spec: fleet.BundleSpec{
			Resources: []fleet.BundleResource{{Name: "cm.yaml", Content: "key: value\n"}},
		},
	}
	newManifestID, err := manifest.FromBundle(desired).ID()
	if err != nil {
		t.Fatal(err)
	}

	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(existing, oldSecret).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if secret, ok := obj.(*corev1.Secret); ok && secret.Name == newManifestID {
					return errors.New("cannot write secret")
				}
				return c.Create(ctx, obj, opts...)
			},
		}).Build()

	newOCIManifestStore = func() ociManifestStore { return &stubOCIStore{} }
	t.Cleanup(func() {
		newOCIManifestStore = func() ociManifestStore { return ocistorage.NewOCIWrapper() }
	})

	if _, err := saveOCIBundle(context.Background(), kubeClient, desired, ocistorage.OCIOpts{
		Reference: "registry.example.com/test",
	}); err == nil {
		t.Fatal("expected secret write error, got nil")
	}

	bundle := &fleet.Bundle{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(existing), bundle); err != nil {
		t.Fatal(err)
	}
	if bundle.Spec.ContentsID != "s-old" {
		t.Fatalf("expected bundle to stay on the previous generation, got %q", bundle.Spec.ContentsID)
	}
}

func TestSaveOCIBundle_BundlePublishFailureKeepsPreviousGeneration(t *testing.T) {
	scheme := ociTestScheme(t)

	existing := &fleet.Bundle{
		ObjectMeta: metav1.ObjectMeta{Name: "my-bundle", Namespace: "fleet-local", UID: types.UID("uid")},
		Spec:       fleet.BundleSpec{ContentsID: "s-old"},
	}
	oldSecret := ociGenerationSecret("s-old", existing)

	desired := &fleet.Bundle{
		ObjectMeta: metav1.ObjectMeta{Name: existing.Name, Namespace: existing.Namespace},
		Spec: fleet.BundleSpec{
			Resources: []fleet.BundleResource{{Name: "cm.yaml", Content: "key: value\n"}},
		},
	}
	newManifestID, err := manifest.FromBundle(desired).ID()
	if err != nil {
		t.Fatal(err)
	}

	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(existing, oldSecret).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, ok := obj.(*fleet.Bundle); ok {
					return errors.New("cannot publish bundle")
				}
				return c.Update(ctx, obj, opts...)
			},
		}).Build()

	newOCIManifestStore = func() ociManifestStore { return &stubOCIStore{} }
	t.Cleanup(func() {
		newOCIManifestStore = func() ociManifestStore { return ocistorage.NewOCIWrapper() }
	})

	if _, err := saveOCIBundle(context.Background(), kubeClient, desired, ocistorage.OCIOpts{
		Reference: "registry.example.com/test",
	}); err == nil {
		t.Fatal("expected bundle publish error, got nil")
	}

	// The new generation is complete (the secret was written), but the
	// Bundle still points at the previous generation and can retry later.
	bundle := &fleet.Bundle{}
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(existing), bundle); err != nil {
		t.Fatal(err)
	}
	if bundle.Spec.ContentsID != "s-old" {
		t.Fatalf("expected bundle to stay on the previous generation, got %q", bundle.Spec.ContentsID)
	}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{
		Name: newManifestID, Namespace: existing.Namespace,
	}, &corev1.Secret{}); err != nil {
		t.Fatalf("expected new generation secret to exist for retry, got %v", err)
	}
}

func TestSaveOCIBundle_FirstPublishKeepsOriginalOrdering(t *testing.T) {
	scheme := ociTestScheme(t)

	kubeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	desired := &fleet.Bundle{
		ObjectMeta: metav1.ObjectMeta{Name: "new-bundle", Namespace: "fleet-local"},
		Spec: fleet.BundleSpec{
			Resources: []fleet.BundleResource{{Name: "cm.yaml", Content: "key: value\n"}},
		},
	}
	newManifestID, err := manifest.FromBundle(desired).ID()
	if err != nil {
		t.Fatal(err)
	}

	newOCIManifestStore = func() ociManifestStore { return &stubOCIStore{} }
	t.Cleanup(func() {
		newOCIManifestStore = func() ociManifestStore { return ocistorage.NewOCIWrapper() }
	})

	saved, err := saveOCIBundle(context.Background(), kubeClient, desired, ocistorage.OCIOpts{
		Reference: "registry.example.com/test",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if saved.Spec.ContentsID != newManifestID {
		t.Fatalf("expected contents id %q, got %q", newManifestID, saved.Spec.ContentsID)
	}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{
		Name: newManifestID, Namespace: desired.Namespace,
	}, &corev1.Secret{}); err != nil {
		t.Fatalf("expected the OCI access secret to be created, got %v", err)
	}
}

func TestSaveOCIBundle_UnchangedContentKeepsGeneration(t *testing.T) {
	scheme := ociTestScheme(t)

	desired := &fleet.Bundle{
		ObjectMeta: metav1.ObjectMeta{Name: "my-bundle", Namespace: "fleet-local"},
		Spec: fleet.BundleSpec{
			Resources: []fleet.BundleResource{{Name: "cm.yaml", Content: "key: value\n"}},
		},
	}
	contentsID, err := manifest.FromBundle(desired).ID()
	if err != nil {
		t.Fatal(err)
	}

	existing := &fleet.Bundle{
		ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace, UID: types.UID("uid")},
		Spec:       fleet.BundleSpec{ContentsID: contentsID},
	}
	secret := ociGenerationSecret(contentsID, existing)
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing, secret).Build()

	newOCIManifestStore = func() ociManifestStore { return &stubOCIStore{} }
	t.Cleanup(func() {
		newOCIManifestStore = func() ociManifestStore { return ocistorage.NewOCIWrapper() }
	})

	saved, err := saveOCIBundle(context.Background(), kubeClient, desired, ocistorage.OCIOpts{
		Reference: "registry.example.com/test",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if saved.Spec.ContentsID != contentsID {
		t.Fatalf("expected contents id to remain %q, got %q", contentsID, saved.Spec.ContentsID)
	}
	if err := kubeClient.Get(context.Background(), types.NamespacedName{
		Name: contentsID, Namespace: desired.Namespace,
	}, &corev1.Secret{}); err != nil {
		t.Fatalf("expected the access secret to remain, got %v", err)
	}
}
