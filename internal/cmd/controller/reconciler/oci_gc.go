package reconciler

import (
	"context"
	"strings"

	"github.com/rancher/fleet/internal/cmd/agent/deployer/kv"
	"github.com/rancher/fleet/internal/ocistorage"
	fleet "github.com/rancher/fleet/pkg/apis/fleet.cattle.io/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ociArtifactDeleter deletes artifacts from an OCI registry.
type ociArtifactDeleter interface {
	DeleteManifest(ctx context.Context, opts ocistorage.OCIOpts, id string) error
}

// newOCIArtifactDeleter builds the OCI registry client used during garbage
// collection. It is a variable so tests can replace the registry client.
var newOCIArtifactDeleter = func() ociArtifactDeleter {
	return ocistorage.NewOCIWrapper()
}

// cleanupUnreferencedOCIArtifacts garbage-collects older OCI generations of
// the given Bundle. A generation consists of the OCI registry artifact and
// the per-generation access Secret in the Bundle's namespace.
//
// A generation is only collected once no non-deleted BundleDeployment
// references its ContentsID anymore, neither in its current nor in its
// staged DeploymentID. Failures while deleting an artifact from the
// (external) OCI registry are reported as Warning events and do not block
// the current generation: the access Secret is kept, so the deletion is
// retried on a later reconcile.
func (r *BundleReconciler) cleanupUnreferencedOCIArtifacts(ctx context.Context, bundle *fleet.Bundle) error {
	if bundle.Spec.ContentsID == "" || !ocistorage.OCIIsEnabled() {
		return nil
	}

	referenced, err := r.referencedContentsIDs(ctx, bundle)
	if err != nil {
		return err
	}
	// The current generation must never be collected.
	referenced.Insert(bundle.Spec.ContentsID)

	secrets := &corev1.SecretList{}
	if err := r.List(ctx, secrets, client.InNamespace(bundle.Namespace)); err != nil {
		return err
	}

	for i := range secrets.Items {
		secret := &secrets.Items[i]
		if !isOCIGenerationSecret(secret, bundle) || referenced.Has(secret.Name) {
			continue
		}
		r.deleteUnreferencedOCIArtifact(ctx, bundle, secret)
	}

	return nil
}

// referencedContentsIDs returns the ContentsIDs of all generations of
// bundle that a non-deleted BundleDeployment still uses as its current or
// staged deployment.
func (r *BundleReconciler) referencedContentsIDs(ctx context.Context, bundle *fleet.Bundle) (sets.Set[string], error) {
	bds, err := r.listBundleDeploymentsForBundle(ctx, bundle)
	if err != nil {
		return nil, err
	}

	ids := sets.New[string]()
	for i := range bds {
		bd := &bds[i]
		if !bd.DeletionTimestamp.IsZero() {
			continue
		}
		ids.Insert(contentsIDFromDeploymentID(bd.Spec.DeploymentID))
		ids.Insert(contentsIDFromDeploymentID(bd.Spec.StagedDeploymentID))
	}
	ids.Delete("")

	return ids, nil
}

// contentsIDFromDeploymentID extracts the ContentsID from a
// BundleDeployment DeploymentID, which has the format
// "<contentsID>:<optionsHash>".
func contentsIDFromDeploymentID(deploymentID string) string {
	id, _ := kv.Split(deploymentID, ":")
	return id
}

// isOCIGenerationSecret reports whether the given Secret is one of the
// per-generation OCI access Secrets created by Fleet for bundle, as opposed
// to the user-provided OCI registry configuration Secret.
func isOCIGenerationSecret(secret *corev1.Secret, bundle *fleet.Bundle) bool {
	if secret.Type != corev1.SecretType(fleet.SecretTypeOCIStorage) {
		return false
	}

	for _, owner := range secret.OwnerReferences {
		if owner.Controller != nil && *owner.Controller &&
			owner.Kind == "Bundle" && owner.Name == bundle.Name &&
			owner.APIVersion == fleet.SchemeGroupVersion.String() {
			return true
		}
	}

	return false
}

// deleteUnreferencedOCIArtifact deletes the registry artifact of one
// unreferenced generation and then its access Secret. Errors are
// best-effort: they are logged and reported as Warning events, but never
// block the current generation.
func (r *BundleReconciler) deleteUnreferencedOCIArtifact(ctx context.Context, bundle *fleet.Bundle, secret *corev1.Secret) {
	contentsID := secret.Name
	logger := log.FromContext(ctx).WithValues("contentsID", contentsID)

	opts, err := ocistorage.ReadOptsFromSecret(ctx, r.Client, types.NamespacedName{
		Namespace: secret.Namespace,
		Name:      contentsID,
	})
	if err != nil {
		logger.Info("failed to read stale OCI access secret, keeping it for a later retry", "error", err)
		r.recordOCIArtifactDeletionFailure(bundle, contentsID, err)
		return
	}

	if err := newOCIArtifactDeleter().DeleteManifest(ctx, opts, contentsID); err != nil {
		if !isOCINotFoundError(err) {
			// The registry is an external component Fleet does not control.
			// Keep the access Secret, warn the user, and retry on a later
			// reconcile instead of blocking the new generation.
			logger.Info("failed to delete stale OCI artifact, keeping it for a later retry", "error", err)
			r.recordOCIArtifactDeletionFailure(bundle, contentsID, err)
			return
		}
		logger.V(1).Info("stale OCI artifact no longer exists in the registry")
	}

	if err := r.Delete(ctx, secret); client.IgnoreNotFound(err) != nil {
		logger.Info("failed to delete stale OCI access secret, keeping it for a later retry", "error", err)
	}
}

// recordOCIArtifactDeletionFailure emits the Warning event used to signal
// failed OCI artifact deletions, matching the existing event semantics.
func (r *BundleReconciler) recordOCIArtifactDeletionFailure(bundle *fleet.Bundle, contentsID string, deleteErr error) {
	r.Recorder.Eventf(
		bundle,
		nil,
		corev1.EventTypeWarning,
		"FailedToDeleteOCIArtifact",
		"DeleteOCIArtifact",
		"deleting OCI artifact %q: %v",
		contentsID,
		"%v",
		deleteErr,
	)
}

// isOCINotFoundError reports whether err indicates the OCI artifact is
// already gone from the registry. OCI registry clients do not expose a
// shared typed "not found" error, so this matches on the error message, as
// done elsewhere for OCI registry errors.
func isOCINotFoundError(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "not found")
}
