// Package clusterregistration implements manager-initiated and agent-initiated registration.
//
// Add or import downstream clusters / agents to Fleet and keep information
// from their registration (e.g. local cluster kubeconfig) up-to-date.
package clusterregistration

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rancher/fleet/internal/cmd/controller/agentmanagement/controllers/resources"
	secretutil "github.com/rancher/fleet/internal/cmd/controller/agentmanagement/secret"
	"github.com/rancher/fleet/internal/config"
	"github.com/rancher/fleet/internal/names"
	"github.com/rancher/fleet/internal/registration"
	fleet "github.com/rancher/fleet/pkg/apis/fleet.cattle.io/v1alpha1"
	"github.com/rancher/fleet/pkg/durations"
	fleetcontrollers "github.com/rancher/fleet/pkg/generated/controllers/fleet.cattle.io/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/rancher/wrangler/v3/pkg/apply"
	corecontrollers "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	rbaccontrollers "github.com/rancher/wrangler/v3/pkg/generated/controllers/rbac/v1"
	"github.com/rancher/wrangler/v3/pkg/generic"
	"github.com/rancher/wrangler/v3/pkg/relatedresource"

	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	AgentCredentialSecretType     = "fleet.cattle.io/agent-credential" //nolint:gosec // not a credential
	clusterByClientID             = "clusterByClientID"
	clusterRegistrationByClientID = "clusterRegistrationByClientID"
	deleteSecretAfter             = durations.ClusterRegistrationDeleteDelay
)

// oldRegistrationGrace is how long older generations of a ClusterRegistration
// are retained after a newer, granted generation for the same client ID has
// appeared. While this overlap window is open, both generations' request
// service accounts and token Secrets stay valid, so an agent that has not
// picked up the new credential yet keeps working and a still-registering
// agent can retry. Only once the window elapses does the newest granted
// generation prune the older ones, and only those expired generations (plus
// the resources they own) are removed.
var oldRegistrationGrace = durations.ClusterRegistrationDeleteDelay

type handler struct {
	systemNamespace             string
	systemRegistrationNamespace string
	clusterRegistration         fleetcontrollers.ClusterRegistrationController
	clusterCache                fleetcontrollers.ClusterCache
	clusters                    fleetcontrollers.ClusterClient
	serviceAccountCache         corecontrollers.ServiceAccountCache
	secretsCache                corecontrollers.SecretCache
	secrets                     corecontrollers.SecretController
	tokenCache                  fleetcontrollers.ClusterRegistrationTokenCache
}

func Register(ctx context.Context,
	apply apply.Apply,
	systemNamespace string,
	systemRegistrationNamespace string,
	serviceAccount corecontrollers.ServiceAccountController,
	secret corecontrollers.SecretController,
	role rbaccontrollers.RoleController,
	roleBinding rbaccontrollers.RoleBindingController,
	clusterRegistration fleetcontrollers.ClusterRegistrationController,
	clusters fleetcontrollers.ClusterController,
	clusterRegistrationToken fleetcontrollers.ClusterRegistrationTokenController) {
	h := &handler{
		systemNamespace:             systemNamespace,
		systemRegistrationNamespace: systemRegistrationNamespace,
		clusterRegistration:         clusterRegistration,
		clusterCache:                clusters.Cache(),
		clusters:                    clusters,
		serviceAccountCache:         serviceAccount.Cache(),
		secrets:                     secret,
		secretsCache:                secret.Cache(),
		tokenCache:                  clusterRegistrationToken.Cache(),
	}

	fleetcontrollers.RegisterClusterRegistrationGeneratingHandler(ctx,
		clusterRegistration,
		apply.WithCacheTypes(serviceAccount,
			secret,
			role,
			roleBinding,
		),
		"",
		"cluster-registration",
		h.OnChange,
		&generic.GeneratingHandlerOptions{
			AllowClusterScoped: true,
		})

	secret.OnChange(ctx, "registration-expire", h.OnSecretChange)
	clusterRegistration.OnChange(ctx, "cluster-registration-grace-gc", h.OnRegistrationChange)
	clusters.OnChange(ctx, "cluster-to-clusterregistration", h.OnCluster)
	clusters.Cache().AddIndexer(clusterByClientID, func(obj *fleet.Cluster) ([]string, error) {
		return []string{
			fmt.Sprintf("%s/%s", obj.Namespace, obj.Spec.ClientID),
		}, nil
	})
	clusterRegistration.Cache().AddIndexer(clusterRegistrationByClientID, func(obj *fleet.ClusterRegistration) ([]string, error) {
		return []string{
			fmt.Sprintf("%s/%s", obj.Namespace, obj.Spec.ClientID),
		}, nil
	})
	relatedresource.Watch(ctx, "sa-to-cluster-registration", saToClusterRegistration, clusterRegistration, serviceAccount)
}

func saToClusterRegistration(namespace, name string, obj runtime.Object) ([]relatedresource.Key, error) {
	if sa, ok := obj.(*v1.ServiceAccount); ok {
		ns := sa.Annotations[fleet.ClusterRegistrationNamespaceAnnotation]
		name := sa.Annotations[fleet.ClusterRegistrationAnnotation]
		if ns != "" && name != "" {
			return []relatedresource.Key{{
				Namespace: ns,
				Name:      name,
			}}, nil
		}
	}
	return nil, nil
}

func (h *handler) OnCluster(key string, cluster *fleet.Cluster) (*fleet.Cluster, error) {
	if cluster == nil || cluster.Status.Namespace == "" {
		return cluster, nil
	}

	crs, err := h.clusterRegistration.Cache().GetByIndex(clusterRegistrationByClientID,
		fmt.Sprintf("%s/%s", cluster.Namespace, cluster.Spec.ClientID))
	if err != nil {
		return nil, err
	}
	for _, cr := range crs {
		if !cr.Status.Granted {
			log.Log.Info(fmt.Sprintf("Namespace assigned to cluster '%s/%s' enqueues cluster registration '%s/%s'", cluster.Namespace, cluster.Name,
				cr.Namespace, cr.Name))
			h.clusterRegistration.Enqueue(cr.Namespace, cr.Name)
		}
	}

	return cluster, nil
}

func (h *handler) OnSecretChange(key string, secret *v1.Secret) (*v1.Secret, error) {
	if secret == nil || secret.Namespace != h.systemRegistrationNamespace ||
		secret.Labels[fleet.ClusterAnnotation] == "" {
		return secret, nil
	}

	if time.Since(secret.CreationTimestamp.Time) > deleteSecretAfter {
		log.Log.Info(fmt.Sprintf("Deleting expired registration secret %s/%s", secret.Namespace, secret.Name))
		return secret, h.secrets.Delete(secret.Namespace, secret.Name, nil)
	}

	h.secrets.EnqueueAfter(secret.Namespace, secret.Name, deleteSecretAfter/2)
	return secret, nil
}

func skipClusterRegistration(cr *fleet.ClusterRegistration) bool {
	if cr == nil {
		return true
	}
	if cr.Labels == nil {
		return false
	}
	if cr.Labels[fleet.ClusterManagementLabel] != "" {
		return true
	}
	return false
}

// OnChange creates the service account and roles for a cluster registration.
// The service account's token is deployed to the downstream cluster, via the
// fleet-secret. It allows the downstream fleet-agent to list
// bundledeployments and update their status in its own cluster namespace on upstream.
// It can also get content resources, but not list them. The name of content
// resources is random.
func (h *handler) OnChange(request *fleet.ClusterRegistration, status fleet.ClusterRegistrationStatus) ([]runtime.Object, fleet.ClusterRegistrationStatus, error) {
	if status.Granted {
		// only create the cluster for the request once
		return nil, status, generic.ErrSkip
	}
	if skipClusterRegistration(request) {
		return nil, status, generic.ErrSkip
	}

	cluster, err := h.createOrGetCluster(request)
	if err != nil || cluster == nil {
		return nil, status, err
	}

	if cluster.Status.Namespace == "" {
		status.ClusterName = cluster.Name
		return nil, status, nil
	}

	// set the Cluster as owner of the cluster registration request
	// ownerFound is used to avoid calling update on request whenever OnChange is called
	ownerFound := false
	for _, owner := range request.OwnerReferences {
		if owner.Kind == "Cluster" && owner.Name == cluster.Name && owner.UID == cluster.UID {
			ownerFound = true
			break
		}
	}
	if !ownerFound {
		request.SetOwnerReferences([]metav1.OwnerReference{
			{
				APIVersion: fleet.SchemeGroupVersion.String(),
				Kind:       "Cluster",
				Name:       cluster.Name,
				UID:        cluster.UID,
			},
		})
		request, err = h.clusterRegistration.Update(request)
		if err != nil {
			return nil, status, err
		}
	}

	saName := names.SafeConcatName(request.Name, string(request.UID))
	sa, err := h.serviceAccountCache.Get(cluster.Status.Namespace, saName)
	if err != nil && apierrors.IsNotFound(err) {
		// create request service account if missing
		status.ClusterName = cluster.Name
		return []runtime.Object{requestSA(saName, cluster, request)}, status, nil
	} else if err != nil {
		return nil, status, fmt.Errorf("failed to retrieve service account from cache: %w", err)
	}

	// try to get request service account's token
	var secret *v1.Secret
	if secret, err = h.authorizeCluster(sa, cluster, request); err != nil {
		return nil, status, fmt.Errorf("failed to authorize cluster, cannot get service account token: %w", err)
	} else if secret == nil {
		status.ClusterName = cluster.Name
		log.Log.Info(fmt.Sprintf("Cluster registration request '%s/%s', cluster '%s/%s' not granted, waiting for service account token",
			request.Namespace, request.Name, cluster.Namespace, cluster.Name))
		return nil, status, nil
	}

	// Older generations of this client ID's registration are NOT deleted
	// here. Granting the new request must not invalidate the request
	// service account token an already working agent is still using while
	// the credential rotation settles. Superseded generations are reaped
	// with an overlap grace period by OnRegistrationChange.

	// request is granted, create the registration secret and roles
	status.ClusterName = cluster.Name
	status.Granted = true
	log.Log.Info(fmt.Sprintf("Cluster registration request '%s/%s' granted, creating cluster, request service account, registration secret", request.Namespace, request.Name))

	objs := []runtime.Object{
		// the registration secret c-clientID-clientRandom
		secret,
		// Update the existing service account 'request-UID' in the
		// cluster namespace, e.g. 'cluster-fleet-default-NAME-ID'
		requestSA(saName, cluster, request),
		// Add role bindings to manage bundledeployments and contents,
		// the agent could previously only access clusterregistrations in
		// the cluster registration namespace (e.g. 'fleet-default'). See
		// clusterregistrationtoken controller for details.
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{
				Name:      request.Name,
				Namespace: request.Namespace,
				Labels: map[string]string{
					fleet.ManagedLabel: "true",
				},
			},
			Rules: []rbacv1.PolicyRule{
				{
					Verbs:         []string{"patch"},
					APIGroups:     []string{fleet.SchemeGroupVersion.Group},
					Resources:     []string{fleet.ClusterResourceNamePlural + "/status"},
					ResourceNames: []string{cluster.Name},
				},
			},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      request.Name,
				Namespace: request.Namespace,
				Labels: map[string]string{
					fleet.ManagedLabel: "true",
				},
			},
			Subjects: []rbacv1.Subject{
				{
					Kind:      "ServiceAccount",
					Name:      saName,
					Namespace: cluster.Status.Namespace,
				},
			},
			RoleRef: rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName,
				Kind:     "Role",
				Name:     request.Name,
			},
		},
		// cluster role "fleet-bundle-deployment" created when
		// fleet-controller starts
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      request.Name,
				Namespace: cluster.Status.Namespace,
				Labels: map[string]string{
					fleet.ManagedLabel: "true",
				},
			},
			Subjects: []rbacv1.Subject{
				{
					Kind:      "ServiceAccount",
					Name:      saName,
					Namespace: cluster.Status.Namespace,
				},
			},
			RoleRef: rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName,
				Kind:     "ClusterRole",
				Name:     resources.BundleDeploymentClusterRole,
			},
		},
		// cluster role "fleet-content" created when fleet-controller
		// starts
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: names.SafeConcatName(request.Name, "content"),
				Labels: map[string]string{
					fleet.ManagedLabel: "true",
				},
			},
			Subjects: []rbacv1.Subject{
				{
					Kind:      "ServiceAccount",
					Name:      saName,
					Namespace: cluster.Status.Namespace,
				},
			},
			RoleRef: rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName,
				Kind:     "ClusterRole",
				Name:     resources.ContentClusterRole,
			},
		},
	}

	// For manager-initiated imports, grant the import service account access
	// to the specific credential secret it needs to read. The import token
	// only exists in manager-initiated flows; agent-initiated flows skip this.
	importTokenName := names.SafeConcatName(config.ImportTokenPrefix + cluster.Name)
	importToken, err := h.tokenCache.Get(request.Namespace, importTokenName)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, status, fmt.Errorf("failed to look up import token %s/%s: %w", request.Namespace, importTokenName, err)
	}
	if err == nil {
		// Grant access scoped to the single credential secret for this cluster.
		importSAName := names.SafeConcatName(importTokenName, string(importToken.UID))
		credRoleName := names.SafeConcatName(importSAName, "creds")
		objs = append(objs,
			&rbacv1.Role{
				ObjectMeta: metav1.ObjectMeta{
					Name:      credRoleName,
					Namespace: h.systemRegistrationNamespace,
					Labels: map[string]string{
						fleet.ManagedLabel: "true",
					},
				},
				Rules: []rbacv1.PolicyRule{
					{
						Verbs:         []string{"get"},
						APIGroups:     []string{""},
						Resources:     []string{"secrets"},
						ResourceNames: []string{secret.Name},
					},
				},
			},
			&rbacv1.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name:      credRoleName,
					Namespace: h.systemRegistrationNamespace,
					Labels: map[string]string{
						fleet.ManagedLabel: "true",
					},
				},
				Subjects: []rbacv1.Subject{
					{
						Kind:      "ServiceAccount",
						Name:      importSAName,
						Namespace: request.Namespace,
					},
				},
				RoleRef: rbacv1.RoleRef{
					APIGroup: rbacv1.GroupName,
					Kind:     "Role",
					Name:     credRoleName,
				},
			},
		)
	} else {
		// Agent-initiated: no import-token-<cluster> exists because the cluster
		// is auto-created at registration time. The ClusterRegistration carries
		// a label identifying the token that was used, so we can grant only
		// that token's SA scoped read access to the credential secret.
		tokenName := request.Labels[fleet.RegistrationTokenLabel]
		if tokenName == "" {
			log.Log.Info(fmt.Sprintf("ClusterRegistration '%s/%s' has no %s label; cannot grant credential secret access for agent-initiated registration",
				request.Namespace, request.Name, fleet.RegistrationTokenLabel))
		} else {
			agentToken, tokenErr := h.tokenCache.Get(request.Namespace, tokenName)
			if tokenErr != nil && !apierrors.IsNotFound(tokenErr) {
				return nil, status, fmt.Errorf("looking up registration token %s/%s: %w", request.Namespace, tokenName, tokenErr)
			}
			if apierrors.IsNotFound(tokenErr) {
				log.Log.Info(fmt.Sprintf("ClusterRegistration '%s/%s': registration token %s/%s not found (may have expired), skipping credential secret access grant",
					request.Namespace, request.Name, request.Namespace, tokenName))
				return objs, status, nil
			}
			agentSAName := names.SafeConcatName(tokenName, string(agentToken.UID))
			credRoleName := names.SafeConcatName(request.Name, "creds")
			objs = append(objs,
				&rbacv1.Role{
					ObjectMeta: metav1.ObjectMeta{
						Name:      credRoleName,
						Namespace: h.systemRegistrationNamespace,
						Labels: map[string]string{
							fleet.ManagedLabel: "true",
						},
					},
					Rules: []rbacv1.PolicyRule{
						{
							Verbs:         []string{"get"},
							APIGroups:     []string{""},
							Resources:     []string{"secrets"},
							ResourceNames: []string{secret.Name},
						},
					},
				},
				&rbacv1.RoleBinding{
					ObjectMeta: metav1.ObjectMeta{
						Name:      credRoleName,
						Namespace: h.systemRegistrationNamespace,
						Labels: map[string]string{
							fleet.ManagedLabel: "true",
						},
					},
					Subjects: []rbacv1.Subject{
						{
							Kind:      "ServiceAccount",
							Name:      agentSAName,
							Namespace: request.Namespace,
						},
					},
					RoleRef: rbacv1.RoleRef{
						APIGroup: rbacv1.GroupName,
						Kind:     "Role",
						Name:     credRoleName,
					},
				},
			)
		}
	}

	return objs, status, nil
}

// OnRegistrationChange garbage collects superseded ClusterRegistration
// generations with an overlap grace period. It runs as a separate handler,
// because the generating handler skips requests that are already granted and
// would therefore never re-evaluate the window.
//
// Only the newest GRANTED generation for a client ID may delete older
// generations, and only after it has existed for oldRegistrationGrace. This
// keeps the request service account token of a generation an agent is still
// using valid while a credential rotation settles, and ensures registration
// retries (each of which creates another in-flight request and resets the
// newest generation) never remove the in-use generation. Ungranted requests
// are ignored, so a failed, half-finished registration cannot delete
// anything.
//
// When a generation is eventually deleted, the generating handler prunes
// exactly that generation's owned objects (service account, token Secret,
// registration Secret and RBAC), nothing else.
func (h *handler) OnRegistrationChange(_ string, request *fleet.ClusterRegistration) (*fleet.ClusterRegistration, error) {
	if request == nil || !request.Status.Granted || skipClusterRegistration(request) {
		return request, nil
	}

	crlist, err := h.clusterRegistration.List(request.Namespace, metav1.ListOptions{})
	if err != nil {
		return request, err
	}

	newest := newestGranted(crlist.Items, request.Spec.ClientID)
	// Only the newest granted generation schedules and performs the sweep.
	// If a newer generation exists, it re-enqueues itself when it gets
	// granted. Deleting from an intermediate generation could remove the
	// generation the agent is still using while an even newer registration
	// is in flight.
	if newest == nil || newest.Name != request.Name {
		return request, nil
	}

	if wait := time.Until(newest.CreationTimestamp.Time.Add(oldRegistrationGrace)); wait > 0 {
		// The overlap window is still open: keep every older generation's
		// credentials valid and re-check once the window elapses.
		h.clusterRegistration.EnqueueAfter(newest.Namespace, newest.Name, wait)
		return request, nil
	}

	if inFlight := newerInFlight(crlist.Items, newest); inFlight != nil {
		// A newer registration has been created but is not granted yet, i.e.
		// an agent is still trying to register. It will either get granted,
		// resetting the overlap window, or fail. Do not reap the generation
		// another agent may still be serving with until the in-flight
		// registration settles.
		log.Log.V(1).Info(fmt.Sprintf("Keeping expired clusterregistrations for client ID %q, registration '%s/%s' is still in flight",
			newest.Spec.ClientID, inFlight.Namespace, inFlight.Name))
		h.clusterRegistration.EnqueueAfter(newest.Namespace, newest.Name, oldRegistrationGrace/2)
		return request, nil
	}

	for i := range crlist.Items {
		creg := crlist.Items[i]
		if !shouldDelete(creg, *newest) {
			continue
		}
		log.Log.Info(fmt.Sprintf("Deleting expired clusterregistration '%s/%s', superseded by '%s/%s' for longer than the grace period",
			creg.Namespace, creg.Name, newest.Namespace, newest.Name))
		if err := h.clusterRegistration.Delete(creg.Namespace, creg.Name, nil); err != nil && !apierrors.IsNotFound(err) {
			return request, err
		}
	}

	return request, nil
}

// newestGranted returns the most recently created granted ClusterRegistration
// for the given client ID, or nil if there is none.
func newestGranted(items []fleet.ClusterRegistration, clientID string) *fleet.ClusterRegistration {
	var newest *fleet.ClusterRegistration
	for i := range items {
		cr := &items[i]
		if !cr.Status.Granted || cr.Spec.ClientID != clientID {
			continue
		}
		if newest == nil || cr.CreationTimestamp.Time.After(newest.CreationTimestamp.Time) {
			newest = cr
		}
	}
	return newest
}

// newerInFlight returns an ungranted ClusterRegistration for clientID that
// is newer than newestGranted, if one exists. Such a request means an agent
// is still trying to register, so older generations must not be reaped yet.
func newerInFlight(items []fleet.ClusterRegistration, newestGranted *fleet.ClusterRegistration) *fleet.ClusterRegistration {
	for i := range items {
		cr := &items[i]
		if cr.Status.Granted || cr.Spec.ClientID != newestGranted.Spec.ClientID {
			continue
		}
		if cr.CreationTimestamp.Time.After(newestGranted.CreationTimestamp.Time) {
			return cr
		}
	}
	return nil
}

// shouldDelete returns true for any other granted-or-not cluster registration
// with the same clientID, but different random and an older creation
// timestamp. Callers only invoke it from the newest granted generation and
// only after the overlap grace period, see OnRegistrationChange.
func shouldDelete(creg fleet.ClusterRegistration, request fleet.ClusterRegistration) bool {
	return creg.Spec.ClientID == request.Spec.ClientID &&
		creg.Spec.ClientRandom != request.Spec.ClientRandom &&
		creg.Name != request.Name &&
		creg.CreationTimestamp.Time.Before(request.CreationTimestamp.Time)
}

func (h *handler) createOrGetCluster(request *fleet.ClusterRegistration) (*fleet.Cluster, error) {
	clusters, err := h.clusterCache.GetByIndex(clusterByClientID, fmt.Sprintf("%s/%s", request.Namespace, request.Spec.ClientID))
	if err == nil && len(clusters) > 0 {
		return clusters[0], nil
	} else if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}

	clusterName := names.SafeConcatName("cluster", names.KeyHash(request.Spec.ClientID))
	if cluster, err := h.clusterCache.Get(request.Namespace, clusterName); !apierrors.IsNotFound(err) {
		if cluster.Spec.ClientID != request.Spec.ClientID {
			// This would happen with a hash collision
			return nil, fmt.Errorf("non-matching ClientID on cluster %s/%s got %s expected %s",
				request.Namespace, clusterName, cluster.Spec.ClientID, request.Spec.ClientID)
		}
		return cluster, err
	}

	// need to create the cluster for agent initiated registration, local
	// and managed clusters would already exist
	labels := map[string]string{}
	if !config.Get().IgnoreClusterRegistrationLabels {
		labels = sanitizeClusterLabels(request)
	}
	labels[fleet.ClusterAnnotation] = clusterName

	cluster, err := h.clusters.Create(&fleet.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: request.Namespace,
			Labels:    labels,
		},
		Spec: fleet.ClusterSpec{
			ClientID: request.Spec.ClientID,
		},
	})
	if apierrors.IsAlreadyExists(err) {
		return h.clusters.Get(request.Namespace, clusterName, metav1.GetOptions{})
	}
	if err == nil {
		log.Log.Info(fmt.Sprintf("Created cluster %s/%s", request.Namespace, clusterName))
	}
	return cluster, err
}

// sanitizeClusterLabels copies the agent-supplied ClusterLabels while dropping
// any key a downstream agent must not be able to assert. Keys under the
// reserved management.cattle.io/ and fleet.cattle.io/ prefixes are Rancher- and
// Fleet-managed and are trusted for cluster targeting.
func sanitizeClusterLabels(request *fleet.ClusterRegistration) map[string]string {
	labels := make(map[string]string, len(request.Spec.ClusterLabels))
	for k, v := range request.Spec.ClusterLabels {
		if isReservedLabel(k) {
			log.Log.Info(fmt.Sprintf("ClusterRegistration %s/%s attempted to set reserved label %q; ignoring that label",
				request.Namespace, request.Name, k))
			continue
		}
		labels[k] = v
	}
	return labels
}

// isReservedLabel reports whether key falls under a reserved, controller-managed
// label prefix that an agent may not assert. These namespaces are managed by
// Rancher and Fleet respectively and are trusted for cluster targeting (e.g.
// management.cattle.io/cluster-display-name is consumed by clusterName
// selectors); letting an agent set them would allow a registrant to spoof
// another cluster's identity and attract its BundleDeployments. The debugging
// label fleet.cattle.io/created-by-agent-pod is explicitly allowed.
func isReservedLabel(key string) bool {
	if key == fleet.CreatedByAgentPodLabel {
		return false
	}
	return strings.HasPrefix(key, fleet.ManagementLabelPrefix) || strings.HasPrefix(key, fleet.FleetLabelPrefix)
}

func (h *handler) authorizeCluster(sa *v1.ServiceAccount, cluster *fleet.Cluster, req *fleet.ClusterRegistration) (*v1.Secret, error) {
	var secret *v1.Secret
	var err error
	if len(sa.Secrets) != 0 {
		secret, err = h.secretsCache.Get(sa.Namespace, sa.Secrets[0].Name)
		if apierrors.IsNotFound(err) {
			// secrets can be slow to propagate to the cache
			secret, err = h.secrets.Get(sa.Namespace, sa.Secrets[0].Name, metav1.GetOptions{})
		}
	} else {
		secret, err = secretutil.GetServiceAccountTokenSecret(sa, h.secrets)
	}
	if err != nil || secret == nil {
		return nil, err
	}
	return &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      registration.SecretName(req.Spec.ClientID, req.Spec.ClientRandom),
			Namespace: h.systemRegistrationNamespace,
			Labels: map[string]string{
				fleet.ClusterAnnotation: cluster.Name,
				fleet.ManagedLabel:      "true",
			},
		},
		Type: AgentCredentialSecretType,
		Data: map[string][]byte{
			"token":               secret.Data["token"],
			"deploymentNamespace": []byte(cluster.Status.Namespace),
			"clusterNamespace":    []byte(cluster.Namespace),
			"clusterName":         []byte(cluster.Name),
			"systemNamespace":     []byte(h.systemNamespace),
		},
	}, nil
}

func requestSA(saName string, cluster *fleet.Cluster, request *fleet.ClusterRegistration) *v1.ServiceAccount {
	return &v1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: cluster.Status.Namespace,
			Labels: map[string]string{
				fleet.ManagedLabel: "true",
			},
			Annotations: map[string]string{
				fleet.ClusterAnnotation:                      cluster.Name,
				fleet.ClusterRegistrationAnnotation:          request.Name,
				fleet.ClusterRegistrationNamespaceAnnotation: request.Namespace,
			},
		},
	}
}
