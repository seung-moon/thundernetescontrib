package main

import (
	"context"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mpsv1alpha1 "github.com/playfab/thundernetes/pkg/operator/api/v1alpha1"
)

var (
	ownerKey = ".metadata.controller"
	apiGVStr = mpsv1alpha1.GroupVersion.String()
)

type DynamicStandbyReconciler struct {
	client.Client
	Scheme   *k8sruntime.Scheme
	Recorder record.EventRecorder
}

func (r *DynamicStandbyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var gsb mpsv1alpha1.GameServerBuild
	if err := r.Get(ctx, req.NamespacedName, &gsb); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Unable to fetch GameServerBuild - skipping")
			return ctrl.Result{}, nil
		}
		log.Error(err, "unable to fetch GameServerBuild")
		return ctrl.Result{}, nil
	}

	// TODO: add logic for dynamic standby

	// check if correspoding configmap exists
	var cfm corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: gsb.Namespace, Name: gsb.Name}, &cfm); err != nil {
		if apierrors.IsNotFound(err) {
			c, err := r.createConfigMap(ctx, &gsb)
			if apierrors.IsConflict(err) {
				log.Info("ConfigMap exists already")
			} else if err != nil {
				log.Error(err, "unable to create configmap")
				return ctrl.Result{}, err
			}
			cfm = *c
		} else {
			log.Error(err, "unable to get configmap")
			return ctrl.Result{}, err
		}
	}

	// check if a new target standby is needed
	isNewTargetStandby, newTargetStandby := checkForNewTargetStandby(&gsb, &cfm)

	// update the target standby if needed
	if isNewTargetStandby {
		gsb.Spec.StandingBy = newTargetStandby
		r.Update(ctx, &gsb)
	} else {
		targetStandby := gsb.Spec.StandingBy
		targetStandbyFloor, _ := strconv.Atoi(cfm.Data["TargetStandbyFloor"])
		if targetStandby > targetStandbyFloor {
			gsb.Spec.StandingBy = ((targetStandby - targetStandbyFloor) / 2) + targetStandbyFloor
			r.Update(ctx, &gsb)
		}
	}

	// TODO: add cooldown period between consecutive updates

	return ctrl.Result{}, nil
}

func (r *DynamicStandbyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &mpsv1alpha1.GameServer{}, ownerKey, func(rawObj client.Object) []string {
		// grab the GameServer object, extract the owner...
		gs := rawObj.(*mpsv1alpha1.GameServer)
		owner := metav1.GetControllerOf(gs)
		if owner == nil {
			return nil
		}
		// ...make sure it's a GameServerBuild...
		if owner.APIVersion != apiGVStr || owner.Kind != "GameServerBuild" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&mpsv1alpha1.GameServerBuild{}).
		Complete(r)
}

func (r *DynamicStandbyReconciler) createConfigMap(ctx context.Context, gsb *mpsv1alpha1.GameServerBuild) (*corev1.ConfigMap, error) {
	cfm := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      gsb.Name,
			Namespace: gsb.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(gsb, schema.GroupVersionKind{
					Group:   mpsv1alpha1.GroupVersion.Group,
					Version: mpsv1alpha1.GroupVersion.Version,
					Kind:    "GameServerBuild",
				}),
			},
		},
		Data: map[string]string{
			"BuildID":            gsb.Spec.BuildID,
			"TargetStandbyFloor": strconv.Itoa(gsb.Spec.StandingBy),
		},
	}

	if err := r.Create(ctx, &cfm); err != nil {
		return nil, err
	}

	return &cfm, nil
}

func checkForNewTargetStandby(gsb *mpsv1alpha1.GameServerBuild, cfm *corev1.ConfigMap) (bool, int) {
	activeServers := gsb.Status.CurrentActive
	activeStandby := gsb.Status.CurrentStandingBy
	targetStandby := gsb.Spec.StandingBy
	targetStandbyFloor, _ := strconv.Atoi(cfm.Data["TargetStandbyFloor"])

	dynamicStandbyActive := false

	if activeServers > targetStandby && (float64(activeStandby/targetStandbyFloor) < 0.5) {
		targetStandby = int(1.5 * float64(targetStandby))
		dynamicStandbyActive = true
	}
	if activeServers > targetStandby && (float64(activeStandby/targetStandbyFloor) < 0.25) {
		targetStandby = 3 * targetStandby
		dynamicStandbyActive = true
	}
	if activeServers > targetStandby && (float64(activeStandby/targetStandbyFloor) < 0.005) {
		targetStandby = 4 * targetStandby
		dynamicStandbyActive = true
	}

	if dynamicStandbyActive {
		return true, targetStandby
	}

	return false, 0
}
