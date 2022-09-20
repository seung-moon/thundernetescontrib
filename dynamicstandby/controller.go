package main

import (
	"context"

	mpsv1alpha1 "github.com/playfab/thundernetes/pkg/operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
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
