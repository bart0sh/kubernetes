/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dynamicresources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	resourcev1alpha2 "k8s.io/api/resource/v1alpha2"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	resourcev1alpha2apply "k8s.io/client-go/applyconfigurations/resource/v1alpha2"
	"k8s.io/client-go/kubernetes"
	resourcev1alpha2listers "k8s.io/client-go/listers/resource/v1alpha2"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
	"k8s.io/utils/ptr"
)

const (
	// Name is the name of the plugin used in Registry and configurations.
	Name = names.DynamicResources

	stateKey framework.StateKey = Name
)

// The state is initialized in PreFilter phase (for scheduling) or
// in StartSimulation (for cluster autoscaling).
//
// Because we save the pointer in framework.CycleState, in the later phases we
// don't need to call Write method to update the value
type stateData struct {
	// listers always gets set when initializing a stateData instance.
	listers

	// preScored is true if PreScore was invoked.
	preScored bool

	// A copy of all claims for the Pod (i.e. 1:1 match with
	// pod.Spec.ResourceClaims), initially with the status from the start
	// of the scheduling cycle. Each claim instance is read-only because it
	// might come from the informer cache. The instances get replaced when
	// the plugin itself successfully does an Update.
	//
	// Empty if the Pod has no claims.
	claims []*resourcev1alpha2.ResourceClaim

	// The indices of all claims that:
	// - are allocated
	// - use delayed allocation or a builtin controller
	// - were not available on at least one node
	//
	// Set in parallel during Filter, so write access there must be
	// protected by the mutex. Used by PostFilter.
	unavailableClaims sets.Set[int]

	// podSchedulingState keeps track of the PodSchedulingContext
	// (if one exists) and the changes made to it.
	podSchedulingState podSchedulingState

	// resourceModel contains the information about available and allocated resources when using
	// structured parameters and the pod needs this information.
	resources resources

	// mutex must be locked while accessing any of the fields below.
	mutex *sync.Mutex

	informationsForClaim []informationForClaim
}

func (d *stateData) Clone() framework.StateData {
	return d
}

type indexers struct {
	claimNameLookup          *resourceclaim.Lookup
	claimIndexer             cache.Indexer
	classIndexer             cache.Indexer
	claimParametersIndexer   cache.Indexer
	classParametersIndexer   cache.Indexer
	nodeResourceSliceIndexer cache.Indexer
}

func (i indexers) listers() listers {
	return listers{
		claimNameLookup:         i.claimNameLookup,
		claimLister:             resourcev1alpha2listers.NewResourceClaimLister(i.claimIndexer),
		classLister:             resourcev1alpha2listers.NewResourceClassLister(i.classIndexer),
		claimParametersLister:   resourcev1alpha2listers.NewResourceClaimParametersLister(i.claimParametersIndexer),
		classParametersLister:   resourcev1alpha2listers.NewResourceClassParametersLister(i.classParametersIndexer),
		nodeResourceSliceLister: resourcev1alpha2listers.NewNodeResourceSliceLister(i.nodeResourceSliceIndexer),
	}
}

type listers struct {
	claimNameLookup         *resourceclaim.Lookup
	claimLister             resourcev1alpha2listers.ResourceClaimLister
	classLister             resourcev1alpha2listers.ResourceClassLister
	claimParametersLister   resourcev1alpha2listers.ResourceClaimParametersLister
	classParametersLister   resourcev1alpha2listers.ResourceClassParametersLister
	nodeResourceSliceLister resourcev1alpha2listers.NodeResourceSliceLister
}

type informationForClaim struct {
	// The availableOnNode node filter of the claim converted from the
	// v1 API to nodeaffinity.NodeSelector by PreFilter for repeated
	// evaluation in Filter. Nil for claim which don't have it.
	availableOnNode *nodeaffinity.NodeSelector

	// The status of the claim gotten from the
	// schedulingCtx by PreFilter for repeated
	// evaluation in Filter. Nil for claim which don't have it.
	status *resourcev1alpha2.ResourceClaimSchedulingStatus

	// structuredParameters is true if the claim is handled via a builtin
	// controller.
	structuredParameters bool
	controller           *claimController

	// Set by Reserved, published by PreBind.
	allocation           *resourcev1alpha2.AllocationResult
	allocationDriverName string
}

type podSchedulingState struct {
	// A pointer to the PodSchedulingContext object for the pod, if one exists
	// in the API server.
	//
	// Conceptually, this object belongs into the scheduler framework
	// where it might get shared by different plugins. But in practice,
	// it is currently only used by dynamic provisioning and thus
	// managed entirely here.
	schedulingCtx *resourcev1alpha2.PodSchedulingContext

	// selectedNode is set if (and only if) a node has been selected.
	selectedNode *string

	// potentialNodes is set if (and only if) the potential nodes field
	// needs to be updated or set.
	potentialNodes *[]string
}

func (p *podSchedulingState) isDirty() bool {
	return p.selectedNode != nil ||
		p.potentialNodes != nil
}

// init checks whether there is already a PodSchedulingContext object.
// Must not be called concurrently,
func (p *podSchedulingState) init(ctx context.Context, pod *v1.Pod, podSchedulingContextLister resourcev1alpha2listers.PodSchedulingContextLister) error {
	schedulingCtx, err := podSchedulingContextLister.PodSchedulingContexts(pod.Namespace).Get(pod.Name)
	switch {
	case apierrors.IsNotFound(err):
		return nil
	case err != nil:
		return err
	default:
		// We have an object, but it might be obsolete.
		if !metav1.IsControlledBy(schedulingCtx, pod) {
			return fmt.Errorf("PodSchedulingContext object with UID %s is not owned by Pod %s/%s", schedulingCtx.UID, pod.Namespace, pod.Name)
		}
	}
	p.schedulingCtx = schedulingCtx
	return nil
}

// publish creates or updates the PodSchedulingContext object, if necessary.
// Must not be called concurrently.
func (p *podSchedulingState) publish(ctx context.Context, pod *v1.Pod, clientset kubernetes.Interface) error {
	if !p.isDirty() {
		return nil
	}

	var err error
	logger := klog.FromContext(ctx)
	if p.schedulingCtx != nil {
		// Update it.
		schedulingCtx := p.schedulingCtx.DeepCopy()
		if p.selectedNode != nil {
			schedulingCtx.Spec.SelectedNode = *p.selectedNode
		}
		if p.potentialNodes != nil {
			schedulingCtx.Spec.PotentialNodes = *p.potentialNodes
		}
		if loggerV := logger.V(6); loggerV.Enabled() {
			// At a high enough log level, dump the entire object.
			loggerV.Info("Updating PodSchedulingContext", "podSchedulingCtx", klog.KObj(schedulingCtx), "podSchedulingCtxObject", klog.Format(schedulingCtx))
		} else {
			logger.V(5).Info("Updating PodSchedulingContext", "podSchedulingCtx", klog.KObj(schedulingCtx))
		}
		_, err = clientset.ResourceV1alpha2().PodSchedulingContexts(schedulingCtx.Namespace).Update(ctx, schedulingCtx, metav1.UpdateOptions{})
		if apierrors.IsConflict(err) {
			// We don't use SSA by default for performance reasons
			// (https://github.com/kubernetes/kubernetes/issues/113700#issuecomment-1698563918)
			// because most of the time an Update doesn't encounter
			// a conflict and is faster.
			//
			// We could return an error here and rely on
			// backoff+retry, but scheduling attempts are expensive
			// and the backoff delay would cause a (small)
			// slowdown. Therefore we fall back to SSA here if needed.
			//
			// Using SSA instead of Get+Update has the advantage that
			// there is no delay for the Get. SSA is safe because only
			// the scheduler updates these fields.
			spec := resourcev1alpha2apply.PodSchedulingContextSpec()
			spec.SelectedNode = p.selectedNode
			if p.potentialNodes != nil {
				spec.PotentialNodes = *p.potentialNodes
			} else {
				// Unchanged. Has to be set because the object that we send
				// must represent the "fully specified intent". Not sending
				// the list would clear it.
				spec.PotentialNodes = p.schedulingCtx.Spec.PotentialNodes
			}
			schedulingCtxApply := resourcev1alpha2apply.PodSchedulingContext(pod.Name, pod.Namespace).WithSpec(spec)

			if loggerV := logger.V(6); loggerV.Enabled() {
				// At a high enough log level, dump the entire object.
				loggerV.Info("Patching PodSchedulingContext", "podSchedulingCtx", klog.KObj(pod), "podSchedulingCtxApply", klog.Format(schedulingCtxApply))
			} else {
				logger.V(5).Info("Patching PodSchedulingContext", "podSchedulingCtx", klog.KObj(pod))
			}
			_, err = clientset.ResourceV1alpha2().PodSchedulingContexts(pod.Namespace).Apply(ctx, schedulingCtxApply, metav1.ApplyOptions{FieldManager: "kube-scheduler", Force: true})
		}

	} else {
		// Create it.
		schedulingCtx := &resourcev1alpha2.PodSchedulingContext{
			ObjectMeta: metav1.ObjectMeta{
				Name:            pod.Name,
				Namespace:       pod.Namespace,
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(pod, schema.GroupVersionKind{Version: "v1", Kind: "Pod"})},
			},
		}
		if p.selectedNode != nil {
			schedulingCtx.Spec.SelectedNode = *p.selectedNode
		}
		if p.potentialNodes != nil {
			schedulingCtx.Spec.PotentialNodes = *p.potentialNodes
		}
		if loggerV := logger.V(6); loggerV.Enabled() {
			// At a high enough log level, dump the entire object.
			loggerV.Info("Creating PodSchedulingContext", "podSchedulingCtx", klog.KObj(schedulingCtx), "podSchedulingCtxObject", klog.Format(schedulingCtx))
		} else {
			logger.V(5).Info("Creating PodSchedulingContext", "podSchedulingCtx", klog.KObj(schedulingCtx))
		}
		_, err = clientset.ResourceV1alpha2().PodSchedulingContexts(schedulingCtx.Namespace).Create(ctx, schedulingCtx, metav1.CreateOptions{})
	}
	if err != nil {
		return err
	}
	p.potentialNodes = nil
	p.selectedNode = nil
	return nil
}

func statusForClaim(schedulingCtx *resourcev1alpha2.PodSchedulingContext, podClaimName string) *resourcev1alpha2.ResourceClaimSchedulingStatus {
	if schedulingCtx == nil {
		return nil
	}
	for _, status := range schedulingCtx.Status.ResourceClaims {
		if status.Name == podClaimName {
			return &status
		}
	}
	return nil
}

// dynamicResources is a plugin that ensures that ResourceClaims are allocated.
type dynamicResources struct {
	enabled                    bool
	fh                         framework.Handle
	clientset                  kubernetes.Interface
	listers                    listers
	indexers                   indexers
	podSchedulingContextLister resourcev1alpha2listers.PodSchedulingContextLister

	// claimAssumeCache enables temporarily storing a newer claim object
	// while the scheduler has allocated it and the corresponding object
	// update from the apiserver has not been processed by the claim
	// informer callbacks. Claims get added here in Reserve and removed by
	// the informer callback (based on the "newer than" comparison in the
	// assume cache) or when the API call in PreBind fails.
	//
	// It uses cache.MetaNamespaceKeyFunc to generate object names, which
	// therefore are "<namespace>/<name>".
	//
	// This is necessary to ensure that reconstructing the resource usage
	// at the start of a pod scheduling cycle doesn't reuse the resources
	// assigned to such a claim. Alternatively, claim allocation state
	// could also get tracked across pod scheduling cycles, but that
	// - adds complexity (need to carefully sync state with informer events
	//   for claims and NodeResourceSlices)
	// - would make integration with cluster autoscaler harder because it would need
	//   to trigger informer callbacks.
	//
	// When implementing cluster autoscaler support, this assume cache or
	// something like it (see https://github.com/kubernetes/kubernetes/pull/112202)
	// might have to be managed by the cluster autoscaler.
	claimAssumeCache volumebinding.AssumeCache

	// inFlightAllocations is map from claim UUIDs to true for those claims
	// for which allocation was triggered during a scheduling cycle and the
	// corresponding claim status update call in PreBind has not been done
	// yet. If another pod needs the claim, the pod is treated as "not
	// schedulable yet". The cluster event for the claim status update will
	// make it schedulable.
	//
	// This mechanism avoids the following problem:
	// - Pod A triggers allocation for claim X.
	// - Pod B shares access to that claim and gets scheduled because
	//   the claim is assumed to be allocated.
	// - PreBind for pod B is called first, tries to update reservedFor and
	//   fails because the claim is not really allocated yet.
	//
	// We could avoid the ordering problem by allowing either pod A or pod B
	// to set the allocation. But that is more complicated and leads to another
	// problem:
	// - Pod A and B get scheduled as above.
	// - PreBind for pod A gets called first, then fails with a temporary API error.
	//   It removes the updated claim from the assume cache because of that.
	// - PreBind for pod B gets called next and succeeds with adding the
	//   allocation and its own reservedFor entry.
	// - The assume cache is now not reflecting that the claim is allocated,
	//   which could lead to reusing the same resource for some other claim.
	//
	// A sync.Map is used because in practice sharing of a claim between
	// pods is expected to be rare compared to per-pod claim, so we end up
	// hitting the "multiple goroutines read, write, and overwrite entries
	// for disjoint sets of keys" case that sync.Map is optimized for.
	inFlightAllocations sync.Map
}

// New initializes a new plugin and returns it.
func New(ctx context.Context, plArgs runtime.Object, fh framework.Handle, fts feature.Features) (framework.Plugin, error) {
	if !fts.EnableDynamicResourceAllocation {
		// Disabled, won't do anything.
		return &dynamicResources{}, nil
	}

	logger := klog.FromContext(ctx)
	indexers := indexers{
		claimNameLookup:          resourceclaim.NewNameLookup(fh.ClientSet()),
		claimIndexer:             fh.SharedInformerFactory().Resource().V1alpha2().ResourceClaims().Informer().GetIndexer(),
		classIndexer:             fh.SharedInformerFactory().Resource().V1alpha2().ResourceClasses().Informer().GetIndexer(),
		claimParametersIndexer:   fh.SharedInformerFactory().Resource().V1alpha2().ResourceClaimParameters().Informer().GetIndexer(),
		classParametersIndexer:   fh.SharedInformerFactory().Resource().V1alpha2().ResourceClassParameters().Informer().GetIndexer(),
		nodeResourceSliceIndexer: fh.SharedInformerFactory().Resource().V1alpha2().NodeResourceSlices().Informer().GetIndexer(),
	}
	listers := indexers.listers()

	pl := &dynamicResources{
		enabled:                    true,
		fh:                         fh,
		clientset:                  fh.ClientSet(),
		indexers:                   indexers,
		listers:                    listers,
		podSchedulingContextLister: fh.SharedInformerFactory().Resource().V1alpha2().PodSchedulingContexts().Lister(),
		claimAssumeCache:           volumebinding.NewAssumeCache(logger, fh.SharedInformerFactory().Resource().V1alpha2().ResourceClaims().Informer(), "claim assume cache", "", nil),
	}

	return pl, nil
}

var _ framework.PreEnqueuePlugin = &dynamicResources{}
var _ framework.PreFilterPlugin = &dynamicResources{}
var _ framework.FilterPlugin = &dynamicResources{}
var _ framework.PostFilterPlugin = &dynamicResources{}
var _ framework.PreScorePlugin = &dynamicResources{}
var _ framework.ReservePlugin = &dynamicResources{}
var _ framework.EnqueueExtensions = &dynamicResources{}
var _ framework.PreBindPlugin = &dynamicResources{}
var _ framework.PostBindPlugin = &dynamicResources{}

// Name returns name of the plugin. It is used in logs, etc.
func (pl *dynamicResources) Name() string {
	return Name
}

// EventsToRegister returns the possible events that may make a Pod
// failed by this plugin schedulable.
func (pl *dynamicResources) EventsToRegister() []framework.ClusterEventWithHint {
	if !pl.enabled {
		return nil
	}

	events := []framework.ClusterEventWithHint{
		// TODO: cluster events for claim and class parameters creation

		// Allocation is tracked in ResourceClaims, so any changes may make the pods schedulable.
		{Event: framework.ClusterEvent{Resource: framework.ResourceClaim, ActionType: framework.Add | framework.Update}, QueueingHintFn: pl.listers.isSchedulableAfterClaimChange},
		// When a driver has provided additional information, a pod waiting for that information
		// may be schedulable.
		{Event: framework.ClusterEvent{Resource: framework.PodSchedulingContext, ActionType: framework.Add | framework.Update}, QueueingHintFn: pl.listers.isSchedulableAfterPodSchedulingContextChange},
		// A resource might depend on node labels for topology filtering.
		// A new or updated node may make pods schedulable.
		{Event: framework.ClusterEvent{Resource: framework.Node, ActionType: framework.Add | framework.UpdateNodeLabel}},
		// A pod might be waiting for a class to get created or modified.
		{Event: framework.ClusterEvent{Resource: framework.ResourceClass, ActionType: framework.Add | framework.Update}},
	}
	return events
}

// PreEnqueue checks if there are known reasons why a pod currently cannot be
// scheduled. When this fails, one of the registered events can trigger another
// attempt.
func (pl *dynamicResources) PreEnqueue(ctx context.Context, pod *v1.Pod) (status *framework.Status) {
	if err := pl.listers.foreachPodResourceClaim(pod, nil); err != nil {
		return statusUnschedulable(klog.FromContext(ctx), err.Error())
	}
	return nil
}

// isSchedulableAfterClaimChange is invoked for add and update claim events reported by
// an informer. It checks whether that change made a previously unschedulable
// pod schedulable. It errs on the side of letting a pod scheduling attempt
// happen. The delete claim event will not invoke it, so newObj will never be nil.
func (l *listers) isSchedulableAfterClaimChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (framework.QueueingHint, error) {
	originalClaim, modifiedClaim, err := schedutil.As[*resourcev1alpha2.ResourceClaim](oldObj, newObj)
	if err != nil {
		// Shouldn't happen.
		return framework.Queue, fmt.Errorf("unexpected object in isSchedulableAfterClaimChange: %w", err)
	}

	usesClaim := false
	if err := l.foreachPodResourceClaim(pod, func(_ string, claim *resourcev1alpha2.ResourceClaim) {
		if claim.UID == modifiedClaim.UID {
			usesClaim = true
		}
	}); err != nil {
		// This is not an unexpected error: we know that
		// foreachPodResourceClaim only returns errors for "not
		// schedulable".
		logger.V(4).Info("pod is not schedulable", "pod", klog.KObj(pod), "claim", klog.KObj(modifiedClaim), "reason", err.Error())
		return framework.QueueSkip, nil
	}

	if !usesClaim {
		// This was not the claim the pod was waiting for.
		logger.V(6).Info("unrelated claim got modified", "pod", klog.KObj(pod), "claim", klog.KObj(modifiedClaim))
		return framework.QueueSkip, nil
	}

	if originalClaim == nil {
		logger.V(4).Info("claim for pod got created", "pod", klog.KObj(pod), "claim", klog.KObj(modifiedClaim))
		return framework.Queue, nil
	}

	// Modifications may or may not be relevant. If the entire
	// status is as before, then something else must have changed
	// and we don't care. What happens in practice is that the
	// resource driver adds the finalizer.
	if apiequality.Semantic.DeepEqual(&originalClaim.Status, &modifiedClaim.Status) {
		if loggerV := logger.V(7); loggerV.Enabled() {
			// Log more information.
			loggerV.Info("claim for pod got modified where the pod doesn't care", "pod", klog.KObj(pod), "claim", klog.KObj(modifiedClaim), "diff", cmp.Diff(originalClaim, modifiedClaim))
		} else {
			logger.V(6).Info("claim for pod got modified where the pod doesn't care", "pod", klog.KObj(pod), "claim", klog.KObj(modifiedClaim))
		}
		return framework.QueueSkip, nil
	}

	logger.V(4).Info("status of claim for pod got updated", "pod", klog.KObj(pod), "claim", klog.KObj(modifiedClaim))
	return framework.Queue, nil
}

// isSchedulableAfterPodSchedulingContextChange is invoked for all
// PodSchedulingContext events reported by an informer. It checks whether that
// change made a previously unschedulable pod schedulable (updated) or a new
// attempt is needed to re-create the object (deleted). It errs on the side of
// letting a pod scheduling attempt happen.
func (l *listers) isSchedulableAfterPodSchedulingContextChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (framework.QueueingHint, error) {
	// Deleted? That can happen because we ourselves delete the PodSchedulingContext while
	// working on the pod. This can be ignored.
	if oldObj != nil && newObj == nil {
		logger.V(4).Info("PodSchedulingContext got deleted")
		return framework.QueueSkip, nil
	}

	oldPodScheduling, newPodScheduling, err := schedutil.As[*resourcev1alpha2.PodSchedulingContext](oldObj, newObj)
	if err != nil {
		// Shouldn't happen.
		return framework.Queue, fmt.Errorf("unexpected object in isSchedulableAfterPodSchedulingContextChange: %w", err)
	}
	podScheduling := newPodScheduling // Never nil because deletes are handled above.

	if podScheduling.Name != pod.Name || podScheduling.Namespace != pod.Namespace {
		logger.V(7).Info("PodSchedulingContext for unrelated pod got modified", "pod", klog.KObj(pod), "podScheduling", klog.KObj(podScheduling))
		return framework.QueueSkip, nil
	}

	// If the drivers have provided information about all
	// unallocated claims with delayed allocation, then the next
	// scheduling attempt is able to pick a node, so we let it run
	// immediately if this occurred for the first time, otherwise
	// we allow backoff.
	pendingDelayedClaims := 0
	if err := l.foreachPodResourceClaim(pod, func(podResourceName string, claim *resourcev1alpha2.ResourceClaim) {
		if claim.Spec.AllocationMode == resourcev1alpha2.AllocationModeWaitForFirstConsumer &&
			claim.Status.Allocation == nil &&
			!podSchedulingHasClaimInfo(podScheduling, podResourceName) {
			pendingDelayedClaims++
		}
	}); err != nil {
		// This is not an unexpected error: we know that
		// foreachPodResourceClaim only returns errors for "not
		// schedulable".
		logger.V(4).Info("pod is not schedulable, keep waiting", "pod", klog.KObj(pod), "reason", err.Error())
		return framework.QueueSkip, nil
	}

	// Some driver responses missing?
	if pendingDelayedClaims > 0 {
		// We could start a pod scheduling attempt to refresh the
		// potential nodes list.  But pod scheduling attempts are
		// expensive and doing them too often causes the pod to enter
		// backoff. Let's wait instead for all drivers to reply.
		if loggerV := logger.V(6); loggerV.Enabled() {
			loggerV.Info("PodSchedulingContext with missing resource claim information, keep waiting", "pod", klog.KObj(pod), "podSchedulingDiff", cmp.Diff(oldPodScheduling, podScheduling))
		} else {
			logger.V(5).Info("PodSchedulingContext with missing resource claim information, keep waiting", "pod", klog.KObj(pod))
		}
		return framework.QueueSkip, nil
	}

	if oldPodScheduling == nil /* create */ ||
		len(oldPodScheduling.Status.ResourceClaims) < len(podScheduling.Status.ResourceClaims) /* new information and not incomplete (checked above) */ {
		// This definitely is new information for the scheduler. Try again immediately.
		logger.V(4).Info("PodSchedulingContext for pod has all required information, schedule immediately", "pod", klog.KObj(pod))
		return framework.Queue, nil
	}

	// The other situation where the scheduler needs to do
	// something immediately is when the selected node doesn't
	// work: waiting in the backoff queue only helps eventually
	// resources on the selected node become available again. It's
	// much more likely, in particular when trying to fill up the
	// cluster, that the choice simply didn't work out. The risk
	// here is that in a situation where the cluster really is
	// full, backoff won't be used because the scheduler keeps
	// trying different nodes. This should not happen when it has
	// full knowledge about resource availability (=
	// PodSchedulingContext.*.UnsuitableNodes is complete) but may happen
	// when it doesn't (= PodSchedulingContext.*.UnsuitableNodes had to be
	// truncated).
	//
	// Truncation only happens for very large clusters and then may slow
	// down scheduling, but should not break it completely. This is
	// acceptable while DRA is alpha and will be investigated further
	// before moving DRA to beta.
	if podScheduling.Spec.SelectedNode != "" {
		for _, claimStatus := range podScheduling.Status.ResourceClaims {
			if sliceContains(claimStatus.UnsuitableNodes, podScheduling.Spec.SelectedNode) {
				logger.V(5).Info("PodSchedulingContext has unsuitable selected node, schedule immediately", "pod", klog.KObj(pod), "selectedNode", podScheduling.Spec.SelectedNode, "podResourceName", claimStatus.Name)
				return framework.Queue, nil
			}
		}
	}

	// Update with only the spec modified?
	if oldPodScheduling != nil &&
		!apiequality.Semantic.DeepEqual(&oldPodScheduling.Spec, &podScheduling.Spec) &&
		apiequality.Semantic.DeepEqual(&oldPodScheduling.Status, &podScheduling.Status) {
		logger.V(5).Info("PodSchedulingContext has only the scheduler spec changes, ignore the update", "pod", klog.KObj(pod))
		return framework.QueueSkip, nil
	}

	// Once we get here, all changes which are known to require special responses
	// have been checked for. Whatever the change was, we don't know exactly how
	// to handle it and thus return Queue. This will cause the
	// scheduler to treat the event as if no event hint callback had been provided.
	// Developers who want to investigate this can enable a diff at log level 6.
	if loggerV := logger.V(6); loggerV.Enabled() {
		loggerV.Info("PodSchedulingContext for pod with unknown changes, maybe schedule", "pod", klog.KObj(pod), "podSchedulingDiff", cmp.Diff(oldPodScheduling, podScheduling))
	} else {
		logger.V(5).Info("PodSchedulingContext for pod with unknown changes, maybe schedule", "pod", klog.KObj(pod))
	}
	return framework.Queue, nil

}

func podSchedulingHasClaimInfo(podScheduling *resourcev1alpha2.PodSchedulingContext, podResourceName string) bool {
	for _, claimStatus := range podScheduling.Status.ResourceClaims {
		if claimStatus.Name == podResourceName {
			return true
		}
	}
	return false
}

func sliceContains(hay []string, needle string) bool {
	for _, item := range hay {
		if item == needle {
			return true
		}
	}
	return false
}

// podResourceClaims returns the ResourceClaims for all pod.Spec.PodResourceClaims.
func (l *listers) podResourceClaims(pod *v1.Pod) ([]*resourcev1alpha2.ResourceClaim, error) {
	claims := make([]*resourcev1alpha2.ResourceClaim, 0, len(pod.Spec.ResourceClaims))
	if err := l.foreachPodResourceClaim(pod, func(_ string, claim *resourcev1alpha2.ResourceClaim) {
		// We store the pointer as returned by the lister. The
		// assumption is that if a claim gets modified while our code
		// runs, the cache will store a new pointer, not mutate the
		// existing object that we point to here.
		claims = append(claims, claim)
	}); err != nil {
		return nil, err
	}
	return claims, nil
}

// foreachPodResourceClaim checks that each ResourceClaim for the pod exists.
// It calls an optional handler for those claims that it finds.
func (l *listers) foreachPodResourceClaim(pod *v1.Pod, cb func(podResourceName string, claim *resourcev1alpha2.ResourceClaim)) error {
	for _, resource := range pod.Spec.ResourceClaims {
		claimName, mustCheckOwner, err := l.claimNameLookup.Name(pod, &resource)
		if err != nil {
			return err
		}
		// The claim name might be nil if no underlying resource claim
		// was generated for the referenced claim. There are valid use
		// cases when this might happen, so we simply skip it.
		if claimName == nil {
			continue
		}
		claim, err := l.claimLister.ResourceClaims(pod.Namespace).Get(*claimName)
		if err != nil {
			return err
		}

		if claim.DeletionTimestamp != nil {
			return fmt.Errorf("resourceclaim %q is being deleted", claim.Name)
		}

		if mustCheckOwner {
			if err := resourceclaim.IsForPod(pod, claim); err != nil {
				return err
			}
		}
		if cb != nil {
			cb(resource.Name, claim)
		}
	}
	return nil
}

// PreFilter invoked at the prefilter extension point to check if pod has all
// immediate claims bound. UnschedulableAndUnresolvable is returned if
// the pod cannot be scheduled at the moment on any node.
func (pl *dynamicResources) PreFilter(ctx context.Context, cs *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	return nil, pl.pre(ctx, cs, pod)
}

// pre is called by both PreFilter and SimulateBindPod. It initializes
// stateData with information about pending claims.
func (pl *dynamicResources) pre(ctx context.Context, cs *framework.CycleState, pod *v1.Pod) *framework.Status {
	if !pl.enabled {
		return framework.NewStatus(framework.Skip)
	}
	logger := klog.FromContext(ctx)

	// If the pod does not reference any claim, we don't need to do
	// anything for it. We just initialize an empty state to record that
	// observation for the other functions. This gets updated below
	// if we get that far.
	//
	// StartSimulation might have been called before, in which case
	// our stateData already exists.
	sf, err := cs.Read(stateKey)
	if err != nil {
		sf = &stateData{
			listers: pl.listers,
			mutex:   new(sync.Mutex),
		}
		cs.Write(stateKey, sf)
	}
	state := sf.(*stateData)

	claims, err := state.podResourceClaims(pod)
	if err != nil {
		return statusUnschedulable(logger, err.Error())
	}
	logger.V(5).Info("pod resource claims", "pod", klog.KObj(pod), "resourceclaims", klog.KObjSlice(claims))

	// If the pod does not reference any claim,
	// DynamicResources Filter has nothing to do with the Pod.
	if len(claims) == 0 {
		return framework.NewStatus(framework.Skip)
	}

	// Fetch PodSchedulingContext, it's going to be needed when checking claims.
	if err := state.podSchedulingState.init(ctx, pod, pl.podSchedulingContextLister); err != nil {
		return statusError(logger, err)
	}

	state.informationsForClaim = make([]informationForClaim, len(claims))
	needResourceInformation := false
	for index, claim := range claims {
		if claim.Spec.AllocationMode == resourcev1alpha2.AllocationModeImmediate &&
			claim.Status.Allocation == nil {
			// TODO: structured parameters
			// This will get resolved by the resource driver.
			return statusUnschedulable(logger, "unallocated immediate resourceclaim", "pod", klog.KObj(pod), "resourceclaim", klog.KObj(claim))
		}
		if claim.Status.DeallocationRequested {
			// This will get resolved by the resource driver.
			return statusUnschedulable(logger, "resourceclaim must be reallocated", "pod", klog.KObj(pod), "resourceclaim", klog.KObj(claim))
		}
		if claim.Status.Allocation != nil &&
			!resourceclaim.CanBeReserved(claim) &&
			!resourceclaim.IsReservedForPod(pod, claim) {
			// Resource is in use. The pod has to wait.
			return statusUnschedulable(logger, "resourceclaim in use", "pod", klog.KObj(pod), "resourceclaim", klog.KObj(claim))
		}

		var class *resourcev1alpha2.ResourceClass
		if claim.Status.Allocation != nil {
			if claim.Status.Allocation.AvailableOnNodes != nil {
				nodeSelector, err := nodeaffinity.NewNodeSelector(claim.Status.Allocation.AvailableOnNodes)
				if err != nil {
					return statusError(logger, err)
				}
				state.informationsForClaim[index].availableOnNode = nodeSelector
			}

			state.informationsForClaim[index].structuredParameters = slices.Contains(claim.Finalizers, resourcev1alpha2.Finalizer)
		} else {
			// The ResourceClass might have a node filter. This is
			// useful for trimming the initial set of potential
			// nodes before we ask the driver(s) for information
			// about the specific pod.
			var err error
			class, err = state.classLister.Get(claim.Spec.ResourceClassName)
			if err != nil {
				// If the class cannot be retrieved, allocation cannot proceed.
				if apierrors.IsNotFound(err) {
					// Here we mark the pod as "unschedulable", so it'll sleep in
					// the unscheduleable queue until a ResourceClass event occurs.
					return statusUnschedulable(logger, fmt.Sprintf("resource class %s does not exist", claim.Spec.ResourceClassName))
				}
				// Other error, retry with backoff.
				return statusError(logger, fmt.Errorf("look up resource class: %v", err))
			}
			if class.SuitableNodes != nil {
				selector, err := nodeaffinity.NewNodeSelector(class.SuitableNodes)
				if err != nil {
					return statusError(logger, err)
				}
				state.informationsForClaim[index].availableOnNode = selector
			}
			state.informationsForClaim[index].status = statusForClaim(state.podSchedulingState.schedulingCtx, pod.Spec.ResourceClaims[index].Name)

			if class.StructuredParameters {
				state.informationsForClaim[index].structuredParameters = true

				// Allocation in flight? Better wait for that
				// to finish, see inFlightAllocations
				// documentation for details.
				if _, found := pl.inFlightAllocations.Load(claim.UID); found {
					return statusUnschedulable(logger, fmt.Sprintf("resource claim %s is in the process of being allocated", klog.KObj(claim)))
				}

				// We need the claim and class parameters. If
				// they don't exist yet, the pod has to wait.
				//
				// TODO: check this already in foreachPodResourceClaim, together with setting up informationsForClaim.
				// Then PreEnqueue will also check for existence of parameters.
				classParameters, claimParameters, status := pl.lookupParameters(logger, class, claim)
				if status != nil {
					return status
				}
				controller, err := newClaimController(logger, class, classParameters, claimParameters)
				if err != nil {
					return statusError(logger, err)
				}
				state.informationsForClaim[index].controller = controller
				needResourceInformation = true
			}
		}
	}

	if needResourceInformation {
		// Doing this over and over again for each pod could be avoided
		// by parsing once when creating the plugin and then updating
		// that state in informer callbacks. But that would cause
		// problems for using the plugin in the Cluster Autoscaler. If
		// this step here turns out to be expensive, we may have to
		// maintain and update state more persistently.
		resources, err := newResourceModel(logger, pl.listers.nodeResourceSliceLister, pl.claimAssumeCache)
		if err != nil {
			return statusError(logger, err)
		}
		state.resources = resources
	}

	state.claims = claims
	return nil
}

func (pl *dynamicResources) lookupParameters(logger klog.Logger, class *resourcev1alpha2.ResourceClass, claim *resourcev1alpha2.ResourceClaim) (classParameters *resourcev1alpha2.ResourceClassParameters, claimParameters *resourcev1alpha2.ResourceClaimParameters, status *framework.Status) {
	classParameters, status = pl.lookupClassParameters(logger, class)
	if status != nil {
		return
	}
	claimParameters, status = pl.lookupClaimParameters(logger, claim)
	return
}

func (pl *dynamicResources) lookupClassParameters(logger klog.Logger, class *resourcev1alpha2.ResourceClass) (*resourcev1alpha2.ResourceClassParameters, *framework.Status) {
	if class.ParametersRef == nil {
		return nil, nil
	}

	if class.ParametersRef.APIGroup == resourcev1alpha2.SchemeGroupVersion.Group &&
		class.ParametersRef.Kind == "ResourceClassParameters" {
		// Use the parameters which were referenced directly.
		parameters, err := pl.listers.classParametersLister.ResourceClassParameters(class.ParametersRef.Namespace).Get(class.ParametersRef.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil, statusUnschedulable(logger, fmt.Sprintf("class parameters %s not found", klog.KRef(class.ParametersRef.Namespace, class.ParametersRef.Name)))
			}
			return nil, statusError(logger, fmt.Errorf("get class parameters %s: %v", klog.KRef(class.Namespace, class.ParametersRef.Name), err))
		}
		return parameters, nil
	}

	allParameters, err := pl.listers.classParametersLister.ResourceClassParameters(class.Namespace).List(labels.Everything())
	if err != nil {
		return nil, statusError(logger, fmt.Errorf("listing class parameters failed: %v", err))
	}
	for _, parameters := range allParameters {
		if parameters.GeneratedFrom == nil {
			continue
		}
		if parameters.GeneratedFrom.APIGroup == class.ParametersRef.APIGroup &&
			parameters.GeneratedFrom.Kind == class.ParametersRef.Kind &&
			parameters.GeneratedFrom.Name == class.ParametersRef.Name &&
			parameters.GeneratedFrom.Namespace == class.ParametersRef.Namespace {
			return parameters, nil
		}
	}
	return nil, statusUnschedulable(logger, fmt.Sprintf("generated class parameters for %s.%s %s not found", class.ParametersRef.Kind, class.ParametersRef.APIGroup, klog.KRef(class.Namespace, class.ParametersRef.Name)))
}

func (pl *dynamicResources) lookupClaimParameters(logger klog.Logger, claim *resourcev1alpha2.ResourceClaim) (*resourcev1alpha2.ResourceClaimParameters, *framework.Status) {
	if claim.Spec.ParametersRef == nil {
		return nil, nil
	}
	if claim.Spec.ParametersRef.APIGroup == resourcev1alpha2.SchemeGroupVersion.Group &&
		claim.Spec.ParametersRef.Kind == "ResourceClaimParameters" {
		// Use the parameters which were referenced directly.
		parameters, err := pl.listers.claimParametersLister.ResourceClaimParameters(claim.Namespace).Get(claim.Spec.ParametersRef.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil, statusUnschedulable(logger, fmt.Sprintf("claim parameters %s not found", klog.KRef(claim.Namespace, claim.Spec.ParametersRef.Name)))
			}
			return nil, statusError(logger, fmt.Errorf("get claim parameters %s: %v", klog.KRef(claim.Namespace, claim.Spec.ParametersRef.Name), err))
		}
		return parameters, nil
	}

	allParameters, err := pl.listers.claimParametersLister.ResourceClaimParameters(claim.Namespace).List(labels.Everything())
	if err != nil {
		return nil, statusError(logger, fmt.Errorf("listing claim parameters failed: %v", err))
	}
	for _, parameters := range allParameters {
		if parameters.GeneratedFrom == nil {
			continue
		}
		if parameters.GeneratedFrom.APIGroup == claim.Spec.ParametersRef.APIGroup &&
			parameters.GeneratedFrom.Kind == claim.Spec.ParametersRef.Kind &&
			parameters.GeneratedFrom.Name == claim.Spec.ParametersRef.Name {
			return parameters, nil
		}
	}
	return nil, statusUnschedulable(logger, fmt.Sprintf("generated claim parameters for %s.%s %s not found", claim.Spec.ParametersRef.Kind, claim.Spec.ParametersRef.APIGroup, klog.KRef(claim.Namespace, claim.Spec.ParametersRef.Name)))
}

// PreFilterExtensions returns prefilter extensions, pod add and remove.
func (pl *dynamicResources) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

func getStateData(cs *framework.CycleState) (*stateData, error) {
	state, err := cs.Read(stateKey)
	if err != nil {
		return nil, err
	}
	s, ok := state.(*stateData)
	if !ok {
		return nil, errors.New("unable to convert state into stateData")
	}
	return s, nil
}

// Filter invoked at the filter extension point.
// It evaluates if a pod can fit due to the resources it requests,
// for both allocated and unallocated claims.
//
// For claims that are bound, then it checks that the node affinity is
// satisfied by the given node.
//
// For claims that are unbound, it checks whether the claim might get allocated
// for the node.
func (pl *dynamicResources) Filter(ctx context.Context, cs *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	if !pl.enabled {
		return nil
	}
	state, err := getStateData(cs)
	if err != nil {
		return statusError(klog.FromContext(ctx), err)
	}
	if len(state.claims) == 0 {
		return nil
	}

	logger := klog.FromContext(ctx)
	node := nodeInfo.Node()

	var unavailableClaims []int
	for index, claim := range state.claims {
		logger.V(10).Info("filtering based on resource claims of the pod", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaim", klog.KObj(claim))
		switch {
		case claim.Status.Allocation != nil:
			if nodeSelector := state.informationsForClaim[index].availableOnNode; nodeSelector != nil {
				if !nodeSelector.Match(node) {
					logger.V(5).Info("AvailableOnNodes does not match", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaim", klog.KObj(claim))
					unavailableClaims = append(unavailableClaims, index)
				}
			}
		case claim.Status.DeallocationRequested:
			// We shouldn't get here. PreFilter already checked this.
			return statusUnschedulable(logger, "resourceclaim must be reallocated", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaim", klog.KObj(claim))
		case claim.Spec.AllocationMode == resourcev1alpha2.AllocationModeWaitForFirstConsumer:
			if selector := state.informationsForClaim[index].availableOnNode; selector != nil {
				if matches := selector.Match(node); !matches {
					return statusUnschedulable(logger, "excluded by resource class node filter", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclassName", claim.Spec.ResourceClassName)
				}
			}
			// Can a builtin controller tell us whether the node is suitable?
			if state.informationsForClaim[index].structuredParameters {
				suitable, err := state.informationsForClaim[index].controller.nodeIsSuitable(ctx, node.Name, state.resources)
				if err != nil {
					return statusError(logger, fmt.Errorf("checking structured parameters: %w", err))
				}
				if !suitable {
					return statusUnschedulable(logger, "resourceclaim cannot be allocated for the node (unsuitable)", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaim", klog.KObj(claim))
				}
			} else {
				if status := state.informationsForClaim[index].status; status != nil {
					for _, unsuitableNode := range status.UnsuitableNodes {
						if node.Name == unsuitableNode {
							return statusUnschedulable(logger, "resourceclaim cannot be allocated for the node (unsuitable)", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaim", klog.KObj(claim), "unsuitablenodes", status.UnsuitableNodes)
						}
					}
				}
			}
		default:
			// This should have been delayed allocation. Immediate
			// allocation was already checked for in PreFilter.
			return statusError(logger, fmt.Errorf("internal error, unexpected allocation mode %v", claim.Spec.AllocationMode))
		}
	}

	if len(unavailableClaims) > 0 {
		state.mutex.Lock()
		defer state.mutex.Unlock()
		if state.unavailableClaims == nil {
			state.unavailableClaims = sets.New[int]()
		}

		for _, index := range unavailableClaims {
			claim := state.claims[index]
			// Deallocation makes more sense for claims with
			// delayed allocation. Claims with immediate allocation
			// would just get allocated again for a random node,
			// which is unlikely to help the pod.
			//
			// Claims with builtin controller are handled like
			// claims with delayed allocation.
			if claim.Spec.AllocationMode == resourcev1alpha2.AllocationModeWaitForFirstConsumer ||
				state.informationsForClaim[index].controller != nil {
				state.unavailableClaims.Insert(index)
			}
		}
		return statusUnschedulable(logger, "resourceclaim not available on the node", "pod", klog.KObj(pod))
	}

	return nil
}

// PostFilter checks whether there are allocated claims that could get
// deallocated to help get the Pod schedulable. If yes, it picks one and
// requests its deallocation.  This only gets called when filtering found no
// suitable node.
func (pl *dynamicResources) PostFilter(ctx context.Context, cs *framework.CycleState, pod *v1.Pod, filteredNodeStatusMap framework.NodeToStatusMap) (*framework.PostFilterResult, *framework.Status) {
	if !pl.enabled {
		return nil, framework.NewStatus(framework.Unschedulable, "plugin disabled")
	}
	logger := klog.FromContext(ctx)
	state, err := getStateData(cs)
	if err != nil {
		return nil, statusError(logger, err)
	}
	if len(state.claims) == 0 {
		return nil, framework.NewStatus(framework.Unschedulable, "no new claims to deallocate")
	}

	// TODO: if picking a node failed because of exhausted capacity in a builtin controller,
	// which event will make the pod schedulable again? Below we return Unschedulable,
	// so we won't retry. With an external controller, that controller will periodically
	// refresh the UnsuitableNodes list. So here we should also just try periodically?

	// Iterating over a map is random. This is intentional here, we want to
	// pick one claim randomly because there is no better heuristic.
	for index := range state.unavailableClaims {
		claim := state.claims[index]
		if len(claim.Status.ReservedFor) == 0 ||
			len(claim.Status.ReservedFor) == 1 && claim.Status.ReservedFor[0].UID == pod.UID {
			// Is the claim is handled by a builtin controller?
			// Then we can simply clear the allocation. Once the
			// claim informer catches up, the controllers will
			// be notified about this change.
			clearAllocation := state.informationsForClaim[index].controller != nil

			// Before we tell a driver to deallocate a claim, we
			// have to stop telling it to allocate. Otherwise,
			// depending on timing, it will deallocate the claim,
			// see a PodSchedulingContext with selected node, and
			// allocate again for that same node.
			if !clearAllocation &&
				state.podSchedulingState.schedulingCtx != nil &&
				state.podSchedulingState.schedulingCtx.Spec.SelectedNode != "" {
				state.podSchedulingState.selectedNode = ptr.To("")
				if err := state.podSchedulingState.publish(ctx, pod, pl.clientset); err != nil {
					return nil, statusError(logger, err)
				}
			}

			claim := claim.DeepCopy()
			claim.Status.ReservedFor = nil
			if clearAllocation {
				claim.Status.Allocation = nil
			} else {
				claim.Status.DeallocationRequested = true
			}
			logger.V(5).Info("Requesting deallocation of ResourceClaim", "pod", klog.KObj(pod), "resourceclaim", klog.KObj(claim))
			if _, err := pl.clientset.ResourceV1alpha2().ResourceClaims(claim.Namespace).UpdateStatus(ctx, claim, metav1.UpdateOptions{}); err != nil {
				return nil, statusError(logger, err)
			}
			return nil, framework.NewStatus(framework.Unschedulable, "deallocation of ResourceClaim completed")
		}
	}
	return nil, framework.NewStatus(framework.Unschedulable, "still not schedulable")
}

// PreScore is passed a list of all nodes that would fit the pod. Not all
// claims are necessarily allocated yet, so here we can set the SuitableNodes
// field for those which are pending.
func (pl *dynamicResources) PreScore(ctx context.Context, cs *framework.CycleState, pod *v1.Pod, nodes []*framework.NodeInfo) *framework.Status {
	if !pl.enabled {
		return nil
	}
	state, err := getStateData(cs)
	if err != nil {
		return statusError(klog.FromContext(ctx), err)
	}
	defer func() {
		state.preScored = true
	}()
	if len(state.claims) == 0 {
		return nil
	}

	logger := klog.FromContext(ctx)
	pending := false
	for index, claim := range state.claims {
		if claim.Status.Allocation == nil &&
			state.informationsForClaim[index].controller == nil {
			pending = true
			break
		}
	}
	if !pending {
		logger.V(5).Info("no pending claims with control plane controller", "pod", klog.KObj(pod))
		return nil
	}

	if haveAllPotentialNodes(state.podSchedulingState.schedulingCtx, nodes) {
		logger.V(5).Info("all potential nodes already set", "pod", klog.KObj(pod), "potentialnodes", klog.KObjSlice(nodes))
		return nil
	}

	// Remember the potential nodes. The object will get created or
	// updated in Reserve. This is both an optimization and
	// covers the case that PreScore doesn't get called when there
	// is only a single node.
	logger.V(5).Info("remembering potential nodes", "pod", klog.KObj(pod), "potentialnodes", klog.KObjSlice(nodes))
	numNodes := len(nodes)
	if numNodes > resourcev1alpha2.PodSchedulingNodeListMaxSize {
		numNodes = resourcev1alpha2.PodSchedulingNodeListMaxSize
	}
	potentialNodes := make([]string, 0, numNodes)
	if numNodes == len(nodes) {
		// Copy all node names.
		for _, node := range nodes {
			potentialNodes = append(potentialNodes, node.Node().Name)
		}
	} else {
		// Select a random subset of the nodes to comply with
		// the PotentialNodes length limit. Randomization is
		// done for us by Go which iterates over map entries
		// randomly.
		nodeNames := map[string]struct{}{}
		for _, node := range nodes {
			nodeNames[node.Node().Name] = struct{}{}
		}
		for nodeName := range nodeNames {
			if len(potentialNodes) >= resourcev1alpha2.PodSchedulingNodeListMaxSize {
				break
			}
			potentialNodes = append(potentialNodes, nodeName)
		}
	}
	sort.Strings(potentialNodes)
	state.podSchedulingState.potentialNodes = &potentialNodes
	return nil
}

func haveAllPotentialNodes(schedulingCtx *resourcev1alpha2.PodSchedulingContext, nodes []*framework.NodeInfo) bool {
	if schedulingCtx == nil {
		return false
	}
	for _, node := range nodes {
		if !haveNode(schedulingCtx.Spec.PotentialNodes, node.Node().Name) {
			return false
		}
	}
	return true
}

func haveNode(nodeNames []string, nodeName string) bool {
	for _, n := range nodeNames {
		if n == nodeName {
			return true
		}
	}
	return false
}

// Reserve reserves claims for the pod.
func (pl *dynamicResources) Reserve(ctx context.Context, cs *framework.CycleState, pod *v1.Pod, nodeName string) (status *framework.Status) {
	if !pl.enabled {
		return nil
	}
	state, err := getStateData(cs)
	if err != nil {
		return statusError(klog.FromContext(ctx), err)
	}
	if len(state.claims) == 0 {
		return nil
	}

	numDelayedAllocationPending := 0
	numClaimsWithStatusInfo := 0
	claimsWithBuiltinController := make([]int, 0, len(state.claims))
	logger := klog.FromContext(ctx)
	for index, claim := range state.claims {
		if claim.Status.Allocation != nil {
			// Allocated, but perhaps not reserved yet. We checked in PreFilter that
			// the pod could reserve the claim. Instead of reserving here by
			// updating the ResourceClaim status, we assume that reserving
			// will work and only do it for real during binding. If it fails at
			// that time, some other pod was faster and we have to try again.
			continue
		}

		// Do we have a builtin controller?
		if state.informationsForClaim[index].controller != nil {
			claimsWithBuiltinController = append(claimsWithBuiltinController, index)
			continue
		}

		// Must be delayed allocation with control plane controller.
		numDelayedAllocationPending++

		// Did the driver provide information that steered node
		// selection towards a node that it can support?
		if statusForClaim(state.podSchedulingState.schedulingCtx, pod.Spec.ResourceClaims[index].Name) != nil {
			numClaimsWithStatusInfo++
		}
	}

	if numDelayedAllocationPending == 0 && len(claimsWithBuiltinController) == 0 {
		// Nothing left to do.
		return nil
	}

	if !state.preScored && numDelayedAllocationPending > 0 {
		// There was only one candidate that passed the Filters and
		// therefore PreScore was not called.
		//
		// We need to ask whether that node is suitable, otherwise the
		// scheduler will pick it forever even when it cannot satisfy
		// the claim.
		if state.podSchedulingState.schedulingCtx == nil ||
			!containsNode(state.podSchedulingState.schedulingCtx.Spec.PotentialNodes, nodeName) {
			potentialNodes := []string{nodeName}
			state.podSchedulingState.potentialNodes = &potentialNodes
			logger.V(5).Info("asking for information about single potential node", "pod", klog.KObj(pod), "node", klog.ObjectRef{Name: nodeName})
		}
	}

	// Prepare allocation of claims handled by the schedulder?
	for _, index := range claimsWithBuiltinController {
		claim := state.claims[index]
		driverName, allocation, err := state.informationsForClaim[index].controller.allocate(ctx, nodeName, state.resources)
		if err != nil {
			// We checked before that the node is suitable. This shouldn't have failed,
			// so treat this as an error.
			return statusError(logger, fmt.Errorf("claim allocation failed unexpectedly: %v", err))
		}
		state.informationsForClaim[index].allocation = allocation
		state.informationsForClaim[index].allocationDriverName = driverName
		pl.inFlightAllocations.Store(claim.UID, true)
		claim = claim.DeepCopy()
		claim.Status.DriverName = driverName
		claim.Status.Allocation = allocation
		if err := pl.claimAssumeCache.Assume(claim); err != nil {
			return statusError(logger, fmt.Errorf("update claim assume cache: %v", err))
		}
		logger.V(5).Info("prepared allocation result", "claim", klog.KObj(claim), "driver", driverName, "allocation", allocation)
	}

	// When there is only one pending resource, we can go ahead with
	// requesting allocation even when we don't have the information from
	// the driver yet. Otherwise we wait for information before blindly
	// making a decision that might have to be reversed later.
	//
	// If all pending claims are handled with a builtin controller,
	// there is no need for a PodSchedulingContext change.
	if numDelayedAllocationPending == 1 && len(claimsWithBuiltinController) == 0 ||
		numClaimsWithStatusInfo+len(claimsWithBuiltinController) == numDelayedAllocationPending && len(claimsWithBuiltinController) < numDelayedAllocationPending {
		// TODO: can we increase the chance that the scheduler picks
		// the same node as before when allocation is on-going,
		// assuming that that node still fits the pod?  Picking a
		// different node may lead to some claims being allocated for
		// one node and others for another, which then would have to be
		// resolved with deallocation.
		if state.podSchedulingState.schedulingCtx == nil ||
			state.podSchedulingState.schedulingCtx.Spec.SelectedNode != nodeName {
			state.podSchedulingState.selectedNode = &nodeName
			logger.V(5).Info("start allocation", "pod", klog.KObj(pod), "node", klog.ObjectRef{Name: nodeName})
			// The actual publish happens in PreBind or Unreserve.
			return nil
		}
	}

	// May have been modified earlier in PreScore or above.
	if state.podSchedulingState.isDirty() {
		// The actual publish happens in PreBind or Unreserve.
		return nil
	}

	// If all pending claims are handled with a builtin controller, then
	// we can allow the pod to proceed. Allocating and reserving the claims
	// will be done in PreBind.
	if numDelayedAllocationPending == 0 {
		return nil
	}

	// More than one pending claim and not enough information about all of them.
	//
	// TODO: can or should we ensure that schedulingCtx gets aborted while
	// waiting for resources *before* triggering delayed volume
	// provisioning?  On the one hand, volume provisioning is currently
	// irreversible, so it better should come last. On the other hand,
	// triggering both in parallel might be faster.
	return statusPending(logger, "waiting for resource driver to provide information", "pod", klog.KObj(pod))
}

func containsNode(hay []string, needle string) bool {
	for _, node := range hay {
		if node == needle {
			return true
		}
	}
	return false
}

// Unreserve clears the ReservedFor field for all claims.
// It's idempotent, and does nothing if no state found for the given pod.
func (pl *dynamicResources) Unreserve(ctx context.Context, cs *framework.CycleState, pod *v1.Pod, nodeName string) {
	if !pl.enabled {
		return
	}
	state, err := getStateData(cs)
	if err != nil {
		return
	}
	if len(state.claims) == 0 {
		return
	}

	logger := klog.FromContext(ctx)

	// Was publishing delayed? If yes, do it now.
	//
	// The most common scenario is that a different set of potential nodes
	// was identified. This revised set needs to be published to enable DRA
	// drivers to provide better guidance for future scheduling attempts.
	if state.podSchedulingState.isDirty() {
		if err := state.podSchedulingState.publish(ctx, pod, pl.clientset); err != nil {
			logger.Error(err, "publish PodSchedulingContext")
		}
	}

	for index, claim := range state.claims {
		// If allocation was in-flight, then it's not anymore and we need to revert the
		// claim object in the assume cache to what it was before.
		if state.informationsForClaim[index].controller != nil {
			if _, found := pl.inFlightAllocations.LoadAndDelete(state.claims[index].UID); found {
				pl.claimAssumeCache.Restore(claim.Namespace + "/" + claim.Name)
			}
		}

		if claim.Status.Allocation != nil &&
			resourceclaim.IsReservedForPod(pod, claim) {
			// Remove pod from ReservedFor. A strategic-merge-patch is used
			// because that allows removing an individual entry without having
			// the latest slice.
			patch := fmt.Sprintf(`{"metadata": {"uid": %q}, "status": { "reservedFor": [ {"$patch": "delete", "uid": %q} ] }}`,
				claim.UID,
				pod.UID,
			)
			logger.V(5).Info("unreserve", "resourceclaim", klog.KObj(claim), "pod", klog.KObj(pod))
			claim, err := pl.clientset.ResourceV1alpha2().ResourceClaims(claim.Namespace).Patch(ctx, claim.Name, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{}, "status")
			if err != nil {
				// We will get here again when pod scheduling is retried.
				logger.Error(err, "unreserve", "resourceclaim", klog.KObj(claim))
			}
		}
	}
}

// PreBind gets called in a separate goroutine after it has been determined
// that the pod should get bound to this node. Because Reserve did not actually
// reserve claims, we need to do it now. For claims with a builtin controller,
// we also handle the allocation.
//
// If anything fails, we return an error and
// the pod will have to go into the backoff queue. The scheduler will call
// Unreserve as part of the error handling.
func (pl *dynamicResources) PreBind(ctx context.Context, cs *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {
	if !pl.enabled {
		return nil
	}
	state, err := getStateData(cs)
	if err != nil {
		return statusError(klog.FromContext(ctx), err)
	}
	if len(state.claims) == 0 {
		return nil
	}

	logger := klog.FromContext(ctx)

	// Was publishing delayed? If yes, do it now and then cause binding to stop.
	// This will not happen if all claims get handled by builtin controllers.
	if state.podSchedulingState.isDirty() {
		if err := state.podSchedulingState.publish(ctx, pod, pl.clientset); err != nil {
			return statusError(logger, err)
		}
		return statusPending(logger, "waiting for resource driver", "pod", klog.KObj(pod), "node", klog.ObjectRef{Name: nodeName})
	}

	for index, claim := range state.claims {
		if !resourceclaim.IsReservedForPod(pod, claim) {
			claim, err := pl.reserveClaim(ctx, state, index, pod, nodeName)
			if err != nil {
				return statusError(logger, err)
			}
			state.claims[index] = claim
		}
	}
	// If we get here, we know that reserving the claim for
	// the pod worked and we can proceed with binding it.
	return nil
}

// reserveClaim gets called by PreBind for claim which is not reserved for the pod yet.
// It might not even be allocated.
func (pl *dynamicResources) reserveClaim(ctx context.Context, state *stateData, index int, pod *v1.Pod, nodeName string) (patchedClaim *resourcev1alpha2.ResourceClaim, finalErr error) {
	logger := klog.FromContext(ctx)
	claim := state.claims[index]
	allocationPatch := ""
	finalizerPatch := ""

	allocation := state.informationsForClaim[index].allocation
	logger.V(5).Info("preparing claim status patch", "claim", klog.KObj(state.claims[index]), "allocation", allocation)

	// Do we need to store an allocation result from Reserve?
	if allocation != nil {
		buffer, err := json.Marshal(allocation)
		if err != nil {
			return nil, fmt.Errorf("marshaling AllocationResult failed: %v", err)
		}
		allocationPatch = fmt.Sprintf(`"driverName": %q, "allocation": %s, `, state.informationsForClaim[index].allocationDriverName, string(buffer))
		finalizerPatch = fmt.Sprintf(`"finalizers": [%q], `, resourcev1alpha2.Finalizer)
	}

	// The claim might be stale, for example because the claim can get shared and some
	// other goroutine has updated it in the meantime. We therefore cannot use
	// SSA here to add the pod because then we would have to send the entire slice
	// or use different field manager strings for each entry.
	//
	// With a strategic-merge-patch, we can simply send one new entry. The apiserver
	// validation will catch if two goroutines try to do that at the same time and
	// the claim cannot be shared.
	//
	// Note that this also works when the allocation result gets added twice because
	// two pods both started using a shared claim: the first pod to get here adds the
	// allocation result. The second pod then only adds itself to reservedFor.
	patch := fmt.Sprintf(`{"metadata": {%s"uid": %q}, "status": {%s "reservedFor": [ {"resource": "pods", "name": %q, "uid": %q} ] }}`,
		finalizerPatch,
		claim.UID,
		allocationPatch,
		pod.Name,
		pod.UID,
	)
	if loggerV := logger.V(6); loggerV.Enabled() {
		logger.V(5).Info("reserve", "pod", klog.KObj(pod), "node", klog.ObjectRef{Name: nodeName}, "resourceclaim", klog.KObj(claim), "patch", patch)
	} else {
		logger.V(5).Info("reserve", "pod", klog.KObj(pod), "node", klog.ObjectRef{Name: nodeName}, "resourceclaim", klog.KObj(claim))
	}
	claim, err := pl.clientset.ResourceV1alpha2().ResourceClaims(claim.Namespace).Patch(ctx, claim.Name, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{}, "status")
	logger.V(5).Info("reserved", "pod", klog.KObj(pod), "node", klog.ObjectRef{Name: nodeName}, "resourceclaim", klog.Format(claim), "err", err)
	if allocationPatch != "" {
		// The scheduler was handling allocation. Now that has
		// completed, either successfully or with a failure.
		if err != nil {
			pl.claimAssumeCache.Restore(claim.Namespace + "/" + claim.Name)
		}
		pl.inFlightAllocations.Delete(claim.UID)
	}
	return claim, err
}

// PostBind is called after a pod is successfully bound to a node. Now we are
// sure that a PodSchedulingContext object, if it exists, is definitely not going to
// be needed anymore and can delete it. This is a one-shot thing, there won't
// be any retries.  This is okay because it should usually work and in those
// cases where it doesn't, the garbage collector will eventually clean up.
func (pl *dynamicResources) PostBind(ctx context.Context, cs *framework.CycleState, pod *v1.Pod, nodeName string) {
	if !pl.enabled {
		return
	}
	state, err := getStateData(cs)
	if err != nil {
		return
	}
	if len(state.claims) == 0 {
		return
	}

	// We cannot know for sure whether the PodSchedulingContext object exists. We
	// might have created it in the previous pod schedulingCtx cycle and not
	// have it in our informer cache yet. Let's try to delete, just to be
	// on the safe side.
	logger := klog.FromContext(ctx)
	err = pl.clientset.ResourceV1alpha2().PodSchedulingContexts(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
	switch {
	case apierrors.IsNotFound(err):
		logger.V(5).Info("no PodSchedulingContext object to delete")
	case err != nil:
		logger.Error(err, "delete PodSchedulingContext")
	default:
		logger.V(5).Info("PodSchedulingContext object deleted")
	}
}

// statusUnschedulable ensures that there is a log message associated with the
// line where the status originated.
func statusUnschedulable(logger klog.Logger, reason string, kv ...interface{}) *framework.Status {
	if loggerV := logger.V(5); loggerV.Enabled() {
		helper, loggerV := loggerV.WithCallStackHelper()
		helper()
		kv = append(kv, "reason", reason)
		// nolint: logcheck // warns because it cannot check key/values
		loggerV.Info("pod unschedulable", kv...)
	}
	return framework.NewStatus(framework.UnschedulableAndUnresolvable, reason)
}

// statusPending ensures that there is a log message associated with the
// line where the status originated.
func statusPending(logger klog.Logger, reason string, kv ...interface{}) *framework.Status {
	if loggerV := logger.V(5); loggerV.Enabled() {
		helper, loggerV := loggerV.WithCallStackHelper()
		helper()
		kv = append(kv, "reason", reason)
		// nolint: logcheck // warns because it cannot check key/values
		loggerV.Info("pod waiting for external component", kv...)
	}

	// When we return Pending, we want to block the Pod at the same time.
	return framework.NewStatus(framework.Pending, reason)
}

// statusError ensures that there is a log message associated with the
// line where the error originated.
func statusError(logger klog.Logger, err error, kv ...interface{}) *framework.Status {
	if loggerV := logger.V(5); loggerV.Enabled() {
		helper, loggerV := loggerV.WithCallStackHelper()
		helper()
		// nolint: logcheck // warns because it cannot check key/values
		loggerV.Error(err, "dynamic resource plugin failed", kv...)
	}
	return framework.AsStatus(err)
}
