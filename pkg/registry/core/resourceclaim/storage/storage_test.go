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

package storage

import (
	"testing"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/diff"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistrytest "k8s.io/apiserver/pkg/registry/generic/testing"
	"k8s.io/apiserver/pkg/registry/rest"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	"k8s.io/kubernetes/pkg/apis/core"
	_ "k8s.io/kubernetes/pkg/apis/core/install"
	"k8s.io/kubernetes/pkg/registry/registrytest"
)

func newStorage(t *testing.T) (*REST, *StatusREST, *etcd3testing.EtcdTestServer) {
	etcdStorage, server := registrytest.NewEtcdStorage(t, core.GroupName)
	restOptions := generic.RESTOptions{
		StorageConfig:           etcdStorage,
		Decorator:               generic.UndecoratedStorage,
		DeleteCollectionWorkers: 1,
		ResourcePrefix:          "resourceclaims",
	}
	resourceClaimStorage, statusStorage, err := NewREST(restOptions)
	if err != nil {
		t.Fatalf("unexpected error from REST storage: %v", err)
	}
	return resourceClaimStorage, statusStorage, server
}

func validNewResourceClaim(name, ns string) *core.ResourceClaim {
	claim := &core.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Spec: core.ResourceClaimSpec{
			ResourceClassName: "example",
			AllocationMode:    core.AllocationModeImmediate,
		},
		Status: core.ResourceClaimStatus{},
	}
	return claim
}

func TestCreate(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	claim := validNewResourceClaim("foo", metav1.NamespaceDefault)
	claim.ObjectMeta = metav1.ObjectMeta{}
	test.TestCreate(
		// valid
		claim,
		// invalid
		&core.ResourceClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "*BadName!"},
		},
	)
}

func TestUpdate(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestUpdate(
		// valid
		validNewResourceClaim("foo", metav1.NamespaceDefault),
		// updateFunc
		func(obj runtime.Object) runtime.Object {
			object := obj.(*core.ResourceClaim)
			if object.Labels == nil {
				object.Labels = map[string]string{}
			}
			object.Labels["foo"] = "bar"
			return object
		},
	)
}

func TestDelete(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ReturnDeletedObject()
	test.TestDelete(validNewResourceClaim("foo", metav1.NamespaceDefault))
}

func TestGet(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestGet(validNewResourceClaim("foo", metav1.NamespaceDefault))
}

func TestList(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestList(validNewResourceClaim("foo", metav1.NamespaceDefault))
}

func TestWatch(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestWatch(
		validNewResourceClaim("foo", metav1.NamespaceDefault),
		// matching labels
		[]labels.Set{},
		// not matching labels
		[]labels.Set{
			{"foo": "bar"},
		},
		// matching fields
		[]fields.Set{
			{"metadata.name": "foo"},
		},
		// not matching fields
		[]fields.Set{
			{"metadata.name": "bar"},
		},
	)
}

func TestUpdateStatus(t *testing.T) {
	storage, statusStorage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	ctx := genericapirequest.NewDefaultContext()

	key, _ := storage.KeyFunc(ctx, "foo")
	claimStart := validNewResourceClaim("foo", metav1.NamespaceDefault)
	err := storage.Storage.Create(ctx, key, claimStart, nil, 0, false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	claim := claimStart.DeepCopy()
	claim.Status.DriverName = "some-driver.example.com"
	claim.Status.Allocation = &core.AllocationResult{}
	_, _, err = statusStorage.Update(ctx, claim.Name, rest.DefaultUpdatedObjectInfo(claim), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	obj, err := storage.Get(ctx, "foo", &metav1.GetOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	claimOut := obj.(*core.ResourceClaim)
	// only compare relevant changes b/c of difference in metadata
	if !apiequality.Semantic.DeepEqual(claim.Status, claimOut.Status) {
		t.Errorf("unexpected object: %s", diff.ObjectDiff(claim.Status, claimOut.Status))
	}
}