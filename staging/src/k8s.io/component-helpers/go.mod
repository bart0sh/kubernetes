// This is a generated file. Do not edit directly.

module k8s.io/component-helpers

go 1.16

require (
	github.com/google/go-cmp v0.5.5
	k8s.io/api v0.0.0
	k8s.io/apimachinery v0.0.0
	k8s.io/client-go v0.0.0
	k8s.io/klog/v2 v2.60.1
	k8s.io/utils v0.0.0-20220210201930-3a6ce19ff2f9
)

replace (
	github.com/fsnotify/fsnotify => github.com/fsnotify/fsnotify v1.4.9
	k8s.io/api => ../api
	k8s.io/apimachinery => ../apimachinery
	k8s.io/client-go => ../client-go
	k8s.io/component-helpers => ../component-helpers
	sigs.k8s.io/yaml => sigs.k8s.io/yaml v1.2.0
)
