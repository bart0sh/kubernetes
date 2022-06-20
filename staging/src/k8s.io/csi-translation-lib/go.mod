// This is a generated file. Do not edit directly.

module k8s.io/csi-translation-lib

go 1.16

require (
	github.com/stretchr/testify v1.7.0
	k8s.io/api v0.0.0
	k8s.io/apimachinery v0.0.0
	k8s.io/klog/v2 v2.60.1
)

replace (
	github.com/fsnotify/fsnotify => github.com/fsnotify/fsnotify v1.4.9
	k8s.io/api => ../api
	k8s.io/apimachinery => ../apimachinery
	k8s.io/csi-translation-lib => ../csi-translation-lib
	sigs.k8s.io/yaml => sigs.k8s.io/yaml v1.2.0
)
