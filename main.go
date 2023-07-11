package main

import (
	"flag"
	"path/filepath"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/klog/v2"
)

func main() {
	klog.InitFlags(nil)
	flag.Set("logtostderr", "false")
	flag.Set("alsologtostderr", "false")
	flag.Parse()

	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	podListWatcher := cache.NewListWatchFromClient(clientset.CoreV1().RESTClient(), "pods", v1.NamespaceDefault, fields.Everything())

	_, informer := NewIndexerInformer(podListWatcher, &v1.Pod{}, 0, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			klog.V(2).Infof("add object: %v", obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			klog.V(2).Infof("add new object: %v", newObj)
		},
		DeleteFunc: func(obj interface{}) {},
	},
		cache.Indexers{})

	stop := make(chan struct{})
	defer close(stop)
	go informer.Run(stop)
	select {}

}
