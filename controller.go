package main

import (
	"fmt"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"os"
	"time"
)
import "k8s.io/client-go/kubernetes"

type Controller struct {
	client *kubernetes.Clientset
	events record.EventRecorder
	queue  workqueue.RateLimitingInterface

	svcIndexer  cache.Indexer
	svcInformer cache.Controller
	epIndexer   cache.Indexer
	epInformer  cache.Controller
}

type svcKey string
type epKey string
type breakpointKey string

func NewController() (*Controller, error) {
	kubeConfig := []byte(`apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJkekNDQVIyZ0F3SUJBZ0lCQURBS0JnZ3Foa2pPUFFRREFqQWpNU0V3SHdZRFZRUUREQmhyTTNNdGMyVnkKZG1WeUxXTmhRREUyTVRnMk5ETTJPRGt3SGhjTk1qRXdOREUzTURjeE5EUTVXaGNOTXpFd05ERTFNRGN4TkRRNQpXakFqTVNFd0h3WURWUVFEREJock0zTXRjMlZ5ZG1WeUxXTmhRREUyTVRnMk5ETTJPRGt3V1RBVEJnY3Foa2pPClBRSUJCZ2dxaGtqT1BRTUJCd05DQUFTV1UvVStreENINkNaUEdVbmRwNzVxWk1TNVRIM3FEaE5BNTRRck9ka3EKNHFaWW1YNFNTTVZpbm81S09SNWU1aDhmd2FCR0doRjRBR2hyUWRwOVpacDhvMEl3UURBT0JnTlZIUThCQWY4RQpCQU1DQXFRd0R3WURWUjBUQVFIL0JBVXdBd0VCL3pBZEJnTlZIUTRFRmdRVVlVNllWblAzWkxSc0lEbzVZL0IyCnNNRHRUWmN3Q2dZSUtvWkl6ajBFQXdJRFNBQXdSUUloQUpJalMzaS84ZTk2YzlQdTh4OHFMQ0p4NmZIeGRHSnAKaEFwMmJpOGZLdDN5QWlBbEJOTVpobFB5bks5MklMTFYyK3M3THJ3d3hSVnNBMXhpbnRmcGtGMTFsZz09Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K
    server: https://10.211.55.4:6443
  name: default
contexts:
- context:
    cluster: default
    user: default
  name: default
current-context: default
kind: Config
preferences: {}
users:
- name: default
  user:
    client-certificate-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJrVENDQVRlZ0F3SUJBZ0lJTnl3ZWdURW5lNFF3Q2dZSUtvWkl6ajBFQXdJd0l6RWhNQjhHQTFVRUF3d1kKYXpOekxXTnNhV1Z1ZEMxallVQXhOakU0TmpRek5qZzVNQjRYRFRJeE1EUXhOekEzTVRRME9Wb1hEVEl5TURReApOekEzTVRRME9Wb3dNREVYTUJVR0ExVUVDaE1PYzNsemRHVnRPbTFoYzNSbGNuTXhGVEFUQmdOVkJBTVRESE41CmMzUmxiVHBoWkcxcGJqQlpNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQTBJQUJKc1BYOE40c1ZtWmlGTzUKc083TUlRbmZTTi9aNkQrVk9YanVmOGRiRzgreGo4Z0h6elkyZDJERnlTdUFaaEZoMUVuZ21KRlpxWThHVUkyYQprVlhHQzVDalNEQkdNQTRHQTFVZER3RUIvd1FFQXdJRm9EQVRCZ05WSFNVRUREQUtCZ2dyQmdFRkJRY0RBakFmCkJnTlZIU01FR0RBV2dCUVFzMTlSR1VOeHEwTGRwUU83amZPdG9hVjlLakFLQmdncWhrak9QUVFEQWdOSUFEQkYKQWlFQTUxdG94R09XOEtzaWR2U2szTTJhdk9sK1pCV0JkeXJDTUZWSDJpUXo2c2tDSUdNRW85MVFGYjc1ZHQ5cwpJM1NuL1JIRTZsWVpXc294bWFMb2lRZnI0Y2JSCi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0KLS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJkekNDQVIyZ0F3SUJBZ0lCQURBS0JnZ3Foa2pPUFFRREFqQWpNU0V3SHdZRFZRUUREQmhyTTNNdFkyeHAKWlc1MExXTmhRREUyTVRnMk5ETTJPRGt3SGhjTk1qRXdOREUzTURjeE5EUTVXaGNOTXpFd05ERTFNRGN4TkRRNQpXakFqTVNFd0h3WURWUVFEREJock0zTXRZMnhwWlc1MExXTmhRREUyTVRnMk5ETTJPRGt3V1RBVEJnY3Foa2pPClBRSUJCZ2dxaGtqT1BRTUJCd05DQUFSSXRoazBUY3hvK000OGlCSnNhWkxDTXdHUEJSWm9zU3V2N2ZmNXlTcU4KczZkSVhjNm9aUFVmTllIa0VYUCtUVmFmcG9Oc3pwZDRENHR3YWJ5dnJlWkRvMEl3UURBT0JnTlZIUThCQWY4RQpCQU1DQXFRd0R3WURWUjBUQVFIL0JBVXdBd0VCL3pBZEJnTlZIUTRFRmdRVUVMTmZVUmxEY2F0QzNhVUR1NDN6CnJhR2xmU293Q2dZSUtvWkl6ajBFQXdJRFNBQXdSUUlnREZXMFhZZVYweDdlK3VFUXpENVBCaFFmTkExQUR3NzQKUXR2NHJERkcrQjRDSVFDRjh1cGVONEVIaTB3K1ViRWZacnpnRmQ2b21TMmFXaUVZdGxkai8rR29adz09Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K
    client-key-data: LS0tLS1CRUdJTiBFQyBQUklWQVRFIEtFWS0tLS0tCk1IY0NBUUVFSUpJZjdaT0c3R0pBeG9ZZTgwMVljSkY2KytiaVZVQzFZRm11MTRKRTlBNENvQW9HQ0NxR1NNNDkKQXdFSG9VUURRZ0FFbXc5ZnczaXhXWm1JVTdtdzdzd2hDZDlJMzlub1A1VTVlTzUveDFzYno3R1B5QWZQTmpaMwpZTVhKSzRCbUVXSFVTZUNZa1ZtcGp3WlFqWnFSVmNZTGtBPT0KLS0tLS1FTkQgRUMgUFJJVkFURSBLRVktLS0tLQo=`)
	k8sConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeConfig)
	if err != nil {
		fmt.Println(err)
	}
	//k8sConfig, err := rest.InClusterConfig()
	//if err != nil {
	//	klog.Error("load k8s config error.")
	//	return nil, err
	//}
	clientSet, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		klog.Error("create k8s client error.")
		return nil, err
	}

	// queue
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	// svc
	svcWatcher := cache.NewListWatchFromClient(clientSet.CoreV1().RESTClient(), "services", v1.NamespaceAll, fields.Everything())
	svcIndexer, svcInformer := cache.NewIndexerInformer(svcWatcher, &v1.Service{}, 0, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(svcKey(key))
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(newObj)
			if err == nil {
				queue.Add(svcKey(key))
			}
		},
		DeleteFunc: func(obj interface{}) {
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(svcKey(key))
			}
		},
	}, cache.Indexers{})
	// ep
	epWatcher := cache.NewListWatchFromClient(clientSet.CoreV1().RESTClient(), "endpoints", v1.NamespaceAll, fields.Everything())
	epIndexer, epInformer := cache.NewIndexerInformer(epWatcher, &v1.Endpoints{}, 0, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(epKey(key))
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(newObj)
			if err == nil {
				queue.Add(epKey(key))
			}
		},
		DeleteFunc: func(obj interface{}) {
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(epKey(key))
			}
		},
	}, cache.Indexers{})
	// events
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(clientSet.CoreV1().RESTClient()).Events("")})
	recorder := broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "olb"})

	klog.Info("Complete the controller creation")
	return &Controller{
		client:      clientSet,
		queue:       queue,
		svcIndexer:  svcIndexer,
		svcInformer: svcInformer,
		epIndexer:   epIndexer,
		epInformer:  epInformer,
		events:      recorder,
	}, nil
}

func (c *Controller) Run(stopCh chan struct{}) {
	defer runtime.HandleCrash()

	// Let the workers stop when we are done
	defer c.queue.ShutDown()
	klog.Info("Starting controller")

	go c.svcInformer.Run(stopCh)
	go c.epInformer.Run(stopCh)

	// Wait for all involved caches to be synced, before processing items from the queue is started
	if !cache.WaitForCacheSync(stopCh, c.svcInformer.HasSynced, c.epInformer.HasSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	c.queue.Add(breakpointKey(""))

	wait.Until(c.sync, time.Second, stopCh)

	<-stopCh
	klog.Info("Stopping Pod controller")
	//if stopCh != nil {
	//	go func() {
	//		<-stopCh
	//		c.queue.ShutDown()
	//	}()
	//}
	//
	//for {
	//	c.sync()
	//}
}

func (c *Controller) sync() {
	// Wait until there is a new item in the working queue
	key, quit := c.queue.Get()
	if quit {
		return
	}
	// Tell the queue that we are done with processing this key. This unblocks the key for other workers
	// This allows safe parallel processing because two pods with the same key are never processed in
	// parallel.
	defer c.queue.Done(key)

	switch k := key.(type) {
	case svcKey:
		svc, exists, err := c.svcIndexer.GetByKey(string(k))
		if err != nil {
			klog.Errorf("Fetching object with key %s from store failed with %v", key.(string), err)
			return
		}
		if !exists {
			// todo delete lb, if any
			fmt.Printf("svc %s does not exist anymore\n", key)
		} else {
			// todo set(add,change,delete) lb
			fmt.Printf("svc %s ssssssssssssss\n", key)
			//fmt.Printf("Sync/Add/Update for svc %v\n", svc)
			fmt.Printf("Sync/Add/Update for svc %s\n", svc.(*v1.Service).GetName())
		}
		return
	case epKey:
		ep, exists, err := c.epIndexer.GetByKey(string(k))
		if err != nil {
			klog.Errorf("Fetching object with key %s from store failed with %v", key.(string), err)
			return
		}
		if !exists {
			fmt.Printf("ep %s does not exist anymore\n", key)
		} else {
			c.queue.AddRateLimited(k)
			// todo update backend ips , if service type=lb
			fmt.Printf("ep %s ssssssssssssss\n", key)
			//fmt.Printf("Sync/Add/Update for ep %v\n", ep)
			c.queue.Forget(k)
			fmt.Printf("Sync/Add/Update for ep %s\n", ep.(*v1.Endpoints).GetName())
		}
		return
	case breakpointKey:
		fmt.Printf("breakpointKey %s ssssssssssssss\n", key)
		ep, exists, err := c.svcIndexer.GetByKey(string(k))
		if err != nil {
			klog.Errorf("Fetching object with key %s from store failed with %v", key.(string), err)
			return
		}
		if !exists {
			fmt.Printf("breakpoint %s does not exist anymore\n", key)
		} else {
			fmt.Printf("Sync/Add/Update for breakpoint %s\n", ep.(*v1.Endpoints).GetName())
		}
		return
	default:
		panic(fmt.Errorf("unknown key type for %#v (%T)", key, key))
	}
}

func main() {
	controller, err := NewController()
	if err != nil {
		fmt.Println("new controller err")
		os.Exit(1)
	}
	controller.Run(nil)
}
