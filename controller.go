package main

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
type syncLB string

func NewController() (*Controller, error) {
	kubeConfig := []byte(`apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJlRENDQVIyZ0F3SUJBZ0lCQURBS0JnZ3Foa2pPUFFRREFqQWpNU0V3SHdZRFZRUUREQmhyTTNNdGMyVnkKZG1WeUxXTmhRREUyTWpVNE5EazVORE13SGhjTk1qRXdOekE1TVRZMU9UQXpXaGNOTXpFd056QTNNVFkxT1RBegpXakFqTVNFd0h3WURWUVFEREJock0zTXRjMlZ5ZG1WeUxXTmhRREUyTWpVNE5EazVORE13V1RBVEJnY3Foa2pPClBRSUJCZ2dxaGtqT1BRTUJCd05DQUFRc0tOcEI3VnFLUkpMQkNqbVNjWEdSK0FseHJpTjI4TTBxaTNXQmlIVVgKNjY3dWY5dDdDVXB1Q0lFSy9YS3pFOG9QUU14dUJTMXI3aUU1RkxlS3FKOVJvMEl3UURBT0JnTlZIUThCQWY4RQpCQU1DQXFRd0R3WURWUjBUQVFIL0JBVXdBd0VCL3pBZEJnTlZIUTRFRmdRVXlidUxlNmZ6b09WZ1M2RkZoVElHClYxTjQ5bWt3Q2dZSUtvWkl6ajBFQXdJRFNRQXdSZ0loQUlCM1J3QkQxTVhublc2YmVFajZqUEFwMHBFOXdrbW8Kb1ZrL0Y1d09oS0k2QWlFQXZ3aWNGaDBDZ3c5bHBLQ3VXZ3BFU2lUS2VlVnI5aEZTc0hCcW1MYnllVlk9Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K
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
    client-certificate-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJrRENDQVRlZ0F3SUJBZ0lJVkd2cURuWTFvV2d3Q2dZSUtvWkl6ajBFQXdJd0l6RWhNQjhHQTFVRUF3d1kKYXpOekxXTnNhV1Z1ZEMxallVQXhOakkxT0RRNU9UUXpNQjRYRFRJeE1EY3dPVEUyTlRrd00xb1hEVEl5TURjdwpPVEUyTlRrd00xb3dNREVYTUJVR0ExVUVDaE1PYzNsemRHVnRPbTFoYzNSbGNuTXhGVEFUQmdOVkJBTVRESE41CmMzUmxiVHBoWkcxcGJqQlpNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQTBJQUJLTmltQnlseW51UjY0eWEKYkZobnMxeHd5dGZpUiswNVlrTzh6a3MwdEN4YzUxN1dENGNndnhyTSsxaCtRaXNvWG1pM3hGaVdKaGo1YkQ2NgpLemJXNnRhalNEQkdNQTRHQTFVZER3RUIvd1FFQXdJRm9EQVRCZ05WSFNVRUREQUtCZ2dyQmdFRkJRY0RBakFmCkJnTlZIU01FR0RBV2dCUlJBelVPcnN1N1kwOHZxUTE4dGhaV2p1YmpqekFLQmdncWhrak9QUVFEQWdOSEFEQkUKQWlCUXV5N0FXWW9VL25hYkpyZkhrRWM5L0I3RTl4VkYwUDhYNE9oR2puWjlOd0lnVjFmam9ZVUpnZUVTMTloRgpnVTd4SDJYZ1JWMWh3TkdtYSswL2tVM1FpNW89Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0KLS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJkakNDQVIyZ0F3SUJBZ0lCQURBS0JnZ3Foa2pPUFFRREFqQWpNU0V3SHdZRFZRUUREQmhyTTNNdFkyeHAKWlc1MExXTmhRREUyTWpVNE5EazVORE13SGhjTk1qRXdOekE1TVRZMU9UQXpXaGNOTXpFd056QTNNVFkxT1RBegpXakFqTVNFd0h3WURWUVFEREJock0zTXRZMnhwWlc1MExXTmhRREUyTWpVNE5EazVORE13V1RBVEJnY3Foa2pPClBRSUJCZ2dxaGtqT1BRTUJCd05DQUFTWVhsS3orOEZidHRPNElmQ2JTNm9DQ203WUdOSWRZMVNSaEVxLzJ6MGkKaFQwNFlBUHlaVlVBOTRWb25Gakt1TEpibWVwY3AwUlhqQ3ZmNFBDM3RGNk9vMEl3UURBT0JnTlZIUThCQWY4RQpCQU1DQXFRd0R3WURWUjBUQVFIL0JBVXdBd0VCL3pBZEJnTlZIUTRFRmdRVVVRTTFEcTdMdTJOUEw2a05mTFlXClZvN200NDh3Q2dZSUtvWkl6ajBFQXdJRFJ3QXdSQUlnU2NaZGV3Ni9lOHBXWFV6NW9BS1ZaajN5MUM0TW56VVQKQnBDRFp4bmxxSEVDSUJ0RDdHVk5xdHlreWxOYVVzZVIxWHRVRkltUnZEeVlneHE5UXlBbWkrU1oKLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo=
    client-key-data: LS0tLS1CRUdJTiBFQyBQUklWQVRFIEtFWS0tLS0tCk1IY0NBUUVFSUhwZzZUUXpqSmQ3d2plYlhRVDRPUnVldTVDb1ZuVy94eE1iR1VNOEgxNmpvQW9HQ0NxR1NNNDkKQXdFSG9VUURRZ0FFbzJLWUhLWEtlNUhyakpwc1dHZXpYSERLMStKSDdUbGlRN3pPU3pTMExGem5YdFlQaHlDLwpHc3o3V0g1Q0t5aGVhTGZFV0pZbUdQbHNQcm9yTnRicTFnPT0KLS0tLS1FTkQgRUMgUFJJVkFURSBLRVktLS0tLQo=`)
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
			// Add event initiated by svc
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(newObj)
			if err == nil {
				queue.Add(epKey(key))
			}
		},
		DeleteFunc: func(obj interface{}) {
			// Delete event initiated by svc
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

	// todo when the program start, Clean up dirty configuration in lb
	c.queue.Add(syncLB(""))

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
			klog.Infof("svc %s does not exist anymore", key)
			// todo delete lb, if any

		} else {
			klog.Infof("Sync/Add/Update for svc %s", svc.(*v1.Service).GetName())
			// todo add,delete lb, or add epKey into queue
			// list all svc(have lb ip, add ep key into queue)
			// changed to LoadBalancer mode or from LoadBalancer mode(add/delete new lb)
			// watch add svc(add new lb)
			if !c.addLoadBalancer(string(k), svc.(*v1.Service)) {
				klog.Error("set load balancer err.")
			}
		}
		return
	case epKey:
		// Since there are only update key in the queue, we only process updates
		ep, exists, err := c.epIndexer.GetByKey(string(k))
		if err != nil {
			klog.Errorf("Fetching object with key %s from store failed with %v", key.(string), err)
			return
		}
		if !exists {
			klog.Errorf("ep %s does not exist anymore. there should be no non-existent objects in the queue!", key)
		} else {
			klog.Infof("Update for ep %s\n", ep.(*v1.Endpoints).GetName())
			// todo update backend ips , if service type=lb

		}
		return
	case syncLB:
		// todo Clean up dirty configuration in lb
		// list lb
		// for range lb, in range get lb-svc, if lb-svc not exist, delete lb
	default:
		panic(fmt.Errorf("unknown key type for %#v (%T)", key, key))
	}
}

func (c *Controller) addLoadBalancer(key string, svc *v1.Service) bool {
	if svc.Spec.Type != "LoadBalancer" {
		klog.Infof("svc %s is not in LoadBalancer mode, or changed from LoadBalancer mode.", key)
		svc.Status.LoadBalancer = v1.LoadBalancerStatus{}
		// todo try clean lb instance, if exist

		return true
	}

	// when the process restarts, need to add the key(epKey type) into the queue
	if len(svc.Status.LoadBalancer.Ingress) != 0 {
		// todo check vip, if changed , log err and update lb. logically impossible, we will not deal with it temporarily

		// add epKey queue, maybe update lb backend
		c.queue.Forget(epKey(key))
		c.queue.AddRateLimited(epKey(key))
		klog.Infof("add the epKey into the queue, this log should occur when the process just started")
	}

	// todo add lb
	// 1.get endpoints
	// 2.create lb get address(if exist, delete it), call lb create api, If it fails, add to the queue and process again

	// this lbAddress for testing only
	lbAddress := "1.2.3.4"
	svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{IP: lbAddress}}
	klog.Infof("get load balancer ip: %s", lbAddress)

	// update k8s svc
	if err := updateStatus(c.client, svc); err != nil {
		klog.Errorf("update svc status err: %v", err)
		return false
	}
	return true

}

func (c *Controller) updateLBBackend(key string, ep v1.Endpoints) bool {
	// todo get endpoints, update lb backend ip&port, if changed
	// 1.get endpoint(if it not have addresses, update nil lb）
	// 2.update lb, if the backend of lb is incorrect
	// 3.return true

	return true
}

func updateStatus(c *kubernetes.Clientset, svc *v1.Service) error {
	_, err := c.CoreV1().Services(svc.Namespace).UpdateStatus(context.TODO(), svc, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	return nil
}

func main() {
	// todo leader cluster
	controller, err := NewController()
	if err != nil {
		fmt.Println("new controller err")
		os.Exit(1)
	}
	controller.Run(nil)
}
