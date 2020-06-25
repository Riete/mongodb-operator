package mongodb

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

	middlewarev1alpha1 "github.com/riete/mongodb-operator/pkg/apis/middleware/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller_mongodb")

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new MongoDB Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileMongoDB{client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("mongodb-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource MongoDB
	err = c.Watch(&source.Kind{Type: &middlewarev1alpha1.MongoDB{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner MongoDB
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &middlewarev1alpha1.MongoDB{},
	})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileMongoDB implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileMongoDB{}

// ReconcileMongoDB reconciles a MongoDB object
type ReconcileMongoDB struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
}

// Reconcile reads that state of the cluster for a MongoDB object and makes changes based on the state read
// and what is in the MongoDB.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileMongoDB) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling MongoDB")

	// Fetch the MongoDB instance
	instance := &middlewarev1alpha1.MongoDB{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	if instance.Spec.Mode == middlewarev1alpha1.ReplicaSet && instance.Spec.Members%2 == 0 {
		return reconcile.Result{}, fmt.Errorf("mongodb replica sets should always have an odd number of members")
	}

	if instance.Spec.AuthEnabled {
		if instance.Spec.StorageClass == "" || instance.Spec.StorageSize == "" {
			return reconcile.Result{}, fmt.Errorf("auth enable must specify storage class and storage size")
		}
		if instance.Spec.Mode == middlewarev1alpha1.ReplicaSet {
			// 	new key file config map
			configMap := newKeyFileConfigMap(instance)
			if err := controllerutil.SetControllerReference(instance, configMap, r.scheme); err != nil {
				return reconcile.Result{}, err
			}
			configMapFound := &corev1.ConfigMap{}
			err = r.client.Get(context.TODO(), types.NamespacedName{Name: configMap.Name, Namespace: configMap.Namespace}, configMapFound)
			if err != nil && errors.IsNotFound(err) {
				reqLogger.Info("Creating a new configmap", "ConfigMap.Namespace", configMap.Namespace, "ConfigMap.Name", configMap.Name)
				err = r.client.Create(context.TODO(), configMap)
				if err != nil {
					return reconcile.Result{}, err
				}
			}
		}
	}

	// Define a new service object
	service := newStatefulSetService(instance)

	// Set RedisCluster instance as the owner and controller
	if err := controllerutil.SetControllerReference(instance, service, r.scheme); err != nil {
		return reconcile.Result{}, err
	}

	// Check if service already exists
	serviceFound := &corev1.Service{}
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: service.Name, Namespace: service.Namespace}, serviceFound)
	if err != nil && errors.IsNotFound(err) {
		reqLogger.Info("Creating a new service", "Service.Namespace", service.Namespace, "Service.Name", service.Name)
		err = r.client.Create(context.TODO(), service)
		if err != nil {
			return reconcile.Result{}, err
		}
	}

	// Define a new statefulset object
	statefulset := newStatefulSet(instance)

	// Set RedisCluster instance as the owner and controller
	if err := controllerutil.SetControllerReference(instance, statefulset, r.scheme); err != nil {
		return reconcile.Result{}, err
	}

	// Check if statefulset already exists
	statefulsetFound := &appsv1.StatefulSet{}
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: statefulset.Name, Namespace: statefulset.Namespace}, statefulsetFound)
	if err != nil && errors.IsNotFound(err) {
		reqLogger.Info("Creating a new statefulset", "StatefulSet.Namespace", statefulset.Namespace, "StatefulSet.Name", statefulset.Name)
		err = r.client.Create(context.TODO(), statefulset)
		if err != nil {
			return reconcile.Result{}, err
		}
	}

	if statefulsetFound.Spec.Replicas == nil {
		return reconcile.Result{Requeue: true}, nil
	} else if *statefulsetFound.Spec.Replicas != statefulsetFound.Status.ReadyReplicas {
		reqLogger.Info("Wait all pod ready", "StatefulSet.Namespace",
			statefulsetFound.Namespace, "StatefulSet.Name", statefulsetFound.Name, "total", *statefulsetFound.Spec.Replicas,
			"current", statefulsetFound.Status.ReadyReplicas)
		return reconcile.Result{Requeue: true, RequeueAfter: time.Second * 30}, nil
	}

	if instance.Spec.Mode == middlewarev1alpha1.ReplicaSet {
		// init replica set
		// Define a new job object
		replicaSetInitJob := newReplicaSetInitJob(instance)

		// Set RedisCluster instance as the owner and controller
		if err := controllerutil.SetControllerReference(instance, replicaSetInitJob, r.scheme); err != nil {
			return reconcile.Result{}, err
		}

		// Check if job already exists
		initJobFound := &batchv1.Job{}
		err = r.client.Get(context.TODO(), types.NamespacedName{Name: replicaSetInitJob.Name, Namespace: replicaSetInitJob.Namespace}, initJobFound)
		if err != nil && errors.IsNotFound(err) {
			reqLogger.Info("Creating a new job", "Job.Namespace", replicaSetInitJob.Namespace, "Job.Name", replicaSetInitJob.Name)
			err = r.client.Create(context.TODO(), replicaSetInitJob)
			if err != nil {
				return reconcile.Result{}, err
			}
		}
		if initJobFound.Status.Succeeded == 0 {
			return reconcile.Result{Requeue: true, RequeueAfter: time.Second * 15}, nil
		}
	}

	// create root user
	// Define a new job object
	createRootJob := newCreateRootJob(instance)

	// Set RedisCluster instance as the owner and controller
	if err := controllerutil.SetControllerReference(instance, createRootJob, r.scheme); err != nil {
		return reconcile.Result{}, err
	}

	// Check if job already exists
	createRootJobFound := &batchv1.Job{}
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: createRootJob.Name, Namespace: createRootJob.Namespace}, createRootJobFound)
	if err != nil && errors.IsNotFound(err) {
		reqLogger.Info("Creating a new job", "Job.Namespace", createRootJob.Namespace, "Job.Name", createRootJob.Name)
		err = r.client.Create(context.TODO(), createRootJob)
		if err != nil {
			return reconcile.Result{}, err
		}
	}

	// enable auth
	if instance.Spec.AuthEnabled {
		_ = r.client.Get(context.TODO(), types.NamespacedName{Name: statefulset.Name, Namespace: statefulset.Namespace}, statefulsetFound)
		if instance.Spec.Mode == middlewarev1alpha1.ReplicaSet {
			statefulsetFound.Spec.Template.Spec.Containers[0].Command = []string{
				"bash",
				"-c",
				fmt.Sprintf("sleep 10 && mongod --replSet %s --keyFile /etc/keyFile --bind_ip_all", statefulsetFound.Name),
			}
			statefulsetFound.Spec.Template.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{
				PostStart: &corev1.Handler{
					Exec: &corev1.ExecAction{
						Command: []string{
							"bash",
							"-c",
							"echo $KEY_FILE > /etc/keyFile && chown mongodb.mongodb /etc/keyFile && chmod 400 /etc/keyFile"},
					},
				},
			}
		} else if instance.Spec.Mode == middlewarev1alpha1.Standalone {
			statefulsetFound.Spec.Template.Spec.Containers[0].Args = []string{
				"--auth",
			}
		}
		err = r.client.Update(context.TODO(), statefulsetFound)
		if err != nil {
			reqLogger.Error(err, "Failed to update StatefulSet", "StatefulSet.Namespace", statefulsetFound.Namespace, "StatefulSet.Name", statefulsetFound.Name)
			return reconcile.Result{}, err
		}
		reqLogger.Info("Enable AUTH", "StatefulSet.Namespace", statefulsetFound.Namespace, "StatefulSet.Name", statefulsetFound.Name)
	}

	// mongodb already exists - don't requeue
	reqLogger.Info("Skip reconcile: mongodb rs already exists", "MongoDB.Namespace", instance.Namespace, "MongoDB.Name", instance.Name)
	return reconcile.Result{}, nil
}

func newReplicaSetInitJob(mongo *middlewarev1alpha1.MongoDB) *batchv1.Job {
	image := fmt.Sprintf("%s:%s", middlewarev1alpha1.REPOSITORY, mongo.Spec.Tag)
	var members []string
	for i := 0; i < int(mongo.Spec.Members); i++ {
		members = append(members, fmt.Sprintf(`{ _id: %d, host: "%s-%d.%s:27017" }`, i, mongo.Name, i, mongo.Name))
	}
	mongoHost := fmt.Sprintf("%s-0.%s:27017", mongo.Name, mongo.Name)
	clusterInitCmd := fmt.Sprintf(
		`rs.initiate( {_id : "%s",members: [%s]})`, mongo.Name, strings.Join(members, ","))

	var retry int32 = 0
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-replica-set-init", mongo.Name),
			Namespace: mongo.Namespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &retry,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    "rs-init",
							Image:   image,
							Command: []string{"mongo"},
							Args:    []string{"--host", mongoHost, "--eval", clusterInitCmd},
						},
					},
				},
			},
		},
	}
}

func newCreateRootJob(mongo *middlewarev1alpha1.MongoDB) *batchv1.Job {
	image := fmt.Sprintf("%s:%s", middlewarev1alpha1.REPOSITORY, mongo.Spec.Tag)
	mongoHost := fmt.Sprintf("%s-0.%s:27017", mongo.Name, mongo.Name)
	createRootCmd := fmt.Sprintf(
		`db.createUser({user:"%s",pwd:"%s",roles:["root"]})`, mongo.Spec.RootUser, mongo.Spec.RootPwd)

	var retry int32 = 0
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-create-root", mongo.Name),
			Namespace: mongo.Namespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &retry,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    "create-root",
							Image:   image,
							Command: []string{"mongo"},
							Args:    []string{"--host", mongoHost, "admin", "--eval", createRootCmd},
						},
					},
				},
			},
		},
	}
}

func newStatefulSetService(mongo *middlewarev1alpha1.MongoDB) *corev1.Service {
	labels := map[string]string{"app": mongo.Name}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mongo.Name,
			Namespace: mongo.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels,
			Type:      corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{{
				Name:       "mongo",
				Protocol:   corev1.ProtocolTCP,
				Port:       27017,
				TargetPort: intstr.FromInt(27017),
			}},
		},
	}
}

func newStatefulSet(mongo *middlewarev1alpha1.MongoDB) *appsv1.StatefulSet {
	labels := map[string]string{"app": mongo.Name}
	image := fmt.Sprintf("%s:%s", middlewarev1alpha1.REPOSITORY, mongo.Spec.Tag)

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mongo.Name,
			Namespace: mongo.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: mongo.Name,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Image: image,
						Name:  mongo.Name,
						Ports: []corev1.ContainerPort{{
							ContainerPort: 27017,
							Name:          "mongo",
							Protocol:      corev1.ProtocolTCP,
						}},
						Resources: mongo.Spec.Resources,
					}},
				},
			},
		},
	}

	if mongo.Spec.StorageClass != "" {
		storageSize, _ := resource.ParseQuantity(mongo.Spec.StorageSize)
		sts.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{
			{Name: "data", MountPath: "/data/db"},
		}
		sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{{
			ObjectMeta: metav1.ObjectMeta{
				Name: "data",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: storageSize,
					},
				},
				StorageClassName: &mongo.Spec.StorageClass,
			},
		}}
	}

	if mongo.Spec.Mode == middlewarev1alpha1.ReplicaSet {
		sts.Spec.Replicas = &mongo.Spec.Members
		sts.Spec.Template.Spec.Containers[0].Command = []string{
			"bash",
			"-c",
			fmt.Sprintf("sleep 10 && mongod --replSet %s --bind_ip_all", mongo.Name),
		}
		sts.Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{
			{Name: "KEY_FILE", ValueFrom: &corev1.EnvVarSource{
				ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: fmt.Sprintf("%s-key-file", mongo.Name),
					},
					Key: "keyFile",
				},
			}},
		}
	}

	if mongo.Spec.Mode == middlewarev1alpha1.Standalone {
		var replicas int32 = 1
		sts.Spec.Replicas = &replicas
		if mongo.Spec.AuthEnabled {
			sts.Spec.Template.Spec.Containers[0].Args = []string{
				"--auth",
			}
		}
	}

	return sts
}

func newKeyFileConfigMap(mongo *middlewarev1alpha1.MongoDB) *corev1.ConfigMap {
	data := base64.StdEncoding.EncodeToString(getRandomString())
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-key-file", mongo.Name),
			Namespace: mongo.Namespace,
		},
		Data: map[string]string{"keyFile": data},
	}
}

func getRandomString() []byte {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	bytesLen := len(bytes)
	var result []byte
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 300; i++ {
		result = append(result, bytes[r.Intn(bytesLen)])
	}
	return result
}
