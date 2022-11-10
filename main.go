package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"
	"syscall"

	"github.com/skerkour/rz"
	"github.com/skerkour/rz/log"
	"golang.org/x/sys/unix"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/sig-storage-lib-external-provisioner/v8/controller"
)

const (
	DefaultProvisionerName  = "roru/hostpath"
	DefaultProvisionerIDAnn = "pv.kubernetes.io/hostpath-provisioner-id"
)

/* Our provisioner class, which implements the controller API. */
type hostPathProvisioner struct {
	name     string // just a name, is not really used anywhere
	identity string // Unique provisioner identity to mark volume objects with
}

// NewHostPathProvisioner creates a new provisioner with a given id and name
func NewHostPathProvisioner(id string, name string) controller.Provisioner {
	return &hostPathProvisioner{
		name:     name,
		identity: id,
	}
}

var _ controller.Provisioner = &hostPathProvisioner{}

// Provision creates the physical on-disk path for this PV and return a new PV object
func (p *hostPathProvisioner) Provision(ctx context.Context, options controller.ProvisionOptions) (*v1.PersistentVolume, controller.ProvisioningState, error) {
	/*
	 * Extract the PV capacity as bytes.  We can use this to set CephFS
	 * quotas.
	 */
	log.Info("Start provision new volume")
	capacity := options.PVC.Spec.Resources.Requests[v1.ResourceStorage]
	volBytes := capacity.Value()
	if volBytes <= 0 {
		return nil, controller.ProvisioningFinished, fmt.Errorf("storage capacity must be >= 0 (not %+v)", capacity.String())
	}

	volumesDir := options.StorageClass.Parameters["pvDir"]

	// check free space on disk
	var fsStat unix.Statfs_t
	err := unix.Statfs(volumesDir, &fsStat)
	if err != nil {
		log.Error("Unable to get filesystem free space", rz.Error("error", err))
		return nil, controller.ProvisioningNoChange, err
	}
	freeSpace := fsStat.Bavail * uint64(fsStat.Bsize)
	log.Info("Update free space on disk", rz.Uint64("space", freeSpace))
	if uint64(volBytes) > freeSpace {
		log.Error("Requested capacity is too large, not enough free space to provision", rz.String("error", "NotEnoughSpace"))
		return nil, controller.ProvisioningFinished, fmt.Errorf("storage capacity must be <= %+v (not %+v)", strconv.FormatUint(freeSpace, 10), capacity.String())
	}

	// Create the on-disk directory.
	volumePath := path.Join(volumesDir, options.PVName)
	if err := os.MkdirAll(volumePath, 0777); err != nil {
		log.Error("failed to mkdir", rz.String("path", volumePath), rz.Error("error", err))
		return nil, controller.ProvisioningFinished, err
	}
	if err := os.Chmod(volumePath, 0777); err != nil {
		log.Error("failed to chmod", rz.String("path", volumePath), rz.Error("error", err))
		return nil, controller.ProvisioningFinished, err
	}
	log.Info("successfully chmoded", rz.String("path", volumePath))

	/* The actual PV we will create */
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: options.PVName,
			Annotations: map[string]string{
				DefaultProvisionerIDAnn: p.identity,
			},
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: *options.StorageClass.ReclaimPolicy,
			AccessModes:                   options.PVC.Spec.AccessModes,
			Capacity: v1.ResourceList{
				v1.ResourceStorage: options.PVC.Spec.Resources.Requests[v1.ResourceStorage],
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: volumePath,
				},
			},
		},
	}

	log.Info("successfully created hostpath volume",
		rz.String("volume", options.PVName), rz.String("path", volumePath))

	return pv, controller.ProvisioningFinished, nil
}

// Delete removes a PV path from the disk by deleting its directory
func (p *hostPathProvisioner) Delete(ctx context.Context, volume *v1.PersistentVolume) error {
	/* Ensure this volume was provisioned by us */
	ann, ok := volume.Annotations[DefaultProvisionerIDAnn]
	if !ok {
		log.Info("not removing volume: identity annotation missing",
			rz.String("volume", volume.Name), rz.String("annotation", DefaultProvisionerIDAnn))
		return errors.New("identity annotation not found on PV")
	}
	log.Info("Remove volume", rz.String("volume", volume.Name))
	if ann != p.identity {
		log.Info("not removing volume <%s>: identity annotation does not match ours",
			rz.String("volume", volume.Name), rz.String("id", p.identity), rz.String("annotation", DefaultProvisionerIDAnn))
		return &controller.IgnoredError{Reason: "identity annotation on PV does not match ours"}
	}

	volumePath := volume.Spec.HostPath.Path
	if err := os.RemoveAll(volumePath); err != nil {
		log.Error("failed to remove PV",
			rz.String("volume", volume.Name), rz.String("path", volumePath), rz.Error("error", err))
		return err
	}

	return nil
}

var (
	master     = flag.String("master", "", "Master URL")
	kubeconfig = flag.String("kubeconfig", "", "Absolute path to the kubeconfig")
	name       = flag.String("name", "", "Provisioner name")
	id         = flag.String("id", "", "Unique provisioner identity")
)

func main() {
	syscall.Umask(0)
	flag.Parse()

	provisionerId := DefaultProvisionerName
	if *id != "" {
		log.Info("setting custom Id")
		provisionerId = *id
	}
	provisionerName := DefaultProvisionerName
	if *name != "" {
		log.Info("setting custom name")
		provisionerName = *name
	}

	log.SetLogger(log.With(rz.Fields(
		rz.String("id", provisionerId),
	)))

	flag.Parse()

	// Configure k8s api client
	var config *rest.Config
	var err error
	if *master != "" || *kubeconfig != "" {
		log.Info("using out-of-cluster configuration")
		config, err = clientcmd.BuildConfigFromFlags(*master, *kubeconfig)
	} else {
		log.Info("using in-cluster configuration; use -master or -kubeconfig to change")
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		log.Fatal("failed to create config", rz.Error("error", err))
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal("failed to create client", rz.Error("error", err))
	}

	// create our provisioner and pass it to controller
	hostPathProvisioner := NewHostPathProvisioner(provisionerName, provisionerId)
	provisionController := controller.NewProvisionController(
		clientset,
		provisionerName,
		hostPathProvisioner,
		controller.MetricsPort(10254))

	provisionController.Run(context.Background())
}
