---
title: Deploying Apache BookKeeper on Kubernetes
tags: [Kubernetes, Google Container Engine]
logo: img/kubernetes-logo.png
---

Apache BookKeeper can be easily deployed in [Kubernetes](https://kubernetes.io/) clusters. The managed clusters on [Google Container Engine](https://cloud.google.com/compute/) is the most convenient way.

The deployment method shown in this guide relies on [YAML](http://yaml.org/) definitions for Kubernetes [resources](https://kubernetes.io/docs/resources-reference/v1.6/). The [`kubernetes`](https://github.com/apache/bookkeeper/tree/master/deploy/kubernetes) subdirectory holds resource definitions for:

* A three-node ZooKeeper cluster
* A BookKeeper cluster with a bookie runs on each node.

## Setup on Google Container Engine

To get started, get source code of [`kubernetes`](https://github.com/apache/bookkeeper/tree/master/deploy/kubernetes) from github by git clone.

If you'd like to change the number of bookies,  or ZooKeeper nodes in your BookKeeper cluster, modify the `replicas` parameter in the `spec` section of the appropriate [`Deployment`](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) or [`StatefulSet`](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) resource.

[Google Container Engine](https://cloud.google.com/container-engine) (GKE) automates the creation and management of Kubernetes clusters in [Google Compute Engine](https://cloud.google.com/compute/) (GCE).

### Prerequisites

To get started, you'll need:

* A Google Cloud Platform account, which you can sign up for at [cloud.google.com](https://cloud.google.com)
* An existing Cloud Platform project
* The [Google Cloud SDK](https://cloud.google.com/sdk/downloads) (in particular the [`gcloud`](https://cloud.google.com/sdk/gcloud/) and [`kubectl`]() tools).

### Create a new Kubernetes cluster

You can create a new GKE cluster using the [`container clusters create`](https://cloud.google.com/sdk/gcloud/reference/container/clusters/create) command for `gcloud`. This command enables you to specify the number of nodes in the cluster, the machine types of those nodes, and more.

As an example, we'll create a new GKE cluster for Kubernetes version [1.6.4](https://github.com/kubernetes/kubernetes/blob/master/CHANGELOG.md#v164) in the [us-central1-a](https://cloud.google.com/compute/docs/regions-zones/regions-zones#available) zone. The cluster will be named `bookkeeper-gke-cluster` and will consist of three VMs, each using two locally attached SSDs and running on [n1-standard-8](https://cloud.google.com/compute/docs/machine-types) machines. These SSDs will be used by Bookie instances, one for the BookKeeper journal and the other for storing the actual data.

```bash
$ gcloud config set compute/zone us-central1-a
$ gcloud config set project your-project-name
$ gcloud container clusters create bookkeeper-gke-cluster \
  --machine-type=n1-standard-8 \
  --num-nodes=3 \
  --local-ssd-count=2 \
  --enable-kubernetes-alpha
```

By default, bookies will run on all the machines that have locally attached SSD disks. In this example, all of those machines will have two SSDs, but you can add different types of machines to the cluster later. You can control which machines host bookie servers using [labels](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels).

### Dashboard

You can observe your cluster in the [Kubernetes Dashboard](https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/) by downloading the credentials for your Kubernetes cluster and opening up a proxy to the cluster:

```bash
$ gcloud container clusters get-credentials bookkeeper-gke-cluster \
  --zone=us-central1-a \
  --project=your-project-name
$ kubectl proxy
```

By default, the proxy will be opened on port 8001. Now you can navigate to [localhost:8001/ui](http://localhost:8001/ui) in your browser to access the dashboard. At first your GKE cluster will be empty, but that will change as you begin deploying.

When you create a cluster, your `kubectl` config in `~/.kube/config` (on MacOS and Linux) will be updated for you, so you probably won't need to change your configuration. Nonetheless, you can ensure that `kubectl` can interact with your cluster by listing the nodes in the cluster:

```bash
$ kubectl get nodes
```

If `kubectl` is working with your cluster, you can proceed to deploy ZooKeeper and Bookies.

### ZooKeeper

You *must* deploy ZooKeeper as the first component, as it is a dependency for the others.

```bash
$ kubectl apply -f zookeeper.yaml
```

Wait until all three ZooKeeper server pods are up and have the status `Running`. You can check on the status of the ZooKeeper pods at any time:

```bash
$ kubectl get pods -l component=zookeeper
NAME      READY     STATUS             RESTARTS   AGE
zk-0      1/1       Running            0          18m
zk-1      1/1       Running            0          17m
zk-2      0/1       Running            6          15m
```

This step may take several minutes, as Kubernetes needs to download the Docker image on the VMs.


If you want to connect to one of the remote zookeeper server, you can use[zk-shell](https://github.com/rgs1/zk_shell), you need to forward a local port to the
remote zookeeper server:

```bash
$ kubectl port-forward zk-0 2181:2181
$ zk-shell localhost 2181
```

### Deploy Bookies

Once ZooKeeper cluster is Running, you can then deploy the bookies. You can deploy the bookies either using a [DaemonSet](https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/) or a [StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/).

> NOTE: _DaemonSet_ vs _StatefulSet_
>
> A _DaemonSet_ ensures that all (or some) nodes run a pod of bookie instance. As nodes are added to the cluster, bookie pods are added automatically to them. As nodes are removed from the
> cluster, those bookie pods are garbage collected. The bookies deployed in a DaemonSet stores data on the local disks on those nodes. So it doesn't require any external storage for Persistent
> Volumes.
>
> A _StatefulSet_ maintains a sticky identity for the pods that it runs and manages. It provides stable and unique network identifiers, and stable and persistent storage for each pod. The pods
> are not interchangeable, the idenifiers for each pod are maintained across any rescheduling.
>
> Which one to use? A _DaemonSet_ is the easiest way to deploy a bookkeeper cluster, because it doesn't require additional persistent volume provisioner and use local disks. BookKeeper manages
> the data replication. It maintains the best latency property. However, it uses `hostIP` and `hostPort` for communications between pods. In some k8s platform (such as DC/OS), `hostIP` and
> `hostPort` are not well supported. A _StatefulSet_ is only practical when deploying in a cloud environment or any K8S installation that has persistent volumes available. Also be aware, latency
> can be potentially higher when using persistent volumes, because there is usually built-in replication in the persistent volumes.

```bash
# deploy bookies in a daemon set
$ kubectl apply -f bookkeeper.yaml

# deploy bookies in a stateful set
$ kubectl apply -f bookkeeper.stateful.yaml
```

You can check on the status of the Bookie pods for these components either in the Kubernetes Dashboard or using `kubectl`:

```bash
$ kubectl get pods
```

While all BookKeeper pods is Running, by zk-shell you could find all available bookies under /ledgers/

You could also run a [bookkeeper tutorial](https://github.com/ivankelly/bookkeeper-tutorial/) instance, which named as 'dice' here, in this bookkeeper cluster.

```bash
$﻿kubectl run -i --tty --attach dice --image=caiok/bookkeeper-tutorial --env ZOOKEEPER_SERVERS="zk-0.zookeeper"
```

An example output of Dice instance is like this:
```aidl
➜ $ kubectl run -i --tty --attach dice --image=caiok/bookkeeper-tutorial --env ZOOKEEPER_SERVERS="zk-0.zookeeper"          
If you don't see a command prompt, try pressing enter.
Value = 1, epoch = 5, leading
Value = 2, epoch = 5, leading
Value = 1, epoch = 5, leading
Value = 4, epoch = 5, leading
Value = 5, epoch = 5, leading
Value = 4, epoch = 5, leading
Value = 3, epoch = 5, leading
Value = 5, epoch = 5, leading
Value = 3, epoch = 5, leading
Value = 2, epoch = 5, leading
Value = 1, epoch = 5, leading
Value = 4, epoch = 5, leading
Value = 2, epoch = 5, leading
```

### Un-Deploy

Delete Demo dice instance

```bash
$﻿kubectl delete deployment dice      
```

Delete BookKeeper
```bash
$ kubectl delete -f bookkeeper.yaml    
```

Delete ZooKeeper
```bash
$ kubectl delete -f zookeeper.yaml    
```

Delete cluster
```bash
$ gcloud container clusters delete bookkeeper-gke-cluster    
```



