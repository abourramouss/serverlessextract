# Interferometry Workflows with Custom Lithops Runtime for Kubernetes

This repository provides guidance on configuring and utilizing a custom Docker runtime for executing interferometry workflows with Lithops using Kubernetes. The custom runtime allows for the inclusion of custom Python modules and system libraries necessary for interferometry data processing.

## Prerequisites

- Docker CE installed on your machine ([Installation Instructions](https://docs.docker.com/get-docker/))
- Access to a Kubernetes cluster
- A Docker Hub account or access to a private Docker registry
- Kubectl installed in your system
- A object storage backend installed in your cluster, in our case we use OVH S3, but any backend compatible with lithops should work (https://lithops-cloud.github.io/docs/source/storage_backends.html) 

## Configuration
First we have to add the lithops configuration so lithops knows where to look at for the built runtime, it's very important to use <DOCKER_USERNAME>/<DOCKER_RUNTIME_NAME> in the runtime config key under the k8s section in the lithops config, we will use that name to build and deploy the runtime to docker and it will be used by the workers.

Ensure your `~/.lithops/config` is properly configured to use Kubernetes as the backend and to specify your custom runtime:

```yaml
lithops:
   backend: k8s
   storage: ceph
   monitoring_interval: 2
k8s:
   docker_server: docker.io
   docker_user: <DOCKER_USERNAME>
   docker_password: <DOCKER_TOKEN>
   runtime: <DOCKER_USERNAME>/<DOCKER_RUNTIME_NAME>
   runtime_timeout: 1800
ceph:
   bucket_name: os-10gb
   endpoint: https://s3.gra.perf.cloud.ovh.net/
   access_key_id: <OVH_ACCESS_KEY_ID>
   secret_access_key: <OVH_SECRET_ACCESS_KEY>
   region: gra

```

After configuring lithops, it's important to download the kubectl file from the ovh managed kubernetes cluster and download it. This downloaded file should be pasted into ~/.kube/config, so lithops knows we will be using that kubernetes cluster.

![alt text](image.png)


## Building and Deploying Custom Runtime

To create and deploy your custom Lithops runtime for interferometry workflows, follow these steps:

### 1. Prepare Your Dockerfile

Locate the Dockerfile under the `docker/Dockerfile.k8s` directory. This file already contains the docker runtime in order to run the radio interferometry workflows, building is required.

### 2. Build the Docker Image
```
lithops runtime build -b k8s <DOCKER_USERNAME>/<DOCKER_RUNTIME_NAME>
```

### 3. Push the Docker Image to Docker Hub (OPTIONAL, LITHOPS ALREADY DOES THIS THE FIRST TIME.)
```
lithops runtime deploy -b k8s -s ceph <DOCKER_USERNAME>/<DOCKER_RUNTIME_NAME>

```


## Preparing the data to run the workflows.

The workflow needs measurement sets (an astronomy-specific format for datasets) for running the workflows.
We recommend using small datasets with a small number of functions for developing and testing.

The measurement sets can be found on https://share.obspm.fr/s/ezBfciEfmSs7Tqd?path=%2FDATA

But we provide a small measurement set for testing that can be found at https://www.dropbox.com/scl/fi/0x6vmv8g4fwuepagbayvd/partition_01.ms.zip?rlkey=fctrkvt3at81q36qlvyvwhic7&st=17p2w6ae&dl=0

This is an already partitioned measurement set that can be partitioned into smaller datasets if needed using the workflow.py file.

The needed parameters for running the workflows are already defined, but there are file parameters, which are provided https://www.dropbox.com/scl/fi/it5n0x1l3d0wtq3n84aed/05-02-2024-11-20-41_files_list.zip?rlkey=5tt2j99ef445o8nm6heaarfk3&st=zmfzioxx&dl=0

Modify the workflow.py parameters to point to the s3-like path where those file-like parameters can be found.

## Installing the radio interferometry package

Install the radio interferometry package in developement mode by using:

```
python -m pip install -e .
```

NOTE: You have to be under the root directory of the project, at the radiointerferometry folder.
## Running Interferometry Workflows


Once the custom runtime is deployed and configured and the data is prepared, you can utilize the workflow.py located under radiointerferometry/examples to execute the workflow, the workflows to be executed are defined under that script.

As a note, an input and output to a step is always a S3-path pointing to a directory, where it expects to find the data (a measurement set or multiple measurement sets).



