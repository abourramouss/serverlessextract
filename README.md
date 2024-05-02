# Interferometry Workflows with Custom Lithops Runtime for Kubernetes

This repository provides guidance on configuring and utilizing a custom Docker runtime for executing interferometry workflows with Lithops using Kubernetes. The custom runtime allows for the inclusion of custom Python modules and system libraries necessary for interferometry data processing.

## Prerequisites

- Docker CE installed on your machine ([Installation Instructions](https://docs.docker.com/get-docker/))
- Access to a Kubernetes cluster
- A Docker Hub account or access to a private Docker registry
- A object storage backend installed in your cluster, in our case we use Minio, but any backend compatible with lithops should work (https://lithops-cloud.github.io/docs/source/storage_backends.html) 

## Configuration

Ensure your `~/.lithops/config` is properly configured to use Kubernetes as the backend and to specify your custom runtime:

```yaml
lithops:
    backend: k8s
    storage: minio
    log_level: INFO
    execution_timeout: 1800

k8s:
    docker_server: docker.io
    docker_user: <your_docker_username>
    docker_password: <your_docker_password>
    runtime: <your_docker_username>/custom_runtime:tag
    runtime_timeout: 1800

minio:
    storage_bucket: <your_storage_bucket>
    endpoint: <your_minio_endpoint>
    access_key_id: <your_access_key>
    secret_access_key: <your_secret_access_key>
```



## Building and Deploying Custom Runtime

To create and deploy your custom Lithops runtime for interferometry workflows, follow these steps:

### 1. Prepare Your Dockerfile

Locate the Dockerfile under the `docker/Dockerfile.k8s` directory. Customize this Dockerfile to include any necessary system packages or Python modules for your interferometry workflow.

### 2. Build the Docker Image
```
lithops runtime build -b k8s <your_docker_username>/custom_runtime:tag
```

### 3. Push the Docker Image to Docker Hub
```
lithops runtime deploy -b k8s <your_docker_username>/custom_runtime:tag

```

### 4. Use the Custom Runtime with Lithops

Specify the full Docker image name in the configuration or when creating the `FunctionExecutor` instance, or directly in the config file.


```

runtime: <your_docker_username>/custom_runtime:tag
```

```python
import lithops

fexec = lithops.FunctionExecutor(runtime='<your_docker_username>/custom_runtime:tag')
```

## Preparing the data to run the workflows.

The workflow needs measurement sets (an astronomy-specific format for datasets) for running the workflows.
We recommend using small datasets with a small number of functions for developing and testing.

The measurement sets can be found on https://share.obspm.fr/s/ezBfciEfmSs7Tqd?path=%2FDATA

But we provide a small measurement set for testing that can be found at https://www.dropbox.com/scl/fi/0x6vmv8g4fwuepagbayvd/partition_01.ms.zip?rlkey=fctrkvt3at81q36qlvyvwhic7&st=17p2w6ae&dl=0

This is an already partitioned measurement set that can be partitioned into smaller datasets if needed using the workflow.py file.

The needed parameters for running the workflows are already defined, but there are file parameters, which are provided https://www.dropbox.com/scl/fi/it5n0x1l3d0wtq3n84aed/05-02-2024-11-20-41_files_list.zip?rlkey=5tt2j99ef445o8nm6heaarfk3&st=zmfzioxx&dl=0

Modify the workflow.py parameters to point to the s3-like path where those file-like parameters can be found.
## Running Interferometry Workflows

Once the custom runtime is deployed and configured and the data is prepared, you can utilize the workflow.py to execute the workflow, the workflows to be executed are defined under that script.

As a note, an input and output to a step is always a S3-path pointing to a directory, where it expects to find the data (a measurement set or multiple measurement sets).



