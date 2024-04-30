# Interferometry Workflows with Custom Lithops Runtime for Kubernetes

This repository provides guidance on configuring and utilizing a custom Docker runtime for executing interferometry workflows with Lithops using Kubernetes. The custom runtime allows for the inclusion of custom Python modules and system libraries necessary for interferometry data processing.

## Prerequisites

- Docker CE installed on your machine ([Installation Instructions](https://docs.docker.com/get-docker/))
- Access to a Kubernetes cluster
- A Docker Hub account or access to a private Docker registry

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

## Running Interferometry Workflows

Once the custom runtime is deployed and configured, you can utilize the pipeline.py to execute the workflows

## Minio

## Data to use for the demo