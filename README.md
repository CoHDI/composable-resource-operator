# composable-resource-operator

[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/cohdi)](https://artifacthub.io/packages/search?repo=cohdi)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/CoHDI/composable-resource-operator/badge)](https://scorecard.dev/viewer/?uri=github.com/CoHDI/composable-resource-operator)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/12016/badge)](https://www.bestpractices.dev/projects/12016)

A Kubernetes operator that dynamically attaches and detaches composable hardware resources—such as GPUs and CXL memory—to cluster nodes using the CDI (Composable Device Infrastructure) API.

## Description

Modern data centres increasingly rely on *composable infrastructure*: hardware resources (GPUs, CXL-attached memory, FPGAs) that can be assigned to workloads on demand rather than being permanently bound to a single host. Kubernetes, however, has no built-in concept of composable hardware—resources are statically allocated per node at boot time.

The `composable-resource-operator` bridges that gap. It watches for `ComposabilityRequest` objects created by cluster users or higher-level orchestrators and drives the CDI API to attach or detach the requested device to the target Kubernetes node—without rebooting the node or restarting workloads. When the request is fulfilled the operator updates the internal `ComposableResource` object to reflect the new state of that device and exposes it back through the node's extended resources.

**Supported resource types**

| Type | Description |
|------|-------------|
| `gpu` | NVIDIA GPU cards exposed via the CDI composable fabric |
| `cxl` | CXL-attached memory modules |

## Architecture

The operator exposes two CRDs:

### `ComposabilityRequest` (user-facing)

Created by end users or automation to request that a hardware resource be attached to (or detached from) a node. The spec includes the resource type, size, model, target node, and allocation policy.

### `ComposableResource` (internal)

Tracks the lifecycle of an individual composable device. Managed exclusively by the operator; users should treat this as read-only.

### Reconciliation flow

```
User creates ComposabilityRequest
        │
        ▼
ComposabilityRequest controller
  ├── Finds a suitable ComposableResource (or creates one)
  ├── Calls CDI API → attach/detach device on target node
  ├── Updates node extended resources via Node status patch
  └── Reflects outcome in ComposabilityRequest.Status
```

### Quick example

```yaml
apiVersion: cro.hpsys.ibm.ie.com/v1alpha1
kind: ComposabilityRequest
metadata:
  name: composabilityrequest-sample
spec:
  resource:
    type: "gpu"
    size: 2
    model: "NVIDIA-A100-PCIE-40GB"
    other_spec:
      milli_cpu: 2
      memory: 40
      ephemeral_storage: 2
      allowed_pod_number: 5
    target_node: "node1"
    force_detach: true
    allocation_policy: "samenode"
```

## Getting Started

### Prerequisites
- go version v1.24.0+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### To Deploy on the cluster
**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/composable-resource-operator:tag
```

**NOTE:** This image ought to be published in the personal registry you specified.
And it is required to have access to pull the image from the working environment.
Make sure you have the proper permission to the registry if the above commands don’t work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/composable-resource-operator:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Project Distribution

Following are the steps to build the installer and distribute this project to users.

1. Build the installer for the image built and published in the registry:

```sh
make build-installer IMG=<some-registry>/composable-resource-operator:tag
```

NOTE: The makefile target mentioned above generates an 'install.yaml'
file in the dist directory. This file contains all the resources built
with Kustomize, which are necessary to install this project without
its dependencies.

2. Using the installer

Users can just run kubectl apply -f <URL for YAML BUNDLE> to install the project, i.e.:

```sh
kubectl apply -f https://raw.githubusercontent.com/<org>/composable-resource-operator/<tag or branch>/dist/install.yaml
```

## Contributing

We welcome contributions! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on
how to submit bug reports, propose features, and open pull requests.

All contributions require a [DCO](DCO) sign-off (`git commit -s`). By signing off you certify
that you wrote or otherwise have the right to contribute the code under the project's open-source
license.

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
