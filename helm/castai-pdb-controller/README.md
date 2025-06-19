# CAST AI PDB Controller Helm Chart

A Helm chart for deploying the CAST AI Pod Disruption Budget (PDB) Controller to Kubernetes clusters.

## Overview

The CAST AI PDB Controller automatically creates, updates, and manages PodDisruptionBudgets for Deployments and StatefulSets in your Kubernetes cluster. It ensures high availability during cluster maintenance and node disruptions.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+
- Cluster admin permissions (for RBAC resources)

## Deployment

### Basic Installation

```bash
# Install the chart
helm install castai-pdb-controller ./helm/castai-pdb-controller
```

### Installation with Custom Values

```bash
# Install with custom namespace
helm install castai-pdb-controller ./helm/castai-pdb-controller \
  --set namespace=my-namespace

# Install with custom PDB configuration
helm install castai-pdb-controller ./helm/castai-pdb-controller \
  --set config.minAvailable="2" \
  --set config.fixPoorPDBs=true
```

### Using Custom Values File

Create a `custom-values.yaml`:

```yaml
replicaCount: 3
namespace: my-custom-namespace

config:
  minAvailable: "2"
  fixPoorPDBs: "true"
```

Install with custom values:
```bash
helm install castai-pdb-controller ./helm/castai-pdb-controller -f custom-values.yaml
```

## Configuration

### Values Reference

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of controller replicas | `2` |
| `image.repository` | Container image repository | `tylerwagner/castai-pdb-controller` |
| `image.tag` | Container image tag | `"1.32"` |
| `image.pullPolicy` | Container image pull policy | `IfNotPresent` |
| `nameOverride` | Override the chart name | `""` |
| `fullnameOverride` | Override the full app name | `""` |
| `serviceAccount.create` | Create a new service account | `true` |
| `serviceAccount.name` | Service account name | `"castai-pdb-controller"` |
| `serviceAccount.annotations` | Service account annotations | `{}` |
| `rbac.create` | Create RBAC resources | `true` |
| `config.minAvailable` | Default minAvailable for PDBs | `"1"` |
| `config.maxUnavailable` | Default maxUnavailable for PDBs | `""` (unset) |
| `config.fixPoorPDBs` | Automatically fix poor PDB configurations | `"false"` |
| `namespace` | Target namespace for deployment | `castai-agent` |

### PDB Configuration

The controller supports two PDB configuration modes:

#### MinAvailable Mode (Default)
```yaml
config:
  minAvailable: "1"  # Ensures at least 1 pod is always available
```

#### MaxUnavailable Mode
```yaml
config:
  maxUnavailable: "50%"  # Allows up to 50% of pods to be unavailable
```

**Note**: Use either `minAvailable` or `maxUnavailable`, not both.

## Monitoring

### Check Controller Status

```bash
# Check pods
kubectl get pods -n castai-agent -l app=castai-pdb-controller

# Check logs
kubectl logs deployment/castai-pdb-controller -n castai-agent

# Check created PDBs
kubectl get pdb -A | grep castai
```

### Check Controller Health

```bash
# Check deployment status
kubectl get deployment castai-pdb-controller -n castai-agent

# Check events
kubectl get events -n castai-agent --sort-by='.lastTimestamp'

# Check RBAC permissions
kubectl auth can-i create poddisruptionbudgets --as=system:serviceaccount:castai-agent:castai-pdb-controller
```

## Upgrading

```bash
# Update to a new version
helm upgrade castai-pdb-controller ./helm/castai-pdb-controller

# Upgrade with new values
helm upgrade castai-pdb-controller ./helm/castai-pdb-controller \
  --set image.tag="1.33" \
  --set config.minAvailable="2"
```

## Uninstalling

```bash
# Uninstall the chart
helm uninstall castai-pdb-controller

# Remove RBAC resources (if not managed by Helm)
kubectl delete clusterrole castai-pdb-controller
kubectl delete clusterrolebinding castai-pdb-controller
``` 