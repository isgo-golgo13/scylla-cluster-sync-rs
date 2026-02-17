# TUI Dashboard CLI - Kubernetes Connectivity Guide

## Overview

The `tui-dash` CLI runs locally (outside Kubernetes) and connects to the `sstable-loader` service running inside a Kubernetes cluster. This document describes the production-grade approach using the **Kubernetes Gateway API** with **Envoy Gateway** to expose the `/status` endpoint securely over TLS.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              KUBERNETES CLUSTER                             │
│                                                                             │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐         │
│  │  sstable-loader │    │  sstable-loader │    │  sstable-loader │         │
│  │    (pod)        │    │    (pod)        │    │    (pod)        │         │
│  │   :9092         │    │   :9092         │    │   :9092         │         │
│  └────────┬────────┘    └────────┬────────┘    └────────┬────────┘         │
│           │                      │                      │                   │
│           └──────────────────────┼──────────────────────┘                   │
│                                  │                                          │
│                      ┌───────────▼───────────┐                              │
│                      │   Service             │                              │
│                      │   sstable-loader      │                              │
│                      │   ClusterIP :9092     │                              │
│                      └───────────┬───────────┘                              │
│                                  │                                          │
│                      ┌───────────▼───────────┐                              │
│                      │   HTTPRoute           │                              │
│                      │   sstable-loader-api  │                              │
│                      │   /status, /health    │                              │
│                      └───────────┬───────────┘                              │
│                                  │                                          │
│                      ┌───────────▼───────────┐                              │
│                      │   Gateway             │                              │
│                      │   scylla-sync-gateway │                              │
│                      │   :443 (TLS)          │                              │
│                      └───────────┬───────────┘                              │
│                                  │                                          │
│                      ┌───────────▼───────────┐                              │
│                      │   Envoy Gateway       │                              │
│                      │   (LoadBalancer)      │                              │
│                      │   External IP         │                              │
│                      └───────────┬───────────┘                              │
│                                  │                                          │
└──────────────────────────────────┼──────────────────────────────────────────┘
                                   │
                                   │ HTTPS :443
                                   │
                      ┌────────────▼────────────┐
                      │   tui-dash CLI          │
                      │   (local workstation)   │
                      │                         │
                      │   make tui-live \       │
                      │     API_URL=https://... │
                      └─────────────────────────┘
```

## Why Kubernetes Gateway API

| Approach | Production Grade | Why/Why Not |
|----------|------------------|-------------|
| `kubectl port-forward` | No | Requires kubectl access, not persistent, debugging only |
| `NodePort` Service | No | Exposes arbitrary ports, no TLS termination, no routing |
| Ingress (nginx/traefik) | Partial | Legacy API, limited features, controller-specific annotations |
| **Gateway API** | **Yes** | Kubernetes-native, role-oriented, TLS-native, portable |


## Quick-Dirty Connect (Not Production-Grade)
As a crack the glass connectivity test to connect the `tui-dash

```shell
kubectl port-forward svc/scylla-cluster-sync-sstable-loader 9092:9092 -n scylla-sync & make tui-dash
```

1. kubectl port-forward tunnels localhost:9092 → sstable-loader pod in cluster
2. make tui-dash connects to http://localhost:9092 (default)
3. tui-dash polls /status endpoint, displays live migration stats1.

DO NOT recommend use of this style for production, even for non-production.



## Prerequisites

### 1. Install Envoy Gateway

```bash
# Install Gateway API CRDs
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.0/standard-install.yaml

# Install Envoy Gateway
helm install envoy-gateway oci://docker.io/envoyproxy/gateway-helm \
  --version v1.2.0 \
  --namespace envoy-gateway-system \
  --create-namespace
```

### 2. Verify Installation

```bash
kubectl wait --for=condition=available deployment/envoy-gateway \
  -n envoy-gateway-system --timeout=120s
```

## Helm Chart Structure (Screaming Architecture)

```
helm/scylla-cluster-sync/
├── Chart.yaml
├── values.yaml
├── templates/
│   ├── _helpers.tpl
│   │
│   ├── # Dual-Writer Domain
│   ├── dual-writer-deployment.yaml
│   ├── dual-writer-service.yaml
│   ├── dual-writer-configmap.yaml
│   │
│   ├── # Dual-Reader Domain
│   ├── dual-reader-deployment.yaml
│   ├── dual-reader-service.yaml
│   ├── dual-reader-configmap.yaml
│   │
│   ├── # SSTable-Loader Domain
│   ├── sstable-loader-deployment.yaml
│   ├── sstable-loader-service.yaml
│   ├── sstable-loader-configmap.yaml
│   │
│   ├── # Gateway API Domain (NEW)
│   ├── gateway-api-gateway.yaml
│   ├── gateway-api-httproute-sstable-loader.yaml
│   ├── gateway-api-tls-certificate.yaml
│   └── gateway-api-reference-grant.yaml
```

## Gateway API Resources

### 1. Gateway (gateway-api-gateway.yaml)

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: scylla-sync-gateway
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "scylla-cluster-sync.labels" . | nindent 4 }}
spec:
  gatewayClassName: envoy
  listeners:
    - name: https
      protocol: HTTPS
      port: 443
      hostname: "{{ .Values.gateway.hostname }}"
      tls:
        mode: Terminate
        certificateRefs:
          - kind: Secret
            name: scylla-sync-tls
            namespace: {{ .Release.Namespace }}
      allowedRoutes:
        namespaces:
          from: Same
        kinds:
          - kind: HTTPRoute
```

### 2. HTTPRoute for SSTable-Loader (gateway-api-httproute-sstable-loader.yaml)

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: sstable-loader-api
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "scylla-cluster-sync.labels" . | nindent 4 }}
spec:
  parentRefs:
    - name: scylla-sync-gateway
      namespace: {{ .Release.Namespace }}
      sectionName: https
  hostnames:
    - "{{ .Values.gateway.hostname }}"
  rules:
    # Health and Status endpoints (read-only, safe to expose)
    - matches:
        - path:
            type: Exact
            value: /status
        - path:
            type: Exact
            value: /health
      backendRefs:
        - name: sstable-loader
          port: 9092
          weight: 1
      timeouts:
        request: 10s
        backendRequest: 5s
    
    # Migration control endpoints (protected)
    - matches:
        - path:
            type: Exact
            value: /start
          method: POST
        - path:
            type: Exact
            value: /stop
          method: POST
      backendRefs:
        - name: sstable-loader
          port: 9092
          weight: 1
      filters:
        - type: RequestHeaderModifier
          requestHeaderModifier:
            add:
              - name: X-Gateway-Auth
                value: "{{ .Values.gateway.authToken }}"
```

### 3. TLS Certificate (gateway-api-tls-certificate.yaml)

Option A: Using cert-manager (recommended)

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: scylla-sync-tls
  namespace: {{ .Release.Namespace }}
spec:
  secretName: scylla-sync-tls
  issuerRef:
    name: {{ .Values.gateway.tls.issuerName }}
    kind: {{ .Values.gateway.tls.issuerKind | default "ClusterIssuer" }}
  dnsNames:
    - "{{ .Values.gateway.hostname }}"
  duration: 2160h    # 90 days
  renewBefore: 360h  # 15 days
```

Option B: Pre-provisioned Secret (for existing certificates)

```yaml
{{- if .Values.gateway.tls.existingSecret }}
# Using existing secret: {{ .Values.gateway.tls.existingSecret }}
{{- else }}
apiVersion: v1
kind: Secret
metadata:
  name: scylla-sync-tls
  namespace: {{ .Release.Namespace }}
type: kubernetes.io/tls
data:
  tls.crt: {{ .Values.gateway.tls.crt | b64enc }}
  tls.key: {{ .Values.gateway.tls.key | b64enc }}
{{- end }}
```

### 4. ReferenceGrant (gateway-api-reference-grant.yaml)

Required if Gateway and Services are in different namespaces:

```yaml
apiVersion: gateway.networking.k8s.io/v1beta1
kind: ReferenceGrant
metadata:
  name: allow-gateway-to-sstable-loader
  namespace: {{ .Release.Namespace }}
spec:
  from:
    - group: gateway.networking.k8s.io
      kind: HTTPRoute
      namespace: {{ .Release.Namespace }}
  to:
    - group: ""
      kind: Service
      name: sstable-loader
```

## Values Configuration

Add to `values.yaml`:

```yaml
# Gateway API Configuration
gateway:
  enabled: true
  hostname: "sstable-loader.example.com"
  authToken: ""  # Set via --set or external secret
  
  tls:
    # Option 1: Use cert-manager
    issuerName: "letsencrypt-prod"
    issuerKind: "ClusterIssuer"
    
    # Option 2: Use existing secret
    # existingSecret: "my-tls-secret"
    
    # Option 3: Provide cert/key directly (not recommended for production)
    # crt: |
    #   -----BEGIN CERTIFICATE-----
    #   ...
    # key: |
    #   -----BEGIN PRIVATE KEY-----
    #   ...
```

## Deployment

### 1. Deploy with Gateway API enabled

```bash
helm upgrade --install scylla-cluster-sync ./helm/scylla-cluster-sync \
  --namespace scylla-sync \
  --create-namespace \
  --set gateway.enabled=true \
  --set gateway.hostname=sstable-loader.prod.example.com \
  --set gateway.tls.issuerName=letsencrypt-prod
```

### 2. Get External IP

```bash
kubectl get gateway scylla-sync-gateway -n scylla-sync -o jsonpath='{.status.addresses[0].value}'
```

### 3. Configure DNS

Point your hostname to the Gateway's external IP:

```
sstable-loader.prod.example.com  A  <EXTERNAL_IP>
```

Or for testing, add to `/etc/hosts`:

```
<EXTERNAL_IP>  sstable-loader.prod.example.com
```

### 4. Verify Connectivity

```bash
curl -s https://sstable-loader.prod.example.com/health
```

Expected response:

```json
{"status": "healthy"}
```

## TUI Dashboard Usage

### Local Workstation

```bash
# Using make target
make tui-live API_URL=https://sstable-loader.prod.example.com

# Or directly
cargo run --bin tui-dash -- --api-url https://sstable-loader.prod.example.com
```

### With Custom Refresh Rate

```bash
cargo run --bin tui-dash -- \
  --api-url https://sstable-loader.prod.example.com \
  --refresh-ms 500
```

## Security Considerations

### 1. Endpoint Exposure

| Endpoint | Method | Exposure | Risk Level |
|----------|--------|----------|------------|
| `/health` | GET | Safe | None |
| `/status` | GET | Safe | Low (read-only stats) |
| `/start` | POST | Controlled | Medium (starts migration) |
| `/stop` | POST | Controlled | Medium (stops migration) |
| `/migrate/:ks/:table` | POST | Controlled | Medium |
| `/discover/:keyspace` | GET | Controlled | Low |

### 2. Recommended: Add Authentication

For production, add authentication via Envoy Gateway's SecurityPolicy:

```yaml
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: SecurityPolicy
metadata:
  name: sstable-loader-auth
  namespace: scylla-sync
spec:
  targetRefs:
    - group: gateway.networking.k8s.io
      kind: HTTPRoute
      name: sstable-loader-api
  basicAuth:
    users:
      name: sstable-loader-users
---
apiVersion: v1
kind: Secret
metadata:
  name: sstable-loader-users
  namespace: scylla-sync
type: Opaque
data:
  .htpasswd: <base64-encoded-htpasswd>
```

Generate htpasswd:

```bash
htpasswd -nbs admin <password> | base64
```

### 3. Network Policy (Optional)

Restrict traffic to Gateway only:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: sstable-loader-gateway-only
  namespace: scylla-sync
spec:
  podSelector:
    matchLabels:
      app: sstable-loader
  policyTypes:
    - Ingress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: envoy-gateway-system
      ports:
        - protocol: TCP
          port: 9092
```

## Fix Worklows (Verifications)

### Gateway Not Getting External IP

```bash
# Check Gateway status
kubectl describe gateway scylla-sync-gateway -n scylla-sync

# Check Envoy Gateway logs
kubectl logs -n envoy-gateway-system deployment/envoy-gateway
```

### HTTPRoute Not Attached

```bash
# Check HTTPRoute status
kubectl describe httproute sstable-loader-api -n scylla-sync

# Verify parentRef matches Gateway
kubectl get gateway -n scylla-sync -o yaml
```

### TLS Certificate Issues

```bash
# Check certificate status (cert-manager)
kubectl describe certificate scylla-sync-tls -n scylla-sync

# Check secret exists
kubectl get secret scylla-sync-tls -n scylla-sync

# Verify certificate validity
kubectl get secret scylla-sync-tls -n scylla-sync -o jsonpath='{.data.tls\.crt}' | base64 -d | openssl x509 -text -noout
```

### Connection Refused from tui-dash

```bash
# Test from within cluster first
kubectl run test-curl --rm -it --image=curlimages/curl -- \
  curl -s http://sstable-loader.scylla-sync.svc.cluster.local:9092/health

# Check sstable-loader pods
kubectl get pods -n scylla-sync -l app=sstable-loader

# Check sstable-loader logs
kubectl logs -n scylla-sync -l app=sstable-loader --tail=50
```

## Cloud-Specific Notes

### AWS EKS

Envoy Gateway creates an AWS Network Load Balancer by default. For Application Load Balancer:

```yaml
# Add to Gateway metadata
metadata:
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-type: "external"
    service.beta.kubernetes.io/aws-load-balancer-scheme: "internet-facing"
    service.beta.kubernetes.io/aws-load-balancer-nlb-target-type: "ip"
```

### GCP GKE

Envoy Gateway works with GKE's default load balancer. For internal-only access:

```yaml
metadata:
  annotations:
    networking.gke.io/load-balancer-type: "Internal"
```

### Azure AKS

```yaml
metadata:
  annotations:
    service.beta.kubernetes.io/azure-load-balancer-internal: "true"
```

## Summary

| Component | Resource | Purpose |
|-----------|----------|---------|
| Envoy Gateway | GatewayClass | Controller implementation |
| Gateway | Gateway CR | TLS termination, external IP |
| HTTPRoute | HTTPRoute CR | Path-based routing to sstable-loader |
| Certificate | Secret/Certificate | TLS certificate for HTTPS |
| tui-dash | Local CLI | Connects via HTTPS to Gateway |

This setup provides:

- Production-grade external access
- TLS encryption in transit
- Kubernetes-native resource management
- No kubectl access required for monitoring
- Portable across cloud providers