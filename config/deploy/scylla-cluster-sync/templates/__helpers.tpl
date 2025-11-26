{{/*
Expand the name of the chart.
*/}}
{{- define "scylla-cluster-sync.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "scylla-cluster-sync.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "scylla-cluster-sync.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "scylla-cluster-sync.labels" -}}
helm.sh/chart: {{ include "scylla-cluster-sync.chart" . }}
{{ include "scylla-cluster-sync.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.global.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "scylla-cluster-sync.selectorLabels" -}}
app.kubernetes.io/name: {{ include "scylla-cluster-sync.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service-specific labels for dual-writer
*/}}
{{- define "scylla-cluster-sync.dualWriter.labels" -}}
{{ include "scylla-cluster-sync.labels" . }}
app.kubernetes.io/component: dual-writer
{{- end }}

{{- define "scylla-cluster-sync.dualWriter.selectorLabels" -}}
{{ include "scylla-cluster-sync.selectorLabels" . }}
app.kubernetes.io/component: dual-writer
{{- end }}

{{/*
Service-specific labels for dual-reader
*/}}
{{- define "scylla-cluster-sync.dualReader.labels" -}}
{{ include "scylla-cluster-sync.labels" . }}
app.kubernetes.io/component: dual-reader
{{- end }}

{{- define "scylla-cluster-sync.dualReader.selectorLabels" -}}
{{ include "scylla-cluster-sync.selectorLabels" . }}
app.kubernetes.io/component: dual-reader
{{- end }}

{{/*
Service-specific labels for sstable-loader
*/}}
{{- define "scylla-cluster-sync.sstableLoader.labels" -}}
{{ include "scylla-cluster-sync.labels" . }}
app.kubernetes.io/component: sstable-loader
{{- end }}

{{- define "scylla-cluster-sync.sstableLoader.selectorLabels" -}}
{{ include "scylla-cluster-sync.selectorLabels" . }}
app.kubernetes.io/component: sstable-loader
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "scylla-cluster-sync.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "scylla-cluster-sync.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Return the proper image name for a service
*/}}
{{- define "scylla-cluster-sync.image" -}}
{{- $registry := .global.imageRegistry -}}
{{- $repository := .service.image.repository -}}
{{- $tag := .service.image.tag | default .Chart.AppVersion -}}
{{- printf "%s/%s:%s" $registry $repository $tag -}}
{{- end }}

{{/*
Return the proper image pull policy
*/}}
{{- define "scylla-cluster-sync.imagePullPolicy" -}}
{{- $policy := .service.image.pullPolicy | default .global.imagePullPolicy -}}
{{- $policy -}}
{{- end }}

{{/*
Return image pull secrets
*/}}
{{- define "scylla-cluster-sync.imagePullSecrets" -}}
{{- with .Values.global.imagePullSecrets }}
imagePullSecrets:
{{- range . }}
  - name: {{ . }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create config checksum annotation
*/}}
{{- define "scylla-cluster-sync.configChecksum" -}}
checksum/config: {{ include (print $.Template.BasePath "/app-config.yaml") . | sha256sum }}
{{- end }}

{{/*
Create secret checksum annotation
*/}}
{{- define "scylla-cluster-sync.secretChecksum" -}}
{{- if not .Values.externalSecrets.enabled }}
checksum/secret: {{ include (print $.Template.BasePath "/app-secrets.yaml") . | sha256sum }}
{{- end }}
{{- end }}

{{/*
Return the secret name for database credentials
*/}}
{{- define "scylla-cluster-sync.secretName" -}}
{{- printf "%s-database-credentials" (include "scylla-cluster-sync.fullname" .) }}
{{- end }}

{{/*
Common environment variables for all services
*/}}
{{- define "scylla-cluster-sync.commonEnv" -}}
- name: RUST_LOG
  value: "info"
- name: RUST_BACKTRACE
  value: "1"
{{- with .Values.extraEnv }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Database environment variables
*/}}
{{- define "scylla-cluster-sync.databaseEnv" -}}
- name: SOURCE_DB_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ include "scylla-cluster-sync.secretName" . }}
      key: source-username
      optional: true
- name: SOURCE_DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "scylla-cluster-sync.secretName" . }}
      key: source-password
      optional: true
- name: TARGET_DB_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ include "scylla-cluster-sync.secretName" . }}
      key: target-username
      optional: true
- name: TARGET_DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "scylla-cluster-sync.secretName" . }}
      key: target-password
      optional: true
{{- end }}