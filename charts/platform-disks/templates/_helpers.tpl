{{- define "platformDisks.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "platformDisks.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "platformDisks.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" -}}
{{- end -}}

{{- define "platformDisks.labels.standard" -}}
app: {{ include "platformDisks.name" . }}
chart: {{ include "platformDisks.chart" . }}
heritage: {{ .Release.Service | quote }}
release: {{ .Release.Name | quote }}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "platformDisks.selectorLabels" -}}
app: {{ include "platformDisks.name" . }}
release: {{ .Release.Name }}
service: {{ include "platformDisks.name" . }}
{{- end }}

{{/*
Admission controller selector labels
*/}}
{{- define "platformDisks.admissionController.selectorLabels" -}}
app: {{ include "platformDisks.name" . }}
release: {{ .Release.Name }}
service: {{ include "platformDisks.name" . }}-admission-controller
{{- end }}

{{- define "platformDisks.kubeAuthMountRoot" -}}
{{- printf "/var/run/secrets/kubernetes.io/serviceaccount" -}}
{{- end -}}

{{- define "platformDisks.admissionControllerCertMountRoot" -}}
{{- printf "/var/run/secrets/admission-controller/cert" -}}
{{- end -}}

{{- define "platformDisks.env" -}}
- name: NP_DISK_API_PLATFORM_AUTH_URL
  value: {{ .Values.platform.authUrl | quote }}
- name: NP_DISK_API_PLATFORM_AUTH_TOKEN
{{- if .Values.platform.token }}
{{ toYaml .Values.platform.token | indent 2 }}
{{- end }}
- name: NP_REGISTRY_EVENTS_URL
  value: {{ .Values.platform.eventsUrl }}
- name: NP_REGISTRY_EVENTS_TOKEN
{{- if .Values.platform.token }}
{{- toYaml .Values.platform.token | nindent 2 }}
{{- end }}
- name: NP_DISK_API_K8S_API_URL
  value: https://kubernetes.default:443
- name: NP_DISK_API_K8S_AUTH_TYPE
  value: token
- name: NP_DISK_API_K8S_CA_PATH
  value: {{ include "platformDisks.kubeAuthMountRoot" . }}/ca.crt
- name: NP_DISK_API_K8S_TOKEN_PATH
  value: {{ include "platformDisks.kubeAuthMountRoot" . }}/token
- name: NP_DISK_API_K8S_NS
  value: {{ .Values.disks.namespace | default "default" | quote }}
{{- if .Values.disks.storageClassName }}
- name: NP_DISK_API_K8S_STORAGE_CLASS
  value: {{ .Values.disks.storageClassName }}
{{- end }}
- name: NP_DISK_API_STORAGE_LIMIT_PER_PROJECT
  value: {{ .Values.disks.limitPerProject | quote }}
- name: NP_DISK_API_ENABLE_DOCS
  value: {{ .Values.docs.enabled | quote }}
- name: NP_CLUSTER_NAME
  value: {{ .Values.platform.clusterName | quote }}
{{- if .Values.cors.origins }}
- name: NP_CORS_ORIGINS
  value: {{ join "," .Values.cors.origins | quote }}
{{- end }}
{{- if .Values.sentry }}
- name: SENTRY_DSN
  value: {{ .Values.sentry.dsn }}
- name: SENTRY_CLUSTER_NAME
  value: {{ .Values.sentry.clusterName }}
- name: SENTRY_APP_NAME
  value: platform-disks
- name: SENTRY_SAMPLE_RATE
  value: {{ .Values.sentry.sampleRate | default 0 | quote }}
{{- end }}
{{- end -}}
