{{/*
Standard helper templates for the DASH chart.
Include from a template with:
  {{- include "dash.fullname" . -}}
or, when an extra suffix is needed:
  {{- include "dash.fullname" (list . "retrieval") -}}
*/}}

{{/* Chart name (allows override via .Values.nameOverride). */}}
{{- define "dash.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified app name. Strips the chart suffix from the
release name so a release called "dash" produces "dash" rather
than "dash-dash".
*/}}
{{- define "dash.fullname" -}}
{{- $ctx := . -}}
{{- if $ctx.Values.fullnameOverride -}}
{{- $ctx.Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default $ctx.Chart.Name $ctx.Values.nameOverride -}}
{{- if contains $name $ctx.Release.Name -}}
{{- $ctx.Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" $ctx.Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Allow callers to append a component suffix safely. */}}
{{- define "dash.componentName" -}}
{{- $top := index . 0 -}}
{{- $component := index . 1 -}}
{{- printf "%s-%s" (include "dash.fullname" $top) $component | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Chart label block (mandatory recommended labels). */}}
{{- define "dash.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "dash.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: dash
{{- end -}}

{{/* Selector-stable label subset (must NOT include version or chart). */}}
{{- define "dash.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dash.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Per-component selector labels. */}}
{{- define "dash.retrievalSelectorLabels" -}}
{{ include "dash.selectorLabels" . }}
app.kubernetes.io/component: retrieval
{{- end -}}

{{- define "dash.ingestionSelectorLabels" -}}
{{ include "dash.selectorLabels" . }}
app.kubernetes.io/component: ingestion
{{- end -}}

{{/* ServiceAccount name for a component. Caller-provided
   .Values.serviceAccount.<component>.name wins; otherwise the
   chart-assigned name is used (the corresponding SA is only
   rendered when .Values.serviceAccount.create is true). */}}
{{- define "dash.retrievalServiceAccountName" -}}
{{- default (include "dash.componentName" (list . "retrieval")) .Values.serviceAccount.retrieval.name -}}
{{- end -}}

{{- define "dash.ingestionServiceAccountName" -}}
{{- default (include "dash.componentName" (list . "ingestion")) .Values.serviceAccount.ingestion.name -}}
{{- end -}}

{{/* Image reference for a component. */}}
{{- define "dash.retrievalImage" -}}
{{- $repo := .Values.image.retrieval.repository | default (printf "%s/%s" .Values.image.repository "retrieval") -}}
{{- $tag := .Values.image.retrieval.tag | default .Values.image.tag -}}
{{- printf "%s/%s:%s" .Values.image.registry $repo $tag -}}
{{- end -}}

{{- define "dash.ingestionImage" -}}
{{- $repo := .Values.image.ingestion.repository | default (printf "%s/%s" .Values.image.repository "ingestion") -}}
{{- $tag := .Values.image.ingestion.tag | default .Values.image.tag -}}
{{- printf "%s/%s:%s" .Values.image.registry $repo $tag -}}
{{- end -}}

{{/* Image pull secrets list (deduplicated). */}}
{{- define "dash.imagePullSecrets" -}}
{{- range .Values.image.pullSecrets }}
- name: {{ . }}
{{- end }}
{{- end -}}
