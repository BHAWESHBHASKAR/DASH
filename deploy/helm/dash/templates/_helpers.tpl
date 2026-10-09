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

{{- define "dash.controlPlaneSelectorLabels" -}}
{{ include "dash.selectorLabels" . }}
app.kubernetes.io/component: control-plane
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

{{- define "dash.controlPlaneServiceAccountName" -}}
{{- default (include "dash.componentName" (list . "control-plane")) .Values.serviceAccount.controlPlane.name -}}
{{- end -}}

{{/*
Image reference for a component: <registry>/<repository>-<service>:<tag>
Usage: include "dash.image" (list . "retrieval" "retrieval")
  arg 1 = root context, arg 2 = service name used in the image name,
  arg 3 = key under .Values.image holding per-service overrides.
*/}}
{{- define "dash.image" -}}
{{- $top := index . 0 -}}
{{- $svc := index . 1 -}}
{{- $key := index . 2 -}}
{{- $o := index $top.Values.image $key -}}
{{- $repo := default (printf "%s-%s" $top.Values.image.repository $svc) $o.repository -}}
{{- $tag := default $top.Values.image.tag $o.tag -}}
{{- printf "%s/%s:%s" $top.Values.image.registry $repo $tag -}}
{{- end -}}

{{- define "dash.retrievalImage" -}}{{ include "dash.image" (list . "retrieval" "retrieval") }}{{- end -}}
{{- define "dash.ingestionImage" -}}{{ include "dash.image" (list . "ingestion" "ingestion") }}{{- end -}}
{{- define "dash.controlPlaneImage" -}}{{ include "dash.image" (list . "control-plane" "controlPlane") }}{{- end -}}

{{/* Image pull secrets list. */}}
{{- define "dash.imagePullSecrets" -}}
{{- range .Values.image.pullSecrets }}
- name: {{ . }}
{{- end }}
{{- end -}}

{{/*
Validate a secret value. Fails the render when the value is missing, shorter
than 32 characters, or looks like a placeholder.
Usage: include "dash.secretValue" (list .Values.secret.retrieval.apiKey "secret.retrieval.apiKey")
*/}}
{{- define "dash.secretValue" -}}
{{- $name := index . 1 -}}
{{- $v := required (printf "%s is required (set it, or reference an existing Secret via secret.existingSecret.*)" $name) (index . 0) | toString -}}
{{- if lt (len $v) 32 -}}
{{- fail (printf "%s must be at least 32 characters (generate one with: openssl rand -hex 32)" $name) -}}
{{- end -}}
{{- $lower := lower $v -}}
{{- range (list "change-me" "changeme" "placeholder" "replace-me" "replace_me" "example" "sample") -}}
{{- if contains . $lower -}}
{{- fail (printf "%s looks like a placeholder value; supply a real random secret" $name) -}}
{{- end -}}
{{- end -}}
{{- if or (hasPrefix "<" $v) (hasSuffix ">" $v) -}}
{{- fail (printf "%s looks like a placeholder value; supply a real random secret" $name) -}}
{{- end -}}
{{- $v -}}
{{- end -}}

{{/* Names of the per-component Secrets (existing or chart-managed). */}}
{{- define "dash.retrievalSecretName" -}}
{{- default (printf "%s-retrieval-secrets" (include "dash.fullname" .)) .Values.secret.existingSecret.retrieval -}}
{{- end -}}
{{- define "dash.ingestionSecretName" -}}
{{- default (printf "%s-ingestion-secrets" (include "dash.fullname" .)) .Values.secret.existingSecret.ingestion -}}
{{- end -}}
{{- define "dash.controlPlaneSecretName" -}}
{{- default (printf "%s-control-plane-secrets" (include "dash.fullname" .)) .Values.secret.existingSecret.controlPlane -}}
{{- end -}}

{{/* Release namespace used by every namespaced resource. */}}
{{- define "dash.namespace" -}}
{{- default "dash-system" .Values.namespace.name -}}
{{- end -}}

{{/*
Init container that creates the data directories on a fresh PVC.
Usage: include "dash.initDataDirs" (list . "<image>" "<dir> <dir> ...")
*/}}
{{- define "dash.initDataDirs" -}}
{{- $top := index . 0 -}}
- name: init-data-dirs
  image: {{ index . 1 | quote }}
  imagePullPolicy: {{ $top.Values.image.pullPolicy }}
  command: ["/bin/sh", "-c", {{ printf "mkdir -p %s" (index . 2) | quote }}]
  resources:
    requests:
      cpu: 10m
      memory: 16Mi
    limits:
      cpu: 100m
      memory: 32Mi
  securityContext:
    {{- toYaml $top.Values.containerSecurityContext | nindent 4 }}
  volumeMounts:
  - name: data
    mountPath: {{ $top.Values.config.persistencePath }}
{{- end -}}

{{/* Merged resources for a component: base, then component overrides. */}}
{{- define "dash.resources" -}}
{{- $top := index . 0 -}}
{{- $key := index . 1 -}}
{{- $base := dict "requests" (deepCopy $top.Values.resources.requests) "limits" (deepCopy $top.Values.resources.limits) -}}
{{- $over := default (dict) (index $top.Values.resources $key) -}}
{{- toYaml (mergeOverwrite $base (deepCopy $over)) -}}
{{- end -}}
