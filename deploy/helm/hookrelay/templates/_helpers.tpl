{{/*
Names and labels.
*/}}
{{- define "hookrelay.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "hookrelay.fullname" -}}
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

{{- define "hookrelay.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "hookrelay.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hookrelay.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "hookrelay.labels" -}}
helm.sh/chart: {{ include "hookrelay.chart" . }}
{{ include "hookrelay.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: hookrelay
{{- end }}

{{/* Labels for one role: (dict "ctx" $ "component" "api") */}}
{{- define "hookrelay.componentLabels" -}}
{{ include "hookrelay.labels" .ctx }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "hookrelay.componentSelectorLabels" -}}
{{ include "hookrelay.selectorLabels" .ctx }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "hookrelay.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "hookrelay.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "hookrelay.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- if .Values.image.digest -}}
{{ .Values.image.repository }}@{{ .Values.image.digest }}
{{- else -}}
{{ .Values.image.repository }}:{{ $tag }}
{{- end -}}
{{- end }}

{{/* Secret the pods read with envFrom (the user's, or the chart-managed dev/CI one). */}}
{{- define "hookrelay.secretName" -}}
{{- if .Values.secrets.existingSecret }}
{{- .Values.secrets.existingSecret }}
{{- else }}
{{- printf "%s-secrets" (include "hookrelay.fullname" .) }}
{{- end }}
{{- end }}

{{- define "hookrelay.configMapName" -}}
{{- printf "%s-config" (include "hookrelay.fullname" .) }}
{{- end }}

{{/*
Guard rails, evaluated by every template that includes "hookrelay.validate".
- no secret-looking keys in the ConfigMap;
- role-owned settings only through their role values;
- shutdown budgets: terminationGracePeriodSeconds must cover preStop + graceful shutdown (API)
  and the drain + completion flush + lease release (worker);
- DB_PGBOUNCER=true needs DATABASE_DIRECT_URL for migrations when the chart owns the Secret.
*/}}
{{- define "hookrelay.secretKeys" -}}
DATABASE_URL DATABASE_DIRECT_URL REDIS_URL OPERATOR_TOKEN SECRETS_ENCRYPTION_KEY SECRETS_ENCRYPTION_KEY_PREVIOUS METRICS_TOKEN
{{- end }}

{{- define "hookrelay.roleOwnedKeys" -}}
PORT HOST METRICS_PORT API_GRACEFUL_SHUTDOWN_S WORKER_DRAIN_TIMEOUT_S WORKER_CONCURRENCY RELAY_HEARTBEAT_FILE RELAY_HEARTBEAT_MAX_AGE_S WORKER_HEARTBEAT_FILE WORKER_HEARTBEAT_MAX_AGE_S WORKER_ID
{{- end }}

{{- define "hookrelay.validate" -}}
{{- $secretKeys := splitList " " (include "hookrelay.secretKeys" .) }}
{{- $roleKeys := splitList " " (include "hookrelay.roleOwnedKeys" .) }}
{{- range $k, $v := .Values.config }}
{{- if has (upper $k) $secretKeys }}
{{- fail (printf "config.%s is a secret: put it in the Secret (secrets.existingSecret or secrets.values), never in the ConfigMap" $k) }}
{{- end }}
{{- if regexMatch "(?i)(PASSWORD|_TOKEN$|_SECRET$|_KEY$|_KEY_PREVIOUS$)" $k }}
{{- fail (printf "config.%s looks like a credential: put it in the Secret, not the ConfigMap" $k) }}
{{- end }}
{{- if has (upper $k) $roleKeys }}
{{- fail (printf "config.%s is set by the chart from api/relay/worker values; set it there instead" $k) }}
{{- end }}
{{- if not (kindIs "string" $v) }}
{{- fail (printf "config.%s must be a string (quote it): got %v" $k $v) }}
{{- end }}
{{- end }}
{{- range $k, $v := .Values.secrets.values }}
{{- if not (has $k $secretKeys) }}
{{- fail (printf "secrets.values.%s is not a known secret (%s); put plain settings under config" $k (join ", " $secretKeys)) }}
{{- end }}
{{- end }}
{{- if and (not .Values.secrets.existingSecret) (not .Values.secrets.values.DATABASE_URL) }}
{{- fail "set secrets.existingSecret (production) or secrets.values.DATABASE_URL (dev/CI)" }}
{{- end }}
{{- if and (eq (toString (default "false" .Values.config.DB_PGBOUNCER)) "true") (not .Values.secrets.existingSecret) (not .Values.secrets.values.DATABASE_DIRECT_URL) }}
{{- fail "config.DB_PGBOUNCER=true: migrations must bypass the pooler, set secrets.values.DATABASE_DIRECT_URL" }}
{{- end }}
{{- $api := .Values.api }}
{{- if le (int $api.terminationGracePeriodSeconds) (add (int $api.preStopSleepSeconds) (int $api.gracefulShutdownSeconds)) }}
{{- fail (printf "api.terminationGracePeriodSeconds (%d) must exceed preStopSleepSeconds + gracefulShutdownSeconds (%d)" (int $api.terminationGracePeriodSeconds) (add (int $api.preStopSleepSeconds) (int $api.gracefulShutdownSeconds))) }}
{{- end }}
{{- $w := .Values.worker }}
{{- if lt (int $w.terminationGracePeriodSeconds) (add (int $w.drainTimeoutSeconds) 10) }}
{{- fail (printf "worker.terminationGracePeriodSeconds (%d) must be at least drainTimeoutSeconds + 10 (%d): drain, completion flush and lease release must finish before SIGKILL" (int $w.terminationGracePeriodSeconds) (add (int $w.drainTimeoutSeconds) 10)) }}
{{- end }}
{{- if and $w.autoscaling.enabled (gt (int $w.autoscaling.minReplicaCount) (int $w.autoscaling.maxReplicaCount)) }}
{{- fail "worker.autoscaling.minReplicaCount must not exceed maxReplicaCount" }}
{{- end }}
{{- if and $api.autoscaling.enabled (gt (int $api.autoscaling.minReplicas) (int $api.autoscaling.maxReplicas)) }}
{{- fail "api.autoscaling.minReplicas must not exceed maxReplicas" }}
{{- end }}
{{- end }}

{{/* Shared env (non-secret config) as a list, for the hook Job that runs before the ConfigMap exists. */}}
{{- define "hookrelay.configEnv" -}}
{{- range $k, $v := .Values.config }}
- name: {{ $k }}
  value: {{ $v | quote }}
{{- end }}
{{- end }}

{{/* envFrom for the long-running pods. */}}
{{- define "hookrelay.envFrom" -}}
- configMapRef:
    name: {{ include "hookrelay.configMapName" . }}
- secretRef:
    name: {{ include "hookrelay.secretName" . }}
{{- end }}

{{/* Roll pods when the ConfigMap or the chart-managed Secret changes. */}}
{{- define "hookrelay.checksums" -}}
checksum/config: {{ include (print .Template.BasePath "/configmap.yaml") . | sha256sum }}
{{- if not .Values.secrets.existingSecret }}
checksum/secret: {{ include (print .Template.BasePath "/secret.yaml") . | sha256sum }}
{{- end }}
{{- end }}

{{/* Pod-level scheduling for one role: (dict "ctx" $ "role" .Values.api "component" "api") */}}
{{- define "hookrelay.scheduling" -}}
{{- $ctx := .ctx }}
{{- $nodeSelector := merge (deepCopy (default dict .role.nodeSelector)) (default dict $ctx.Values.nodeSelector) }}
{{- with $nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- $affinity := default $ctx.Values.affinity .role.affinity }}
{{- with $affinity }}
affinity:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- $tolerations := concat (default list $ctx.Values.tolerations) (default list .role.tolerations) }}
{{- with $tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- if $ctx.Values.topologySpread.enabled }}
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: ScheduleAnyway
    labelSelector:
      matchLabels:
        {{- include "hookrelay.componentSelectorLabels" (dict "ctx" $ctx "component" .component) | nindent 8 }}
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: ScheduleAnyway
    labelSelector:
      matchLabels:
        {{- include "hookrelay.componentSelectorLabels" (dict "ctx" $ctx "component" .component) | nindent 8 }}
{{- end }}
{{- end }}

{{/* Pod security + common volumes. */}}
{{- define "hookrelay.podSecurity" -}}
securityContext:
  {{- toYaml .Values.podSecurityContext | nindent 2 }}
{{- with .Values.imagePullSecrets }}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}

{{- define "hookrelay.tmpVolume" -}}
- name: tmp
  emptyDir:
    medium: Memory
    sizeLimit: {{ .Values.tmpVolume.sizeLimit }}
{{- end }}

{{- define "hookrelay.tmpMount" -}}
- name: tmp
  mountPath: /tmp
{{- end }}

{{/* Exec probe on a role's heartbeat file (stdlib-only probe, ~0.05 s CPU). */}}
{{- define "hookrelay.heartbeatProbe" -}}
exec:
  command: ["python", "-m", "hookrelay.healthcheck", "--heartbeat", {{ .role | quote }}]
periodSeconds: {{ .probe.periodSeconds }}
timeoutSeconds: {{ .probe.timeoutSeconds }}
failureThreshold: {{ .probe.failureThreshold }}
{{- end }}

{{/* Default KEDA demand query, scoped to the release namespace. */}}
{{- define "hookrelay.kedaQuery" -}}
{{- if .Values.worker.autoscaling.prometheus.query }}
{{- .Values.worker.autoscaling.prometheus.query }}
{{- else }}
{{- $sel := printf "{namespace=%q}" .Release.Namespace }}
{{- printf "max(hookrelay_deliveries_due%s) + max(hookrelay_deliveries_delivering%s)" $sel $sel }}
{{- end }}
{{- end }}

{{- define "hookrelay.kedaThreshold" -}}
{{- $w := .Values.worker }}
{{- if $w.autoscaling.deliveriesPerReplica }}
{{- $w.autoscaling.deliveriesPerReplica | toString }}
{{- else }}
{{- max 1 (div (mul (int $w.concurrency) 3) 4) | toString }}
{{- end }}
{{- end }}
