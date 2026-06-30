{{- define "t2c.name" -}}
{{- .Values.appName -}}
{{- end -}}

{{- define "t2c.labels" -}}
app.kubernetes.io/name: {{ .Values.appName }}
app.kubernetes.io/part-of: {{ .Values.appName }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "t2c.image" -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}
