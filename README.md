# Entrega 4 - Procesamiento Stateful con Apache Flink sobre AWS

## Autor

**Maximiliano Jaida Santander**

## Objetivo

Implementar una solución de procesamiento de datos en tiempo real utilizando:

- Amazon Kinesis Data Streams
- Amazon Managed Service for Apache Flink
- Amazon CloudWatch
- Amazon S3
- AWS IAM
- Terraform

La aplicación consume eventos de sensores urbanos desde un stream de Kinesis, procesa la información utilizando Event Time y Watermarks, 
calcula métricas agregadas por ventanas temporales y mantiene estado utilizando Keyed State.

---

# Arquitectura

+----------------------+
| Productor de Eventos |
| JSON Sensores        |
+----------+-----------+
           |
		   v
+----------------------+
| Amazon Kinesis       |
| Data Stream          |
| dev-stream           |
+----------+-----------+
           |
           v
+---------------------------------+
| Amazon Managed Service          |
| for Apache Flink                |
| urban-sensors-flink-dev         |
+---------------+---------------- +
                |
                |
+---------------+------------------+
|                                  |
v                                  v

+-------------------------+ +-------------------------+
| Event Time Processing   | | Stateful Processing     |
| Watermarks (10 sec)     | | ValueState<Long>        |
| Tumbling Window (1 min) | | processedWindows        |
+-------------------------+ +-------------------------+

                           |
                           v

+---------------------------------+
| Results / Window Aggregations   |
| Avg Temperature                 |
| Avg Air Quality                 |
| Event Count                     |
+---------------+-----------------+
                |
                v
 
+----------------------+
| CloudWatch Logs      |
| Monitoring           |
| Troubleshooting      |
+----------------------+
 
           |
           v
 
+----------------------+
| Checkpoints          |
| S3 Managed Storage   |
+----------------------+

La solución está compuesta por los siguientes componentes:

1. Productor de eventos
2. Amazon Kinesis Data Streams
3. Apache Flink (Managed Service)
4. Stateful Processing
5. Checkpointing
6. CloudWatch Logs
7. Amazon S3
8. Terraform

Flujo:

Eventos JSON
↓
Kinesis Data Stream
↓
Apache Flink
↓
Watermarks
↓
Tumbling Window (1 minuto)
↓
State Management
↓
CloudWatch Logs

---

# Infraestructura Terraform

## Módulos implementados

```text
modules/
├── network
├── identity
├── kinesis
└── flink
```

## Recursos AWS creados

### Kinesis

```hcl
aws_kinesis_stream
```

### Firehose

```hcl
aws_kinesis_firehose_delivery_stream
```

### S3

```hcl
aws_s3_bucket
```

### KMS

```hcl
aws_kms_key
```

### Apache Flink

```hcl
aws_kinesisanalyticsv2_application
```

### CloudWatch

```hcl
aws_cloudwatch_log_group
aws_cloudwatch_log_stream
```

### IAM

```hcl
aws_iam_role
aws_iam_role_policy
```

---

# Stream utilizado

Nombre:

```text
dev-stream
```

Obtención mediante Terraform:

```hcl
module.kinesis.stream_name
```

---

# Aplicación Apache Flink

Clase principal:

```text
UrbanSensorsFlinkApp
```

Aplicación desplegada:

```text
urban-sensors-flink-dev
```

Runtime:

```text
FLINK-1_19
```

---

# Eventos procesados

Formato JSON:

```json
{
  "sensor_id": "sensor_1",
  "temperature": 21.5,
  "humidity": 55.2,
  "air_quality_index": 42,
  "timestamp": "2026-08-29T15:12:30.507Z"
}
```

---

# Event Time

Se implementó procesamiento basado en tiempo del evento.

Código:

```java
return event.getTimestamp();
```

[CONFIGURACIÓN EVENT TIME]

                        .withTimestampAssigner(
                                new SerializableTimestampAssigner
                                        <UrbanSensorEvent>() {

                                    @Override
                                    public long extractTimestamp(
                                            UrbanSensorEvent event,
                                            long recordTimestamp) {

                                        return event.getTimestamp();
                                    }
                                }
                        )

---

# Watermarks

Se configuró una tolerancia de eventos fuera de orden de 10 segundos.

Código:

```java
WatermarkStrategy
        .forBoundedOutOfOrderness(
                Duration.ofSeconds(10)
        )
```


[CONFIGURACIÓN WATERMARK]

 WatermarkStrategy<UrbanSensorEvent> watermarkStrategy =
                WatermarkStrategy
                        .<UrbanSensorEvent>forBoundedOutOfOrderness(
                                Duration.ofSeconds(10)
                        )

# Ventanas Temporales

Se implementó una ventana Tumbling de 1 minuto.

Código:

```java
TumblingEventTimeWindows.of(
        Time.minutes(1)
)
```


[CONFIGURACIÓN WINDOW]

                events
                        .keyBy(UrbanSensorEvent::getSensorId)
                        .window(
                                TumblingEventTimeWindows.of(
                                        Time.minutes(1)
                                )
                        )
                        .aggregate(new SensorMetricsAggregate());

---

# Stateful Processing

La aplicación mantiene estado por sensor utilizando:

```java
ValueState<Long>
```

Código:

```java
private transient ValueState<Long> processedWindows;
```

El contador registra cuántas ventanas han sido procesadas por cada sensor.

[IMPLEMENTACIÓN VALUESTATE]

        private transient ValueState<Long> processedWindows;

        @Override
        public void open(Configuration parameters) {

            ValueStateDescriptor<Long> descriptor =
                    new ValueStateDescriptor<>(
                            "processed-windows-by-sensor",
                            Long.class
                    );

            processedWindows =
                    getRuntimeContext().getState(descriptor);
					
					
            Long currentCount =
                    processedWindows.value();

            if (currentCount == null) {
                currentCount = 0L;
            }

            long updatedCount = currentCount + 1L;

            processedWindows.update(updatedCount);

---

# Checkpointing

Configuración:

```java
environment.enableCheckpointing(60000L);
```

Configuraciones adicionales:

```java
.setMinPauseBetweenCheckpoints(30000L)

.setCheckpointTimeout(120000L)

.setMaxConcurrentCheckpoints(1)
```


[CONFIGURACIÓN CHECKPOINT]

        environment.enableCheckpointing(60000L);

        environment
                .getCheckpointConfig()
                .setMinPauseBetweenCheckpoints(30000L);

        environment
                .getCheckpointConfig()
                .setCheckpointTimeout(120000L);

        environment
                .getCheckpointConfig()
                .setMaxConcurrentCheckpoints(1);
---

# Validación de Terraform

Validación ejecutada:

```powershell
terraform validate
```

Resultado:

```text
Success! The configuration is valid.
```


[TERRAFORM VALIDATE]

PS C:\Users\jaidasan\OneDrive - TomTom\Documents\Curso Data Engineering\Entrega1\environments\dev> terraform validate
Success! The configuration is valid.
---

# Despliegue de Apache Flink

Consulta del estado:

```powershell
aws kinesisanalyticsv2 describe-application `
  --application-name urban-sensors-flink-dev `
  --query "ApplicationDetail.[ApplicationName,ApplicationStatus,RuntimeEnvironment]" `
  --output table
```

Resultado esperado:

```text
RUNNING
```


[FLINK RUNNING]

PS C:\Users\jaidasan\OneDrive - TomTom\Documents\Curso Data Engineering\Entrega1\environments\dev> aws kinesisanalyticsv2 describe-application --application-name urban-sensors-flink-dev --query "ApplicationDetail.ApplicationStatus" --output text --no-cli-pager
RUNNING
---

# Verificación del Checkpointing

Comando utilizado:

```powershell
aws logs tail "/aws/kinesis-analytics/urban-sensors-flink-dev" `
  --log-stream-names "application" `
  --since 30m `
  --format short `
  --no-cli-pager
```

Evidencia esperada:

```text
checkpointStatus=COMPLETED
```

y

```text
Completed checkpoint
```


[CHECKPOINT COMPLETADO]

2026-08-29T16:49:15 {"applicationARN":"arn:aws:kinesisanalytics:us-east-1:032619915567:application/urban-sensors-flink-dev",
"applicationVersionId":"2","locationInformation":"org.apache.flink.traces.slf4j.Slf4jTraceReporter.notifyOfAddedSpan(Slf4jTraceReporter.java:37)",
"logger":"org.apache.flink.traces.slf4j.Slf4jTraceReporter","message":"Reported span: SimpleSpan{scope=org.apache.flink.runtime.checkpoint.CheckpointStatsTracker,
name=Checkpoint, startTsMillis=1788022155185, endTsMillis=1788022155360, attributes={jobId=cdb5f9e280aae82c88aaaa30e5bcdeb6, checkpointId=49,
checkpointStatus=COMPLETED, fullSize=30788, checkpointedSize=30788}}","messageSchemaVersion":"1","messageType":"INFO","threadName":"jobmanager-io-thread-1"}

---

# Verificación de conexión con Kinesis

Evidencia observada:

```text
Source: Kinesis Urban Sensors Source
```


[CONEXIÓN KINESIS]

2026-08-29T16:49:15 {"applicationARN":"arn:aws:kinesisanalytics:us-east-1:032619915567:application/urban-sensors-flink-dev",
"applicationVersionId":"2","locationInformation":"org.apache.flink.runtime.source.coordinator.SourceCoordinator.lambda$notifyCheckpointComplete$8(SourceCoordinator.java:411)",
"logger":"org.apache.flink.runtime.source.coordinator.SourceCoordinator",
"message":"Marking checkpoint 49 as completed for source Source: Kinesis Urban Sensors Source.","messageSchemaVersion":"1","messageType":"INFO","threadName":"SourceCoordinator-Source: Kinesis Urban Sensors Source"}

---
