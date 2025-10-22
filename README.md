# Documentação

Esse script tem como finalidade de converter/mover arquivos para o S3.
Seja do S3 para o S3 como do GCS para o S3.

# Implantação

A implantação do código é feita manualmente, enviando o código para o ACR. Antes é necessário se autenticar corretamente
[Configurar a autenticação no Artifact Registry para o Docker](https://cloud.google.com/artifact-registry/docs/docker/authentication?hl=pt-br)

## main

```shell
docker build -t us-central1-docker.pkg.dev/api-project-1033684201634/s3-handler/s3-handler .
docker push us-central1-docker.pkg.dev/api-project-1033684201634/s3-handler/s3-handler
```

## gcs_to_s3

```shell
docker build -f Dockerfile.fromgcs -t us-central1-docker.pkg.dev/api-project-1033684201634/s3-handler/s3-handler-from-gcs . 
docker push us-central1-docker.pkg.dev/api-project-1033684201634/s3-handler/s3-handler-from-gcs
```

# Usos

- [s3-handler](https://console.cloud.google.com/run/jobs/details/us-central1/s3-handler/executions?project=api-project-1033684201634)
- [s3-handler-airbyte-landing](https://console.cloud.google.com/run/jobs/details/us-central1/s3-handler-airbyte-landing/executions?project=api-project-1033684201634)
- [s3-handler-from-gcs](https://console.cloud.google.com/run/jobs/details/us-central1/s3-handler-from-gcs/executions?project=api-project-1033684201634)
- [s3-handler-ga4](https://console.cloud.google.com/run/jobs/details/us-central1/s3-handler-ga4/executions?project=api-project-1033684201634)

# Environment

- MAX_WORKERS: numero de workers para o thread pool executor
- FILE_LIMIT: limite de arquivos convertidos, use 0 para indefinido
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- S3_BUCKET: bucket de onde serão obtidos os arquivos
- S3_PATHS: paths de obtenção dos arquivos separados por ";"
- GOOGLE_SERVICE_ACCOUNT: conta de serviço para acessar o GCS (necessário apenas na execução gcs_to_s3)
- BUCKET_ID: bucket id do GCS de onde os arquivos serão enviados (necessário apenas na execução gcs_to_s3)
- S3_LANDING: path para onde serão enviados os arquivos (necessário apenas na execução gcs_to_s3)