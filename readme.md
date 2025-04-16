# Open source Datalake
Tudo o que foi necessáro para fazer Orquestração de Spark com Airflow, tudo rodando em Docker Containers

O projeto foi construído:
- Orquestrador: Airflow
- Engine: Spark
- Object Storage: MinIO
- Catálogo: Unity Catalog OSS *

*Na versão atual do Unity Catalog OSS, a integração com o MinIO não funciona bem

Esta é meu segundo projeto desse tipo, e ele ficou mais limpo e organizado que o primeiro.

- Não usamos um container pronto do Spark. Em vez disso, utilizamos um container base ubuntu e:
    - Fizemos a instalçao do Spark 3.5
    - Baixamos os jars necessários para integrar o Spark com Deltalake, MinIO e Uniy Catalog

Os serviços sobem com o comando `make build`.

O comando `make notebook` sobre o serviço do jupyter, que pode ser conectado a partir da máquina local.