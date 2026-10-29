# VisaoUnicaPreco: mapeamento para o teste no Databricks

Levantado em 2026-10-26 a partir do código no bucket do Composer, dos manifestos SparkApplication, dos logs de pod e dos logs do Airflow no Cloud Logging. O código nesta pasta é a cópia do bucket nessa data, sem nenhuma alteração. Os dados que a DAG lê e escreve estão no projeto `maga-bigdata-databricks-poc`, em [Dados no projeto da POC](#dados-no-projeto-da-poc).

## Origem

| Item | Valor |
| --- | --- |
| Ambiente Composer | `critical-prd` (produção) |
| Airflow | https://31c5efd94119490bb4cb9f8673a79a42-dot-us-east1.composer.googleusercontent.com/dags/VisaoUnicaPreco/grid |
| Dag id | `VisaoUnicaPreco` |
| Arquivo da DAG | `visao_unica_preco/dags/main.py` |
| Código copiado de | `gs://us-east1-critical-prd-34bbf635-bucket/dags/visao_unica_preco/` e `.../dags/visao_unica_commons/` |
| Cluster GKE | `minecraft-critical`, zona `us-east1-b`, namespace `critical` |
| Gatilho | Datasets do Airflow: roda quando `base_prices_current` E `selling_prices2_current` são atualizados |

Fluxo: `Start >> trusted_prices >> bq_prices >> publisher >> End`. Só `trusted_prices` e `publisher` são Spark. O alvo do teste é `trusted_prices`, que concentra 99% do custo.

Pastas desta cópia:

- `visao_unica_preco/etls/trusted/visao_precos.py`: o job da `trusted_prices`.
- `visao_unica_preco/etls/refined/publisher_changed_prices.py`: o job da `publisher`.
- `visao_unica_commons/etls/utils/utils.py`: enviado aos dois jobs como `pyFiles`.
- `main_fast.py` e `main_historic.py` definem outras duas DAGs, `VisaoUnicaPrecoFast` e `VisaoUnicaPreco_Historico`, fora do escopo.

## Entradas e saídas de dados

Todos os datasets são tabelas Delta (têm `_delta_log`). O caminho segue `gs://{env}-lake-{zona}-{namespace}/{dataset}`, resolvido em tempo de execução pela sness-path-api e confirmado nos logs desta DAG.

### Entradas da trusted_prices

As entradas de origem são lidas sempre de produção, independente do `--environment`, porque o código fixa `"prd"` ou `Environment.PRODUCTION`.

| # | Dataset | Caminho GCS | Região do bucket | Função no código | Filtros aplicados |
| --- | --- | --- | --- | --- | --- |
| 1 | base_prices_current | `gs://prd-lake-refined-visao_unica/base_prices_current` | US-CENTRAL1 | `get_base_prices` | `is_deleted = false` |
| 2 | selling_prices2_current | `gs://prd-lake-refined-visao_unica/selling_prices2_current` | US-CENTRAL1 | `get_selling_prices` | não deletado, não expirado, `channel_id='*'`, `campaign_code in (0, 8349)` |
| 3 | discounts_v2 | `gs://prd-lake-trusted-visao_unica/discounts_v2` | US-CENTRAL1 | `get_arcoiro_discounts` | último evento por `_id`, campanha ativa agora |
| 4 | products_discounts | `gs://prd-lake-trusted-visao_unica/products_discounts` | US-CENTRAL1 | `get_product_discounts` | último evento por `_id`, campanha não terminada |
| 5 | campaigns_v2 | `gs://prd-lake-trusted-visao_unica/campaigns_v2` | US-CENTRAL1 | `get_arcoiro_campaigns` | campanha ativa agora, pagamento `visa` ou `luiza_ouro` |
| 6 | robinhood_campaigns | `gs://prd-lake-trusted-visao_unica/robinhood_campaigns` | US-CENTRAL1 | `get_robinhood_campaigns` | ativa agora e `benefit='loyalty'` |
| 7 | robinhood_campaign_products | `gs://prd-lake-trusted-visao_unica/robinhood_campaign_products` | US-CENTRAL1 | `get_robinhood_campaigns` | `action='insert'` |
| 8 | visao_catalogo_produto_atual | `gs://prd-lake-trusted-catalogo/visao_catalogo_produto_atual` | US-EAST1 | `get_catalog` em utils.py | remove inativos há mais de 30 dias e `category_id='TM'` |

Entradas que são saídas do próprio job, lidas de volta no ambiente do `--environment`:

| Dataset | Quando é lido |
| --- | --- |
| `refined/visao_unica/prices` | No início, para copiar para `prices_penultimate_execution`. No fim, para calcular as mudanças. |
| `refined/visao_unica/prices_historic` | Para saber se a partição de hoje já existe. |
| `refined/visao_unica/prices_penultimate_execution` | No fim, para calcular as mudanças. |

Atenção: vários filtros usam `current_timestamp()`. O mesmo snapshot processado em horários diferentes dá resultados diferentes.

### Saídas da trusted_prices

Com `--environment prd`, o bucket é `prd-lake-refined-visao_unica` (US-CENTRAL1). Com `--environment stg`, é `stg-lake-refined-visao_unica` (US-EAST1).

| Ordem | Dataset | Modo | Partição |
| --- | --- | --- | --- |
| 1 | `prices_penultimate_execution` | overwrite, cópia da `prices` anterior | nenhuma |
| 2 | `prices` | overwrite, 150 arquivos | nenhuma |
| 3 | `prices_historic` | append. Grava a tabela inteira na 1ª execução do dia e só as linhas alteradas nas outras. | `price_date` |

Efeito colateral: a SnessSpark registra cada escrita como tabela no Hive Metastore, por exemplo `visao_unica_refined.prices`. O metastore está em `thrift://10.209.0.58:9083` e o warehouse em `gs://prd-hive-metadata/spark/warehouse`. Nos logs de hoje esse registro falha sem derrubar o job, com `DELTA_CREATE_TABLE_SCHEME_MISMATCH`.

### Volume no GCS

Medido na origem em 2026-10-26 com `gcloud storage ls -r -l`. A coluna da pasta soma todos os objetos, inclusive versões antigas que o VACUUM ainda não removeu. Os arquivos da versão atual vêm do `numOfAddFiles` do último checkpoint Delta.

| Dataset | Bucket | Região | Objetos na pasta | Tamanho da pasta | Arquivos na versão atual |
| --- | --- | --- | --- | --- | --- |
| base_prices_current | prd-lake-refined-visao_unica | US-CENTRAL1 | 20.515 | 359,5 GiB | 200 |
| selling_prices2_current | prd-lake-refined-visao_unica | US-CENTRAL1 | 352.486 | 2.106,5 GiB | 800 |
| discounts_v2 | prd-lake-trusted-visao_unica | US-CENTRAL1 | 17.702 | 8,1 GiB | 2.120 |
| products_discounts | prd-lake-trusted-visao_unica | US-CENTRAL1 | 315 | 0,04 GiB | 286 |
| campaigns_v2 | prd-lake-trusted-visao_unica | US-CENTRAL1 | 14.628 | 43,6 GiB | 1.959 |
| robinhood_campaigns | prd-lake-trusted-visao_unica | US-CENTRAL1 | 7.336 | 0,25 GiB | 1.709 |
| robinhood_campaign_products | prd-lake-trusted-visao_unica | US-CENTRAL1 | 7.330 | 1,2 GiB | 1.691 |
| visao_catalogo_produto_atual | prd-lake-trusted-catalogo | US-EAST1 | 45.397 | 657,5 GiB | 8.960 |
| prices | prd-lake-refined-visao_unica | US-CENTRAL1 | 4.225 | 23,3 GiB | 150 |
| prices_penultimate_execution | prd-lake-refined-visao_unica | US-CENTRAL1 | 4.378 | 25,2 GiB | 150 |
| prices_historic | prd-lake-refined-visao_unica | US-CENTRAL1 | 122.598 | 4.044,4 GiB | 121.776 |

As oito entradas somam cerca de 3.177 GiB de pasta, ou 3,1 TiB. A maior parte é histórico de versões da selling_prices2_current: são 352 mil objetos na pasta e 800 arquivos na versão atual.

### Tasks fora do Spark que tocam dados

| Task | Lê | Escreve |
| --- | --- | --- |
| `bq_prices` | `gs://prd-lake-refined-visao_unica/prices/` | BigQuery `maga-bigdata.visao_unica.prices`, overwrite, partição diária por `price_date` |
| `publisher` | `refined/visao_unica/prices` e `prices_penultimate_execution` | Pub/Sub `projects/magalu-digital-project/topics/visao-unica-tags`, uma mensagem por preço alterado |

O tópico do publisher está fixo no código e é o mesmo em qualquer ambiente. Rodar o publisher fora de produção publica mudanças de preço reais. Não rode no teste.

### Dependências externas do job

| Dependência | Uso |
| --- | --- |
| sness (`SnessSpark`, `Environment`, `Zone`) | Leitura e escrita no lake. Vem do PyPI interno. |
| sness-path-api (`SNESS_PATH_API_URL`, `SNESS_PATH_API_TOKEN`) | Resolve o caminho de cada dataset em tempo de execução |
| Hive Metastore | Registro das tabelas escritas |
| Delta Lake | Formato de todas as tabelas |
| Uniffle (`RssShuffleManager`) | Shuffle remoto no GKE |

## Tempo de execução

Últimas 60 execuções completas, de 2026-10-24 06:27 a 2026-10-26 18:18 UTC. Por execução, o arquivo `execucoes_ultimas_60.csv` traz início, fim, duração de cada task, máquina do executor, pico de executores e custo estimado. Nos últimos 6 dias foram 142 execuções, todas com sucesso, sem falha nem retry.

| Task | Média | Mediana | P90 | Mínimo | Máximo |
| --- | --- | --- | --- | --- | --- |
| trusted_prices | 22,4 min | 14,4 min | 41,3 min | 12,0 min | 55,6 min |
| bq_prices | 3,4 min | 3,4 min | 3,5 min | 3,1 min | 3,7 min |
| publisher | 4,8 min | 5,2 min | 5,9 min | 3,3 min | 8,0 min |
| DAG inteira | 31,2 min | 23,1 min | 50,5 min | 20,0 min | 65,3 min |

A duração da trusted_prices depende da máquina em que os executores caem:

| Máquina do executor | Execuções | Média | Mediana | Custo médio estimado |
| --- | --- | --- | --- | --- |
| n4d-highmem-96 | 42 | 14,1 min | 13,6 min | R$ 1,85 |
| m1-ultramem-40 | 18 | 41,7 min | 41,0 min | R$ 7,57 |

O custo vem da linha `[CUSTO]` do MinecraftOperator. Ela estima vCPU e memória reservadas a preço de spot, e não é a fatura. A n4d-highmem-96 é o caso mais comum, e a m1-ultramem-40 é o pior caso.

## Configuração Spark atual

Tirada do manifesto SparkApplication da execução de 2026-10-26 17:30. A configuração completa, sem segredos, está em `spark_config_atual.json`.

| Item | trusted_prices | publisher |
| --- | --- | --- |
| Imagem | `us-east1-docker.pkg.dev/maga-bigdata/dataops/cloud-magalu/spark-jupyter-pyspark-joystick-line-3.5.5:1.0.21` | a mesma |
| Spark | 3.5.5, Python 3.11 | 3.5.5 |
| PodType | `DRIVER_LARGE` e `EXECUTOR_LARGE` | `DRIVER_SMALL` e `EXECUTOR_LARGE` |
| Driver | 3 cores, 10 GB e 2 GB de overhead. Request de 2 vCPU e limite de 3. | 1 core, 4 GB e 1 GB de overhead |
| Node pool do driver | `driver-large-os`, **e2-highmem-16**, on-demand | `driver-small-os`, **e2-highmem-8**, on-demand |
| Executor | 8 cores, 88 GB e 6 GB de overhead. Request de 4 vCPU e limite de 8. | 8 cores, 4 GB e 6 GB de overhead |
| Executores | Alocação dinâmica: inicial 12, mínimo 0, máximo 20. Picos observados de 20 a 39, com 20 em 37 das 60 execuções. | Alocação dinâmica: inicial 1, máximo 4 |
| Node pool do executor | Spot, escolhido por execução: `sp-e-n4d-v2` (**n4d-highmem-96**) ou `sp-e-m1-v2` (**m1-ultramem-40**) | `od-e-n4`, **n4-highmem-80**, on-demand |
| Service account | `dag-canaisdigitais` | `dag-canaisdigitais` |

O `spark.executor.cores=10` do sparkConf não bate com os 8 cores do pod. O pico de até 39 executores passa do `maxExecutors=20`. A causa não foi verificada. Uma hipótese é que executores spot substituídos entram na contagem.

Confs que o próprio job muda em tempo de execução, na função `config_spark_parallism`:

```
spark.sql.adaptive.enabled = false
spark.sql.adaptive.coalescePartitions.enabled = false
spark.sql.shuffle.partitions = 800
spark.default.parallelism = 800
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 2MB
```

Outras confs relevantes do manifesto: compressão zstd nível 1, KryoSerializer, Delta via `DeltaSparkSessionExtension` e `DeltaCatalog`, rebase LEGACY de datetime e int96 no Parquet.

## Dados no projeto da POC

Os dados que a DAG lê e escreve estão no projeto `maga-bigdata-databricks-poc`. São uma foto da produção de 2026-10-26, por volta das 21:00 UTC, e não acompanham as mudanças de produção depois disso.

### Buckets

| Bucket | Região | Conteúdo |
| --- | --- | --- |
| `prd-lake-refined-visao_unica_dbxpoc` | US-CENTRAL1 | base_prices_current, selling_prices2_current, prices, prices_penultimate_execution, prices_historic |
| `prd-lake-trusted-visao_unica_dbxpoc` | US-CENTRAL1 | discounts_v2, products_discounts, campaigns_v2, robinhood_campaigns, robinhood_campaign_products |
| `prd-lake-trusted-catalogo_dbxpoc` | US-EAST1 | visao_catalogo_produto_atual |
| `stg-lake-refined-visao_unica_dbxpoc` | US-CENTRAL1 | Vazio. Só seria usado com `--environment stg`. |

Cada bucket fica na mesma região do bucket de origem. Todos têm acesso uniforme, bloqueio de acesso público e os labels `se_environment=prd` e `purpose=databricks-poc-visaounicapreco`. O acesso hoje vem só dos papéis do projeto: owner e editor podem ler e escrever, e viewer pode ler.

### Tabelas

Todas são tabelas Delta. Cada pasta tem a tabela completa: os dados de todas as versões e o `_delta_log`.

| Dataset | Papel na DAG | Caminho na POC | Objetos | Tamanho |
| --- | --- | --- | --- | --- |
| base_prices_current | entrada | `gs://prd-lake-refined-visao_unica_dbxpoc/base_prices_current` | 20.714 | 363,1 GiB |
| selling_prices2_current | entrada | `gs://prd-lake-refined-visao_unica_dbxpoc/selling_prices2_current` | 354.087 | 2.116,1 GiB |
| discounts_v2 | entrada | `gs://prd-lake-trusted-visao_unica_dbxpoc/discounts_v2` | 17.704 | 8,1 GiB |
| products_discounts | entrada | `gs://prd-lake-trusted-visao_unica_dbxpoc/products_discounts` | 314 | 0,04 GiB |
| campaigns_v2 | entrada | `gs://prd-lake-trusted-visao_unica_dbxpoc/campaigns_v2` | 14.630 | 43,6 GiB |
| robinhood_campaigns | entrada | `gs://prd-lake-trusted-visao_unica_dbxpoc/robinhood_campaigns` | 7.337 | 0,3 GiB |
| robinhood_campaign_products | entrada | `gs://prd-lake-trusted-visao_unica_dbxpoc/robinhood_campaign_products` | 7.331 | 1,2 GiB |
| visao_catalogo_produto_atual | entrada | `gs://prd-lake-trusted-catalogo_dbxpoc/visao_catalogo_produto_atual` | 45.395 | 657,5 GiB |
| prices | saída, lida de volta | `gs://prd-lake-refined-visao_unica_dbxpoc/prices` | 4.380 | 25,2 GiB |
| prices_penultimate_execution | saída, lida de volta | `gs://prd-lake-refined-visao_unica_dbxpoc/prices_penultimate_execution` | 4.379 | 25,2 GiB |
| prices_historic | saída, lida de volta | `gs://prd-lake-refined-visao_unica_dbxpoc/prices_historic` | 122.593 | 4.044,4 GiB |

O total é de 7.285 GiB, cerca de 7,1 TiB. A última versão da `prices` na POC é a 17605.

### Fora da POC

O BigQuery da `bq_prices` (`maga-bigdata.visao_unica.prices`) e o Pub/Sub da `publisher` não foram copiados: são destinos de produção. As duas tasks não devem rodar no teste.

## Pontos de atenção

- **Regiões diferentes.** Sete das oito entradas estão em US-CENTRAL1, e o catálogo está em US-EAST1. Os buckets da POC seguem a mesma separação.
- **Foto de 2026-10-26.** Os dados da POC não acompanham produção. Os filtros com `current_timestamp()` vão descartar campanhas e preços que expiraram depois de 2026-10-26, então o resultado muda conforme o dia em que o teste roda.
- **Skew.** O job faz salting manual com `SKEW_MULTIPLY=100` e desliga o AQE.
- **Credenciais no código.** O código vai como está em produção e carrega duas credenciais. A primeira é uma URL de webhook do Google Chat com chave e token, na função `data_quality` em `visao_unica_commons/etls/utils/utils.py`, linha 1261, repetida no `README.md` da mesma pasta. A segunda é um token da API interna de catálogo em `visao_unica_preco/etls/trusted/fetch_catalogo.py`, linha 32. A trusted_prices não usa nenhuma das duas. Não versione esta pasta em repositório aberto.
