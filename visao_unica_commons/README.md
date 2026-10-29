# Visão Única Commons - Utilitários

## 📋 Índice
- [Objetivo](#-objetivo)
- [Estrutura](#-estrutura)
- [Funções Principais](#-funções-principais)
- [Uso nos Pipelines](#-uso-nos-pipelines)
- [Exemplos de Código](#-exemplos-de-código)
- [Referências](#-referências)

---

## 🎯 Objetivo

Biblioteca de **funções utilitárias compartilhadas** entre todos os pipelines da Visão Única. Centraliza lógica comum de:
- ✅ Configuração Spark (Delta Lake)
- ✅ Leitura/escrita streaming e batch
- ✅ Parsing de timestamps (micro/milli segundos)
- ✅ Deduplicação (base_prices, selling_prices)
- ✅ Change detection
- ✅ Skew handling (data imbalancing)
- ✅ Checkpoint management
- ✅ Qualidade de dados (Google Chat notifications)

---

## 📂 Estrutura

```
visao_unica_commons/
├── etls/
│   └── utils/
│       ├── __init__.py
│       └── utils.py              # Funções principais (único arquivo)
└── tests/
    └── utils/
        └── test_utils.py
```

**Nota:** Ao contrário da documentação anterior, existe apenas **1 arquivo** (`utils.py`) com todas as funções.

---

## 🔧 Funções Principais

### 1. **Configuração Spark**

#### `config_spark()`
Configura Spark com suporte a Delta Lake.

```python
def config_spark() -> SnessSpark:
    """Configure spark with packages necessary to replaceWhere work."""
    _ = (
        SparkSession.builder.config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .getOrCreate()
    )
    ss = SnessSpark()
    return ss
```

**Uso:**
```python
from visao_unica_commons.etls.utils.utils import config_spark

ss = config_spark()  # Spark configurado com Delta Lake
```

---

### 2. **Parsing de Argumentos**

#### `parse_args()`
Parse argumentos CLI padrão dos pipelines.

```python
def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse arguments from command line."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--environment", type=Environment, required=False, default="stg"
    )
    parser.add_argument(
        "--n_repartitions", type=int, required=False, default=20
    )
    parser.add_argument(
        "--start_date",
        type=parse_data_command_arg,
        required=False,
        default=None,
        help="Initial start date to process: format YYYY-MM-DD"
    )
    parser.add_argument(
        "--end_date",
        type=parse_data_command_arg,
        required=False,
        default=None,
        help="End date to process: format YYYY-MM-DD"
    )
    parser.add_argument(
        "--writing_mode",
        type=str,
        default="append",
    )
    parser.add_argument(
        "--days_to_process",
        type=int,
    )
    
    args = parser.parse_args(argv)
    return args
```

#### `parse_args_with_database()`
Parse com argumentos de conexão MongoDB.

```python
def parse_args_with_database(
    argv: Optional[Sequence[str]] = None,
) -> argparse.Namespace:
    """Parse arguments from command line."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--environment", type=Environment, required=False, default="stg")
    parser.add_argument("--n_repartitions", type=int, required=False, default=20)
    
    str_arguments = [
        ("--host", "host of the database instance", None),
        ("--port", "port of the database instance", None),
        ("--db_user", "user of the database", None),
        ("--db_password", "password of the database", None),
        ("--database", "the database name", None),
        ("--collection", "collection of the database", None),
    ]
    
    for argument, help_message, default_value in str_arguments:
        parser.add_argument(
            argument,
            type=str,
            required=False,
            default=default_value,
            help=help_message,
        )
    
    args = parser.parse_args(argv)
    return args
```

---

### 3. **Leitura de Dados**

#### `read_visao_unica_dataset()`
Lê dataset do namespace visao_unica.

```python
def read_visao_unica_dataset(
    ss: SnessSpark, environment: Environment, zone: Zone, dataset: str
) -> DataFrame:
    df = ss.read.parquet(
        environment=environment,
        zone=zone,
        namespace="visao_unica",
        dataset=dataset,
    )
    return df
```

#### `get_catalog_active_and_in_stock()`
Lê catálogo de produtos ativos com estoque.

```python
def get_catalog_active_and_in_stock(ss: SnessSpark) -> DataFrame:
    df = ss.read.parquet(
        Environment.PRODUCTION,
        Zone.TRUSTED,
        "catalogo",
        "visao_catalogo_produto_atual",
    ).filter((sf.col("active")) & (sf.col("stock")))
    return df
```

#### `get_catalog()`
Lê catálogo completo (sem filtros).

```python
def get_catalog(ss: SnessSpark) -> DataFrame:
    df = ss.read.parquet(
        Environment.PRODUCTION,
        Zone.TRUSTED,
        "catalogo",
        "visao_catalogo_produto_atual",
    )
    return df
```

#### `get_dataframe_in_mongo()`
Lê DataFrame diretamente do MongoDB.

```python
def get_dataframe_in_mongo(
    ss: SnessSpark,
    host: str,
    port: int,
    db_user: str,
    db_password: str,
    database: str,
    collection: str,
) -> DataFrame:
    host_uri = f"mongodb://{db_user}:{db_password}@{host}:{port}/?tls=false&ssl=false&replicaSet=mongolu_rs0"
    df = (
        ss.spark.read.option("uri", host_uri)
        .option("database", database)
        .option("collection", collection)
        .format("mongo")
        .load()
    )
    
    df.printSchema()
    return df
```

---

### 4. **Escrita de Dados**

#### `write_to_lake_overwrite_mode()`
Escreve em modo overwrite com particionamento.

```python
def write_to_lake_overwrite_mode(
    ss: SnessSpark,
    df: DataFrame,
    lake_environment: Environment,
    zone: Zone,
    namespace: str,
    dataset: str,
    repartition: Any,
    partition_by: Sequence[str],
):
    """Write the dataframe to lake in batch mode."""
    logger.info("Writing to lake in batch mode.")
    
    if repartition:
        df = df.repartition(repartition)
    
    ss.write.parquet(
        df=df,
        environment=lake_environment,
        zone=zone,
        mode="overwrite",
        namespace=namespace,
        dataset=dataset,
        partitionBy=partition_by,
    )
```

#### `write_to_lake_with_replace_where()`
Escreve usando Delta Lake replaceWhere (substitui apenas partições específicas).

```python
def write_to_lake_with_replace_where(
    ss: SnessSpark,
    df: DataFrame,
    lake_environment: Environment,
    zone: Zone,
    namespace: str,
    dataset: str,
    partition_by: Sequence[str],
    start_date: datetime,
    end_date: datetime,
):
    """Write the dataframe to lake in batch mode."""
    logger.info("Writing to lake in batch mode.")
    
    replace_where = f"{partition_by} >= '{start_date.date()}' and {partition_by} <= '{end_date.date()}' "
    
    ss.write.parquet(
        df=df,
        environment=lake_environment,
        zone=zone,
        mode="overwrite",
        namespace=namespace,
        dataset=dataset,
        partitionBy=partition_by,
        replaceWhere=replace_where,
    )
```

#### `write_to_lake_streaming_with_checkpoint()`
Escreve em modo streaming com checkpoint.

```python
def write_to_lake_streaming_with_checkpoint(
    ss: SnessSpark,
    df: DataFrame,
    lake_environment: Environment,
    zone: Zone,
    namespace: str,
    dataset: str,
    repartition: Any,
    partition_by: Sequence[str] = None,
):
    """Write the dataframe using streaming to the lake."""
    checkpoint_location = f"gs://{lake_environment.value}-lake-{zone.value}-{namespace}/checkpoint/{dataset}"
    print(f"writing the streaming checkpoint to {checkpoint_location}")
    
    if repartition:
        df = df.repartition(repartition)
    
    ss.writeStream.parquet(
        df=df,
        environment=lake_environment,
        zone=zone,
        mode="append",
        namespace=namespace,
        dataset=dataset,
        checkpointLocation=checkpoint_location,
        partitionBy=partition_by,
    )
```

---

### 5. **Kafka Streaming**

#### `get_messages_from_kafka()`
Lê mensagens de tópico Kafka.

```python
def get_messages_from_kafka(ss: SnessSpark, topic_name: str) -> DataFrame:
    """Get messages from a topic in Kafka."""
    df_read_kafka = (
        ss.spark.readStream.format("kafka")
        .option(
            "kafka.bootstrap.servers",
            "kafka-data-engineering-ext.gcp.luizalabs.com:19092",
        )
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .load()
    )
    return df_read_kafka
```

#### `apply_base_message_schema()`
Aplica schema base na mensagem Kafka.

```python
def apply_base_message_schema(df: DataFrame, schema: str) -> DataFrame:
    """Apply a schema in the raw message from kafka."""
    df = df.select(
        sf.col("key").alias("kafka_key"),
        "timestamp",
        "offset",
        sf.from_json(sf.col("value").cast("string"), schema).alias("value"),
    ).select(
        "kafka_key",
        sf.col("timestamp").alias("timestamp_kafka"),
        sf.col("offset"),
        "value.*",
    )
    return df
```

#### `apply_table_schema()`
Aplica schema do MongoDB na mensagem.

```python
def apply_table_schema(df: DataFrame, table_schema: str) -> DataFrame:
    """Apply the schema of the mongodb database."""
    df = df.select(
        "kafka_key",
        "payload.op",
        sf.from_json("payload.after", table_schema).alias("values"),
        "timestamp_kafka",
    )
    
    # remove delete and null operations
    df = df.filter(sf.col("op").isin(["c", "u", "d"]))
    return df
```

#### `apply_kafka_key_scheme()`
Parse kafka key para extrair object ID.

```python
def apply_kafka_key_scheme(df: DataFrame, schema: str):
    """This function parse the kafka key value to get the object id of the row"""
    
    df = df.withColumn(
        "kafka_key",
        sf.from_json(sf.col("kafka_key").cast("string"), schema),
    )
    
    oid_schema = st.StructType([st.StructField("$oid", st.StringType())])
    
    df = df.withColumn(
        "kafka_key",
        sf.from_json(sf.col("kafka_key.payload.id"), oid_schema),
    )
    
    df = df.withColumn("obj_id_from_kafka_key", sf.col("kafka_key.$oid"))
    df = df.drop("kafka_key")
    
    return df
```

---

### 6. **Timestamp Parsing**

#### `parse_timestamp_in_micro()`
Parse timestamp em microsegundos (vários formatos).

```python
def parse_timestamp_in_micro(column: str) -> sf.col:
    """
    Parse a pyspark column that represents a timestamp in microseconds.
    
    Suporta:
    - ISO format: 1969-12-31T21:00:00
    - Epoch: 1721115793
    - NumberLong: {"$numberLong":"253402214400"}
    """
    TIMESTAMP_PATTERN = r"[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])T(2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9]"
    EPOCH_PATTERN = r"\d*"
    NUMBER_LONG_PATTERN = r"numberLong"
    
    match_nulo = sf.col(column).isNull()
    match_timestamp = sf.col(column).rlike(TIMESTAMP_PATTERN)
    match_epoch_time = sf.col(column).rlike(EPOCH_PATTERN)
    match_number_long = sf.col(column).rlike(NUMBER_LONG_PATTERN)
    
    extract_number_long = sf.from_unixtime(
        sf.regexp_extract(sf.col(column), r"(\d+)", 1)
    )
    
    col = sf.to_timestamp(
        (
            sf.when(match_nulo, sf.col(column))
            .when(match_timestamp, sf.to_timestamp(column))
            .when(match_number_long, extract_number_long)
            .when(match_epoch_time, sf.from_unixtime(column))
            .otherwise(sf.col(column))
        )
    )
    
    return col
```

#### `parse_timestamp_in_milli()`
Parse timestamp em milissegundos.

```python
def parse_timestamp_in_milli(column: str):
    """
    Parse a pyspark column that represents a timestamp in milliseconds.
    """
    col = sf.to_timestamp(
        sf.when(
            sf.col(column).isNull(), sf.lit(None).cast(st.TimestampType())
        ).otherwise(
            (sf.col(column) / 1000).cast(st.TimestampType()),
        )
    )
    return col
```

---

### 7. **Deduplicação Específica**

#### `selling_prices_dedup()`
Deduplica selling_prices com lógica complexa (origin priority).

```python
def selling_prices_dedup(df: DataFrame) -> DataFrame:
    """
    Deduplicação de selling_prices com priorização por origin.
    
    Lógica:
    1. Remove deletados (op='d')
    2. Row number por _id.$oid (timestamp_kafka desc)
    3. Filtra deleted_at IS NULL
    4. Filtra preços válidos (current_timestamp BETWEEN start_at AND end_at)
    5. Aplica origin priority (acme-admin-priority=1, kratos=2, ...)
    6. Row number por id (origin_priority asc, price asc)
    7. Mantém rank=1
    """
    
    # removendo linhas que foram deletadas.
    df = df.withColumn(
        "rank_actions",
        sf.row_number().over(
            Window.partitionBy("_id.$oid").orderBy(sf.desc("timestamp_kafka"))
        ),
    ).filter(sf.col("rank_actions") == 1)
    
    df = df.filter(sf.col("op") != 'd')
    
    # filtrar por preços ainda válidos.
    df = df.filter(sf.col("deleted_at").isNull())
    df = df.filter(
        sf.current_timestamp().between(sf.col("start_at"), sf.col("end_at"))
    )
    
    # rankeamento conforme a origem.
    ORIGIN_PRIORITY = [
        ("acme-admin-priority", 1),
        ("kratos", 2),
        ("acme-admin", 3),
        ("adm-site", 4),
        ("promo3p", 5),
        ("integra", 6),
        ("api", 7),
    ]
    
    origin_rank_col = None
    for origin, priority in ORIGIN_PRIORITY:
        if origin_rank_col is None:
            origin_rank_col = sf.when(
                sf.col("origin") == origin, sf.lit(priority)
            )
        else:
            origin_rank_col = origin_rank_col.when(
                sf.col("origin") == origin, sf.lit(priority)
            )
    
    df = df.withColumn("origin_priority", origin_rank_col)
    
    df = df.withColumn(
        "price_rank",
        sf.row_number().over(
            Window.partitionBy("id").orderBy(
                sf.asc("origin_priority"), sf.asc("price")
            )
        ),
    )
    
    df = df.filter(sf.col("price_rank") == sf.lit(1))
    
    return df.select(
        "seller_id", "sku", "channel_id", "campaign_code", "price"
    )
```

#### `base_prices_dedup()`
Deduplica base_prices (mantém mais recente por seller+sku).

```python
def base_prices_dedup(df: DataFrame) -> DataFrame:
    """
    Deduplicação simples de base_prices.
    
    Lógica:
    1. Row number por (seller_id, sku)
    2. Order by timestamp_kafka desc
    3. Mantém pos=1 (mais recente)
    """
    
    df = (
        df.select(
            'seller_id',
            'sku',
            'list_price',
            'timestamp_kafka',
            'selections',
            'currency',
            sf.row_number()
            .over(
                Window.partitionBy("seller_id", "sku").orderBy(
                    sf.col("timestamp_kafka").desc()
                )
            )
            .alias("pos"),
        )
        .filter(sf.col("pos") == 1)
        .drop("pos")
    )
    
    return df.select("seller_id", "sku", "list_price")
```

---

### 8. **Change Detection**

#### `get_changed_rows()`
Detecta mudanças entre execuções (penultimate vs ultimate).

```python
def get_changed_rows(
    fixed_fields,
    fields_to_compare,
    columns_to_join,
    actual_df,
    last_df,
):
    """
    Compara DataFrames atual vs anterior para detectar mudanças.
    
    Args:
        fixed_fields: Colunas fixas a manter
        fields_to_compare: Campos para comparar mudanças
        columns_to_join: Colunas de join
        actual_df: DataFrame atual (ultimate)
        last_df: DataFrame anterior (penultimate)
    
    Returns:
        DataFrame com apenas registros que mudaram
    """
    last_df = last_df.select(
        *[sf.col(c).alias(f"{c}_old") for c in last_df.columns]
    )
    
    filters = []
    
    for price_field in fields_to_compare:
        filters.append(
            sf.coalesce(actual_df[price_field], sf.lit(0))
            != sf.coalesce(last_df[f"{price_field}_old"], sf.lit(0))
        )
    
    condicoes_join = reduce(
        lambda a, b: a & b,
        [actual_df[col] == last_df[f"{col}_old"] for col in columns_to_join],
    )
    
    changed_prices = (
        actual_df.join(
            last_df,
            how="left",
            on=[condicoes_join],
        )
        .filter(reduce(lambda a, b: a | b, filters))
        .select(*fixed_fields, *fields_to_compare)
    )
    
    return changed_prices
```

#### `save_penultimate_execution()`
Salva snapshot da execução atual como penultimate para próxima run.

```python
def save_penultimate_execution(
    ss: SnessSpark,
    environment: Environment,
    zone: Zone,
    dataset_readen: str,
    dataset_written: str,
):
    """
    Copia dataset atual para dataset penultimate (próxima execução usa como comparação).
    """
    try:
        df = ss.read.parquet(
            environment=environment,
            zone=zone,
            namespace="visao_unica",
            dataset=dataset_readen,
        )
        
        ss.write.parquet(
            df,
            environment=environment,
            zone=zone,
            namespace="visao_unica",
            dataset=dataset_written,
            mode="overwrite",
            overwriteSchema=True,
        )
    
    except Exception as e:
        print(f"Dataframe not found: {e}")
```

---

### 9. **Skew Handling (Data Imbalancing)**

#### `add_skew_column()`
Adiciona coluna skew usando hash (para join com explode).

```python
def add_skew_column(
    df: DataFrame, skew_multiply: int, columns: List[str]
) -> DataFrame:
    """
    Adiciona coluna 'skew' calculando hash de colunas + random.
    
    Uso: Lado "pequeno" do join (que será hasheado).
    """
    return df.withColumn(
        "skew",
        sf.abs(
            (
                sf.xxhash64(
                    sf.concat(
                        *[sf.col(col) for col in columns],
                        (sf.rand() * sf.lit(10)).cast(st.StringType()),
                    )
                )
                % sf.lit(skew_multiply)
            ).cast("integer")
        ),
    )
```

#### `add_skew_array()`
Adiciona coluna skew usando array + explode (para lado grande do join).

```python
def add_skew_array(df: DataFrame, skew_multiply: int) -> DataFrame:
    """
    Adiciona coluna 'skew' com array [0, 1, 2, ..., skew_multiply-1]
    e explode (multiplica registros).
    
    Uso: Lado "grande" do join (que será explodido).
    """
    return df.withColumn(
        "skew",
        sf.explode(sf.array([sf.lit(n) for n in range(0, skew_multiply)])),
    )
```

#### `join_solve_imbalancing()`
Executa join com skew handling automático.

```python
def join_solve_imbalancing(
    df_to_hash: DataFrame,
    df_to_explode: DataFrame,
    how: str,
    conditions: sf.Column,
    skew_multiply: int,
    columns_for_hashing: List[str],
) -> DataFrame:
    """
    Resolve data skew em joins usando técnica de hash + explode.
    
    Técnica:
    1. df_to_hash: Adiciona coluna skew com hash (lado pequeno)
    2. df_to_explode: Adiciona coluna skew com array explodido (lado grande)
    3. Join com condição + (skew_hash == skew_array)
    4. Remove coluna skew
    
    Args:
        df_to_hash: DataFrame pequeno (será hasheado)
        df_to_explode: DataFrame grande (será explodido)
        how: Tipo de join (inner, left, etc)
        conditions: Condições originais do join
        skew_multiply: Fator de multiplicação (ex: 10 = 10x registros)
        columns_for_hashing: Colunas para calcular hash
    """
    df_to_hash = add_skew_column(
        df_to_hash, skew_multiply, columns_for_hashing
    )
    
    df_to_explode = add_skew_array(df_to_explode, skew_multiply)
    
    conditions = conditions & (df_to_hash.skew == df_to_explode.skew)
    
    join_df = df_to_hash.join(
        df_to_explode,
        on=conditions,
        how=how,
    )
    return join_df.drop("skew")
```

**Exemplo de uso:**
```python
# Join normal causaria skew (1 seller com milhões de SKUs)
df_result = join_solve_imbalancing(
    df_to_hash=sellers,  # Pequeno (poucos sellers)
    df_to_explode=products,  # Grande (milhões de SKUs)
    how="inner",
    conditions=(sellers.seller_id == products.seller_id),
    skew_multiply=10,
    columns_for_hashing=["seller_id"]
)
```

---

### 10. **Checkpoint Management**

#### `get_checkpoint()`
Lê data do último checkpoint (ou usa default se não existir).

```python
def get_checkpoint(
    ss: SnessSpark,
    environment: Environment,
    zone: Zone,
    namespace: str,
    dataset_name: str,
    days_if_missing: int = 1,
) -> datetime:
    """
    Get the checkpoint date, if its not set, set the number of days behind to use.
    """
    try:
        checkpoint_date = get_last_execution_date(
            ss, environment, zone, namespace, dataset_name
        )
    except Exception as e:
        logger.warning(e)
        checkpoint_date = datetime.now() - timedelta(days=days_if_missing)
        logger.warning(
            f"Checkpoint date not found, setting checkpoint date to yesterday {checkpoint_date}"
        )
    return checkpoint_date
```

#### `get_last_execution_date()`
Lê timestamp do dataset de checkpoint.

```python
def get_last_execution_date(
    ss: SnessSpark,
    environment: str,
    zone: str,
    namespace: str,
    dataset_name: str,
) -> datetime:
    """Get the checkpoint date of a dataset."""
    LOG_TABLE_LAST_EXECUTION = f"{dataset_name}_checkpoint"
    
    last_exec_date = (
        ss.read.parquet(
            environment,
            zone,
            namespace,
            LOG_TABLE_LAST_EXECUTION,
        )
        .select("timestamp_last_execution")
        .collect()[0][0]
    )
    
    logger.warning(f"Read the last execution date as {last_exec_date}")
    
    return last_exec_date
```

#### `set_last_execution_date()`
Salva timestamp do checkpoint.

```python
def set_last_execution_date(
    ss: SnessSpark,
    last_exec_date: datetime,
    lake_environment: str,
    zone: str,
    namespace: str,
    dataset_name: str,
) -> None:
    """Set the checkpoint date of a dataset in the datalake."""
    
    logger.warning(f" setting the last execution date to {last_exec_date}")
    
    rows = [[last_exec_date]]
    df = ss.spark.createDataFrame(
        rows, schema="timestamp_last_execution timestamp"
    )
    ss.write.parquet(
        df,
        lake_environment,
        zone,
        namespace,
        f"{dataset_name}_checkpoint",
        mode="overwrite",
    )
    
    logger.warning(f" setted the last execution date to {last_exec_date}")
```

---

### 11. **Processamento em Lotes (Batching)**

#### `get_days_to_process()`
Divide período de datas em batches para processamento.

```python
def get_days_to_process(
    start_date: datetime,
    end_date: Optional[datetime] = datetime.now(),
    n_days_to_process_each_epoch: int = 5,
) -> Sequence[Tuple[datetime, datetime]]:
    """
    Build the list of days to be processed divided by days.
    
    Args:
        start_date: the day to start the etl.
        end_date: the day to end the etl.
        n_days_to_process_each_epoch: the number of days to group periods.
    
    Returns:
        A list of day periods to process the etl.
    
    Example:
        >>> periods = get_days_to_process(
        ...     start_date=datetime(2024, 1, 1),
        ...     end_date=datetime(2024, 1, 15),
        ...     n_days_to_process_each_epoch=5
        ... )
        >>> # Retorna: [(2024-01-01, 2024-01-05), (2024-01-06, 2024-01-10), (2024-01-11, 2024-01-15)]
    """
    # it alway read one day before the checkpoint
    start_date -= timedelta(days=1)
    
    days = [
        start_date + timedelta(days=i)
        for i in range(0, (end_date - start_date).days + 1)
    ]
    
    def split_list(
        lista: Sequence[datetime], qtd_itens: int
    ) -> Sequence[Tuple[datetime, datetime]]:
        return [
            lista[i : i + qtd_itens] for i in range(0, len(lista), qtd_itens)
        ]
    
    lista_share = split_list(days, n_days_to_process_each_epoch)
    
    return lista_share
```

---

### 12. **Qualidade de Dados**

#### `data_quality()`
Compara contagem entre zonas e notifica via Google Chat.

```python
def data_quality(
    ss,
    environment,
    zones,
    namespace,
    dataset,
    date_process,
):
    """
    Compara a contagem de registros entre duas zonas de dados para um dataset específico
    e envia uma notificação para o Google Chat se houver divergência.
    
    Args:
        ss: SnessSpark instance
        environment: Environment (prd, stg)
        zones: Lista de zonas para comparar (ex: ['raw', 'trusted'])
        namespace: Namespace do dataset
        dataset: Nome do dataset
        date_process: Data de processamento para filtro
    """
    webhook_url = 'https://chat.googleapis.com/v1/spaces/AAQAfTOVMwQ/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=BqQ36qeyZaOhCMS2l80J7dvl5N8VXJx6jVvff1Cq4n4'
    
    count_zones = []
    
    # for para fazer a leitura de todos os dataframes das zonas que foram passadas para a função
    for zone in zones:
        df = ss.read.parquet(environment, zone, namespace, dataset).filter(
            sf.col("timestamp_kafka_day") == date_process
        )
        count = df.count()
        count_zones.append((zone, count))
    
    resultados = []
    
    # for para calcular as diferenças entre uma zona e outra
    for i in range(len(count_zones) - 1):
        zone_1, count_1 = count_zones[i]
        zone_2, count_2 = count_zones[i + 1]
        resultado = count_1 - count_2
        
        if resultado != 0:
            resultados.append(
                f"- {zone_1} ({count_1}) - {zone_2} ({count_2}) | Diferença: {resultado}"
            )
    
    if resultados:
        message = (
            f"Diferenças de registros detectadas na ingestão kafka:\n"
            f"Dataset: `{dataset}` | Namespace: `{namespace}` | Data: `{date_process}`\n\n"
            + "\n".join(resultados)
        )
        send_google_chat_message(webhook_url, message)
```

#### `send_google_chat_message()`
Envia mensagem para webhook do Google Chat.

```python
def send_google_chat_message(webhook_url, message):
    """Envia uma mensagem para um webhook do Google Chat"""
    
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    payload = {"text": message}
    response = requests.post(
        webhook_url, headers=headers, data=json.dumps(payload), timeout=10
    )
    if response.status_code != 200:
        raise Exception(
            f"Erro ao enviar mensagem para o Google Chat: {response.status_code} - {response.text}"
        )
```

---

### 13. **Streaming Helpers**

#### `read_streaming_dataset()`
Lê dataset em modo streaming.

```python
def read_streaming_dataset(
    ss: SnessSpark,
    environment: Environment,
    zone: Zone,
    namespace: str,
    dataset_name: str,
) -> DataFrame:
    """Read a dataframe using streaming from the lake."""
    df = ss.readStream.parquet(environment, zone, namespace, dataset_name)
    return df
```

---

## 📖 Uso nos Pipelines

### Pipeline `notificacao_preco`
```python
from visao_unica_commons.etls.utils.utils import (
    config_spark,
    read_visao_unica_dataset,
    selling_prices_dedup,
    base_prices_dedup,
    get_changed_rows,
    save_penultimate_execution,
    write_to_lake_overwrite_mode
)

# 1. Configurar Spark com Delta Lake
ss = config_spark()

# 2. Ler datasets
ultimate_base = read_visao_unica_dataset(ss, 'prd', 'trusted', 'base_prices')
penultimate_base = read_visao_unica_dataset(ss, 'prd', 'trusted', 'price_de_penultimate_execution')

# 3. Deduplica
ultimate_base_dedup = base_prices_dedup(ultimate_base)

# 4. Change detection
changed_prices = get_changed_rows(
    fixed_fields=["seller_id", "sku"],
    fields_to_compare=["list_price", "op"],
    columns_to_join=["seller_id", "sku"],
    actual_df=ultimate_base_dedup,
    last_df=penultimate_base
)

# 5. Escreve
write_to_lake_overwrite_mode(
    ss=ss,
    df=changed_prices,
    lake_environment='prd',
    zone='refined',
    namespace='visao_unica',
    dataset='changed_prices',
    repartition=50,
    partition_by=['change_date']
)

# 6. Salva penultimate para próxima execução
save_penultimate_execution(
    ss, 'prd', 'trusted',
    dataset_readen='base_prices',
    dataset_written='price_de_penultimate_execution'
)
```

### Pipeline `visao_preco_carga_historica`
```python
from visao_unica_commons.etls.utils.utils import (
    config_spark,
    get_days_to_process,
    write_to_lake_with_replace_where
)

# 1. Configurar Spark com Delta Lake
ss = config_spark()

# 2. Dividir período em batches de 3 dias
periods = get_days_to_process(
    start_date=datetime(2024, 8, 23),
    end_date=datetime(2025, 1, 27),
    n_days_to_process_each_epoch=3
)

# 3. Processar cada batch
for start, end in periods:
    df_batch = read_backup().filter(
        col("price_date").between(start, end)
    )
    
    # 4. Escreve com replaceWhere (Delta Lake)
    write_to_lake_with_replace_where(
        ss=ss,
        df=df_batch,
        lake_environment='prd',
        zone='refined',
        namespace='visao_unica',
        dataset='prices_historic',
        partition_by=['price_date'],
        start_date=start,
        end_date=end
    )
```

### Pipeline com Skew Handling
```python
from visao_unica_commons.etls.utils.utils import join_solve_imbalancing

# Join normal causaria skew (1 seller com milhões de SKUs)
df_result = join_solve_imbalancing(
    df_to_hash=sellers,  # Pequeno (poucos sellers)
    df_to_explode=products,  # Grande (milhões de SKUs)
    how="inner",
    conditions=(sellers.seller_id == products.seller_id),
    skew_multiply=10,
    columns_for_hashing=["seller_id"]
)
```

---

## 💡 Exemplos de Código

### Exemplo 1: Pipeline Streaming Kafka → Lake
```python
from visao_unica_commons.etls.utils.utils import (
    config_spark,
    get_messages_from_kafka,
    apply_base_message_schema,
    apply_table_schema,
    parse_timestamp_in_micro,
    write_to_lake_streaming_with_checkpoint
)

ss = config_spark()

# 1. Lê Kafka
kafka_df = get_messages_from_kafka(ss, "base_prices_topic")

# 2. Aplica schema base
base_schema = "payload struct<op:string, after:string>"
df_with_base = apply_base_message_schema(kafka_df, base_schema)

# 3. Aplica schema tabela
table_schema = """
    sku string, 
    seller_id string, 
    list_price decimal(10,2),
    timestamp_kafka long
"""
df_with_table = apply_table_schema(df_with_base, table_schema)

# 4. Parse timestamps
df_final = df_with_table.withColumn(
    "timestamp_kafka",
    parse_timestamp_in_micro("timestamp_kafka")
)

# 5. Escreve com checkpoint
write_to_lake_streaming_with_checkpoint(
    ss=ss,
    df=df_final,
    lake_environment='prd',
    zone='trusted',
    namespace='visao_unica',
    dataset='base_prices',
    repartition=20,
    partition_by=['timestamp_kafka_day']
)
```

### Exemplo 2: Qualidade de Dados Multi-Zone
```python
from visao_unica_commons.etls.utils.utils import data_quality

# Compara RAW → TRUSTED → REFINED
data_quality(
    ss=ss,
    environment='prd',
    zones=['raw', 'trusted', 'refined'],
    namespace='visao_unica',
    dataset='base_prices',
    date_process='2025-01-15'
)

# Se houver divergências, notifica Google Chat:
# "Diferenças de registros detectadas na ingestão kafka:
#  Dataset: `base_prices` | Namespace: `visao_unica` | Data: `2025-01-15`
#  - raw (1000000) - trusted (999950) | Diferença: 50
#  - trusted (999950) - refined (999900) | Diferença: 50"
```

### Exemplo 3: Checkpoint Incremental
```python
from visao_unica_commons.etls.utils.utils import (
    get_checkpoint,
    set_last_execution_date
)

# 1. Lê checkpoint (ou usa D-7 se não existir)
last_exec = get_checkpoint(
    ss=ss,
    environment='prd',
    zone='refined',
    namespace='visao_unica',
    dataset_name='prices',
    days_if_missing=7
)

print(f"Processando desde: {last_exec}")

# 2. Processa dados incrementais
df = (
    read_visao_unica_dataset(ss, 'prd', 'trusted', 'base_prices')
    .filter(col("timestamp_kafka") > last_exec)
)

# 3. Salva novo checkpoint
set_last_execution_date(
    ss=ss,
    last_exec_date=datetime.now(),
    lake_environment='prd',
    zone='refined',
    namespace='visao_unica',
    dataset_name='prices'
)
```

---

## 📚 Referências

### Código Fonte
- **Arquivo Principal:** `visao_unica_commons/etls/utils/utils.py`
- **Testes:** `tests/utils/test_utils.py`

### Pipelines que Usam
- `notificacao_preco` - Change detection (get_changed_rows, save_penultimate_execution)
- `notificacao_campanha_percentual` - Deduplicação (selling_prices_dedup, base_prices_dedup)
- `visao_preco_carga_historica` - Delta Lake (config_spark, write_to_lake_with_replace_where)
- `visao_unica_att_preco_de` - Parsing timestamps (parse_timestamp_in_micro)
- `visao_unica_preco` - Qualidade (data_quality)
- `visao_unica_melhor_oferta` - Skew handling (join_solve_imbalancing)

### Funções por Categoria

| Categoria | Funções |
|-----------|---------|
| **Configuração** | `config_spark()` |
| **Parsing Args** | `parse_args()`, `parse_args_with_database()`, `parse_data_command_arg()` |
| **Leitura** | `read_visao_unica_dataset()`, `get_catalog()`, `get_catalog_active_and_in_stock()`, `get_dataframe_in_mongo()`, `read_streaming_dataset()` |
| **Escrita** | `write_to_lake_overwrite_mode()`, `write_to_lake_with_replace_where()`, `write_to_lake_streaming_with_checkpoint()` |
| **Kafka** | `get_messages_from_kafka()`, `apply_base_message_schema()`, `apply_table_schema()`, `apply_kafka_key_scheme()` |
| **Timestamps** | `parse_timestamp_in_micro()`, `parse_timestamp_in_milli()` |
| **Deduplicação** | `selling_prices_dedup()`, `base_prices_dedup()` |
| **Change Detection** | `get_changed_rows()`, `save_penultimate_execution()` |
| **Skew Handling** | `add_skew_column()`, `add_skew_array()`, `join_solve_imbalancing()` |
| **Checkpoint** | `get_checkpoint()`, `get_last_execution_date()`, `set_last_execution_date()` |
| **Batching** | `get_days_to_process()` |
| **Qualidade** | `data_quality()`, `send_google_chat_message()` |

---

**Última Atualização:** Novembro 2025  
**Versão da Documentação:** 2.0  
**Maintainer:** Squad AI Growth  
**Arquivo Fonte:** `visao_unica_commons/etls/utils/utils.py` (único arquivo - 700+ linhas)