"""Util functions related to visao_unica project."""

import argparse
import json
import logging
from datetime import datetime, timedelta
from functools import reduce
from typing import TYPE_CHECKING, Any, List, Optional, Sequence, Tuple

import pyspark.sql.functions as sf
import pyspark.sql.types as st
import requests
from pyspark import StorageLevel
from pyspark.sql import DataFrame, Row, SparkSession, Window
from sness.datalake.metadata import Environment, Zone
from sness.datalake.sness_spark import SnessSpark

if TYPE_CHECKING:
    from delta.tables import DeltaTable

DAY_ISO_FORMAT = "%Y-%m-%d"
logger = logging.getLogger()


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


def get_days_to_process(
    start_date: datetime,
    end_date: Optional[datetime] = datetime.now(),
    n_days_to_process_each_epoch: int = 5,
) -> Sequence[Tuple[datetime, datetime]]:
    """Build the list of days to be processed divided by days.

    Args:
        start_date:  the day to start the etl.
        end_date: the day to end the etl.
        n_days_to_process_each_epoch: the number of days to group periods.

    Returns:
        A list of day periods to process the etl.

    """
    # it alway read one day before the checkpoint
    start_date -= timedelta(days=1)

    days = [
        start_date + timedelta(days=i)
        for i in range(0, (end_date - start_date).days + 1)
    ]

    # Reparte a lista de partições de x em x, de acordo com a qtd_partitions definidas
    # copiado do script do Kana
    def split_list(
        lista: Sequence[datetime], qtd_itens: int
    ) -> Sequence[Tuple[datetime, datetime]]:
        return [
            lista[i : i + qtd_itens] for i in range(0, len(lista), qtd_itens)
        ]

    lista_share = split_list(days, n_days_to_process_each_epoch)

    return lista_share


def parse_data_command_arg(date_str: str) -> datetime:
    """Parse date that whe receive in the command line interface."""
    date = datetime.strptime(date_str, DAY_ISO_FORMAT)

    return date


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse arguments from command line."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--environment", type=Environment, required=False, default="stg"
    )

    parser.add_argument(
        "--n_repartitions", type=int, required=False, default=20
    )

    msg_date = "If specified sets the initial {type} to process: pass the date in the format YYYY-MM-DD"
    parser.add_argument(
        "--start_date",
        type=parse_data_command_arg,
        required=False,
        default=None,
        help=msg_date.format(type="start date"),
    )
    parser.add_argument(
        "--end_date",
        type=parse_data_command_arg,
        required=False,
        default=None,
        help=msg_date.format(type="end date"),
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
    parser.add_argument(
        "--webhook_url",
        type=str,
        required=False,
        default=None,
    )

    args = parser.parse_args(argv)

    return args


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


def set_last_execution_date(
    ss: SnessSpark,
    last_exec_date: datetime,
    lake_environment: str,
    zone: str,
    namespace: str,
    dataset_name: str,
) -> None:
    """Set the checkpoint date of a dataset in the datalake."""
    # Right way to set the timestamp of the last execution

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


def get_checkpoint(
    ss: SnessSpark,
    environment: Environment,
    zone: Zone,
    namespace: str,
    dataset_name: str,
    days_if_missing: int = 1,
) -> datetime:
    """Get the checkpoint date, if its not set, set the number of days behind to use.

    Args:
        ss : Instance of SnessSpark.
        environment : The environment to save the checkpoint.
        zone : Zone to save the checkpoint.
        namespace : name of the namespace to save the dataset.
        dataset_name : name of the dataset to save the checkpoint.
        days_if_missing: the number of days behind to use if not found the dataset checkpoint.

    Returns:
        The checkpoint date.

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


def get_messages_from_kafka(ss: SnessSpark, topic_name: str) -> DataFrame:
    """Get messages from a topic in Kafka.

    Args:
        ss: instance of SnessSpark.
        topic_name: the name of the topic to be fetched.

    Returns:
        A dataframe containing messages of the topic since the last read.

    """
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


def get_messages_from_kafka_mgc(ss: SnessSpark, topic_name: str) -> DataFrame:
    """Get messages from a topic in Kafka.

    Args:
        ss: instance of SnessSpark.
        topic_name: the name of the topic to be fetched.

    Returns:
        A dataframe containing messages of the topic since the last read.

    """
    df_read_kafka = (
        ss.spark.readStream.format("kafka")
        .option(
            "kafka.bootstrap.servers",
            "mgc-br-se1-data-engineering-ikafka-01.mglu.io:9092",
        )
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 100000)
        .option("failOnDataLoss", "false")
        .option("minPartitions", 64)
        .load()
    )

    return df_read_kafka


def apply_base_message_schema(df: DataFrame, schema: str) -> DataFrame:
    """Apply a schema in the raw message from kafka.

    The message from kafka comes with a specific format, to parse the
    message a schema must be applied, the first schema can be applied
    ussing this function.

    Args:
        df: the raw dataframe that comes from reading a topic in Kafka.
        schema : the schema to be applied in the value field of message.

    Returns:
        A  dataframe to be applied in the message.

    """
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


def apply_table_schema(df: DataFrame, table_schema: str) -> DataFrame:
    """Apply the schema of the mongodb database.

    The collection of mongo comes as a string in the kafka message.
    To parse the collection is needed to pass a schema.

    Args:
        df : the dataframe containing the message
        table_schema : the schema of the collection in mongo
    Returns:
        a dataframe representing a collection in mongo with the schema applied.

    """
    df = df.select(
        "kafka_key",
        "payload.op",
        sf.from_json("payload.after", table_schema).alias("values"),
        "timestamp_kafka",
    )

    # remove delete and null operations
    df = df.filter(sf.col("op").isin(["c", "u", "d"]))

    return df


def apply_kafka_key_scheme(df: DataFrame, schema: str):
    "This function parse the kafka key value to get the object id of the row"

    # tratando o schema do kafka key, para o caso de chaves que tenham o delete.
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


def write_to_lake_streaming_with_checkpoint_v2(
    df: DataFrame,
    lake_environment: str,
    zone: Zone,
    namespace: str,
    dataset: str,
    partition_by: Sequence[str] = None,
):
    """Escreve um DataFrame de streaming no lake em formato Delta.

    Função alternativa à write_to_lake_streaming_with_checkpoint que
    utiliza a escrita writeStream da sness_lib, pois na implementação
    da sness, o writer de streaming fixa internamente o trigger como
    trigger(once=True), o que impede esta pipeline de usar a opção
    trigger(availableNow=True), causando erro ao consumir tópicos com
    grandes volumes de dados.

    Para manter o mesmo objetivo operacional e, ao mesmo tempo, permitir o
    uso de 'availableNow', esta versão monta diretamente o caminho do
    dataset no GCS e o caminho de checkpoint, executando a escrita com a API
    nativa 'df.writeStream' do Spark e forçando sempre o formato Delta Lake.

    Args:
        df: DataFrame de streaming que será escrito.
        lake_environment: Prefixo do ambiente usado no nome do bucket.
        zone: Zona de destino no lake.
        namespace: Namespace de destino dentro do lake.
        dataset: Nome do dataset de destino dentro do bucket do namespace.
        partition_by: Colunas opcionais usadas para particionar os arquivos.
    """

    bucket_name = f"{lake_environment.value}-lake-{zone.value}-{namespace}"
    dataset_path = f"gs://{bucket_name}/{dataset}/"
    checkpoint_location = f"gs://{bucket_name}/checkpoint/{dataset}"

    print(f"writing streaming data to {dataset_path} (format=delta)")
    print(f"writing the streaming checkpoint to {checkpoint_location}")

    writer = (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .trigger(availableNow=True)
    )

    if partition_by:
        writer = writer.partitionBy(partition_by)

    query = writer.start(dataset_path)
    query.awaitTermination()


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


def parse_timestamp_in_micro(column: str) -> sf.col:
    """
    Parse a pyspark column that represents a timestamp in microseconds.

    Args:
        column: name of the column to be parsed
    Returns:
        a `pyspark.sql.functions.col` with type `TimestampType`

    """
    # matchs timeestamp 1969-12-31T21:00:00
    TIMESTAMP_PATTERN = r"[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])T(2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9]"
    # matches epoch pattern 1721115793
    EPOCH_PATTERN = r"\d*"
    # matchs strings {"$numberLong":"253402214400"}
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


def parse_timestamp_in_milli(column: str):
    """
    Parse a pyspark column that represents a timestamp in milliseconds.

    Args:
        column: name of the column to be parsed

    Returns:
        a `pyspark.sql.functions.col` with type `TimestampType`

    """
    col = sf.to_timestamp(
        sf.when(
            sf.col(column).isNull(), sf.lit(None).cast(st.TimestampType())
        ).otherwise(
            (sf.col(column) / 1000).cast(st.TimestampType()),
        )
    )

    return col


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


def parse_args_with_database(
    argv: Optional[Sequence[str]] = None,
) -> argparse.Namespace:
    """Parse arguments from command line."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--environment", type=Environment, required=False, default="stg"
    )

    parser.add_argument(
        "--n_repartitions", type=int, required=False, default=20
    )

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


def get_catalog_active_and_in_stock(ss: SnessSpark) -> DataFrame:
    df = ss.read.parquet(
        Environment.PRODUCTION,
        Zone.TRUSTED,
        "catalogo",
        "visao_catalogo_produto_atual",
    ).filter((sf.col("active")) & (sf.col("stock")))

    return df


def get_catalog(ss: SnessSpark) -> DataFrame:
    df = ss.read.parquet(
        Environment.PRODUCTION,
        Zone.TRUSTED,
        "catalogo",
        "visao_catalogo_produto_atual",
    )

    return df


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


def get_refined_path(
    environment: Environment, namespace: str, dataset: str
) -> str:
    """Monta o path GCS da zona refined de um dataset."""
    return f"gs://{environment.value}-lake-refined-{namespace}/{dataset}"


def apply_delta_merge(
    delta_table: "DeltaTable",
    df: DataFrame,
    merge_keys: str,
    update_condition: str,
    set_expr: dict,
) -> None:
    """Executa merge condicional num DeltaTable já resolvido.

    Função pura (path independente) — testável localmente com DeltaTable
    sobre diretório temporário. Aplica:
      - whenMatchedUpdate(condition=update_condition, set=set_expr)
      - whenNotMatchedInsert(values=source.*)

    Args:
        delta_table: instância de DeltaTable (target).
        df: DataFrame fonte (source), já deduplicado.
        merge_keys: condição de join target/source (usar '<=>' p/ nulos).
        update_condition: condição que dispara o UPDATE (mudança de valor).
        set_expr: dict de colunas a atualizar (inclui updated_at).
    """
    df = df.withColumn("checkpoint_timestamp", sf.current_timestamp())
    insert_expr = {col: f"source.{col}" for col in df.columns}

    (
        delta_table.alias("target")
        .merge(df.alias("source"), merge_keys)
        .whenMatchedUpdate(condition=update_condition, set=set_expr)
        .whenNotMatchedInsert(values=insert_expr)
        .execute()
    )


def merge_current_state(
    ss: SnessSpark,
    df: DataFrame,
    environment: Environment,
    zone: Zone,
    namespace: str,
    dataset: str,
    merge_keys: str,
    update_condition: str,
    set_expr: dict,
    partition_by: Sequence[str] = None,
):
    """Faz merge condicional de estado corrente numa tabela Delta da zona REFINED.

    Na primeira execução (tabela não existe) cria com escrita direta
    overwrite + coluna checkpoint_timestamp. Nas demais, delega a lógica
    de merge para `apply_delta_merge`.

    Args:
        ss: instância de SnessSpark.
        df: DataFrame com as linhas novas/atualizadas (já deduplicadas).
        environment: Environment (prd/stg).
        zone: Zone do lake (deve ser REFINED).
        namespace: namespace do lake (ex: 'visao_unica').
        dataset: nome do dataset refined (ex: 'base_prices_current').
        merge_keys: condição de join entre target e source (usar '<=>' p/ nulos).
        update_condition: condição que dispara o UPDATE (mudança de valor).
        set_expr: dict de colunas a atualizar (inclui updated_at = source.updated_at).
        partition_by: colunas de partição da tabela (opcional).
    """
    from delta.tables import DeltaTable

    path = get_refined_path(environment, namespace, dataset)

    if not DeltaTable.isDeltaTable(ss.spark, path):
        logger.warning(
            f"Tabela {dataset} não existe, criando na primeira execução: {path}"
        )
        df_initial = df.withColumn(
            "checkpoint_timestamp", sf.current_timestamp()
        )
        if partition_by:
            df_initial = df_initial.repartition(*partition_by)
        df_initial.write.format("delta").mode("overwrite").save(path)
        return

    delta_table = DeltaTable.forPath(ss.spark, path)
    apply_delta_merge(delta_table, df, merge_keys, update_condition, set_expr)


def load_changed_since_checkpoint(
    ss: SnessSpark,
    environment: Environment,
    zone: Zone,
    namespace: str,
    dataset: str,
    checkpoint_date,
    lookback_days: int = 1,
) -> DataFrame:
    """Lê as linhas alteradas (updated_at) de uma refined desde o checkpoint.

    O checkpoint é uma data; usa lookback para não perder linhas do dia
    corrente que tenham updated_at no mesmo dia do checkpoint.
    """
    start = checkpoint_date - timedelta(days=lookback_days)
    df = ss.read.parquet(environment, zone, namespace, dataset).filter(
        sf.col("checkpoint_timestamp") > start
    )

    return df


def get_changed_rows(
    fixed_fields,
    fields_to_compare,
    columns_to_join,
    actual_df,
    last_df,
):
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


def add_skew_column(
    df: DataFrame, skew_multiply: int, columns: List[str]
) -> DataFrame:
    """
    Adiciona uma coluna 'skew' ao DataFrame calculando o valor baseado
    em uma combinação de colunas especificadas e uma constante de multiplicação.
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


def add_skew_array(df: DataFrame, skew_multiply: int) -> DataFrame:
    """
    Adiciona uma coluna 'skew' ao DataFrame, criando um array com números de 0 até skew_multiply - 1
    e o explode para gerar múltiplas linhas para cada linha original.
    """
    return df.withColumn(
        "skew",
        sf.explode(sf.array([sf.lit(n) for n in range(0, skew_multiply)])),
    )


def join_solve_imbalancing(
    df_to_hash: DataFrame,
    df_to_explode: DataFrame,
    how: str,
    conditions: sf.Column,
    skew_multiply: int,
    columns_for_hashing: List[str],
) -> DataFrame:
    """
    Resolve o desequilíbrio de dados (skew) entre os clusters do PySpark,
    processando os dados desbalanceados e realizando um join balanceado.
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


def save_penultimate_execution(
    ss: SnessSpark,
    environment: Environment,
    zone: Zone,
    dataset_readen: str,
    dataset_written: str,
):
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


def selling_prices_dedup(df: DataFrame) -> DataFrame:
    """
    Deduplica e processa o DataFrame de preços de venda, mantendo apenas o registro mais recente por produto/seller e aplicando regras para identificar preços ativos, expirados ou deletados.

    Fluxo da função:
        - Seleciona o registro mais recente por "_id.$oid" (usando timestamp_kafka).
        - Se o registro está ativo (não deletado, dentro do período de validade), aplica ranking de prioridade de origem e seleciona o melhor preço.
        - Se o registro está expirado, verifica se existe campanha ativa para substituir o preço e marca como atualização ("u"), ou marca como deletado ("d") caso não haja campanha ativa.
        - Enriquece registros expirados com seller_id e sku do histórico, se necessário.
        - Une os resultados e retorna apenas as colunas essenciais para downstream.

    Parâmetros:
        df (DataFrame): DataFrame de preços de venda, incluindo informações de operação, validade, origem e histórico.

    Retorna:
        DataFrame: DataFrame deduplicado e processado, contendo apenas os preços válidos ou deletados, pronto para publicação ou análise.
    """

    # projetado antes do window para nao arrastar o payload CDC inteiro pelo shuffle/sort

    df = df.select(
        "_id",
        "timestamp_kafka",
        'updated_at',
        "deleted_at",
        "op",
        "start_at",
        "end_at",
        "origin",
        "price",
        "id",
        "seller_id",
        "sku",
        "channel_id",
        "campaign_code",
    )

    window_spec = Window.partitionBy("_id.$oid").orderBy(
        sf.desc("timestamp_kafka")
    )
    # persistido porque df_ativos/df_inativos derivam daqui e o window seria refeito a cada branch
    df_rank = (
        df.withColumn("rank_actions", sf.row_number().over(window_spec))
        .filter(sf.col("rank_actions") == 1)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # condicao utilizada para campanhas de preco ativas
    condicao_ativo = (
        (sf.col("deleted_at").isNull())
        & (sf.col('op') != 'd')
        & (
            sf.current_timestamp().between(
                sf.col("start_at"), sf.col("end_at")
            )
        )
    )

    # campanhas expiradas vão passar pela validação de campanha ativa
    df_inativos = df_rank.filter(
        (~condicao_ativo) & (sf.col("timestamp_kafka") >= "2025-09-10")
    )
    # campanhas ativas vão passar pelo rankeamento de preco pela origem
    df_ativos = df_rank.filter(condicao_ativo)

    dataframes_para_unir = []

    if df_ativos.take(1):
        ORIGIN_PRIORITY = [
            ("acme-admin-priority", 1),
            ("kratos", 2),
            ("acme-admin", 3),
            ("adm-site", 4),
            ("promo3p", 5),
            ("integra", 6),
            ("api", 7),
        ]

        origin_rank_col = sf.when(
            sf.col("origin") == ORIGIN_PRIORITY[0][0],
            sf.lit(ORIGIN_PRIORITY[0][1]),
        )
        for origin, priority in ORIGIN_PRIORITY[1:]:
            origin_rank_col = origin_rank_col.when(
                sf.col("origin") == origin, sf.lit(priority)
            )
        origin_rank_col = origin_rank_col.otherwise(99)

        df_ativos_com_prioridade = df_ativos.withColumn(
            "origin_priority", origin_rank_col
        )

        price_window_spec = Window.partitionBy("id").orderBy(
            sf.asc("origin_priority"), sf.asc("price")
        )

        df_melhor_preco_ativo = (
            df_ativos_com_prioridade.withColumn(
                "price_rank", sf.row_number().over(price_window_spec)
            )
            .filter(sf.col("price_rank") == 1)
            .withColumn("campanha_expirada", sf.lit(False))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        dataframes_para_unir.append(df_melhor_preco_ativo)

    # os registros expirados verificam se ha uma campanha ativa, se sim substitui o preco para o preco da campanha ativa e o op = "u"
    if df_inativos.take(1):

        df_inativos_processados = None
        # se existirem preços ativos, envia substituindo o preço para o preço da campanha ativa e a op para "u"
        if dataframes_para_unir:
            df_substitutos = dataframes_para_unir[0].select(
                sf.col("id"), sf.col("price").alias("preco_substituto")
            )
            df_inativos_com_join = (
                df_inativos.alias("inativo")
                .join(
                    df_substitutos.alias("sub"),
                    on=(sf.col("inativo.id") == sf.col("sub.id")),
                    how="left",
                )
                .select("inativo.*", "sub.preco_substituto")
            )
        else:
            # sem ativos, equivale ao left join sem match
            df_inativos_com_join = df_inativos.alias("inativo").select(
                "inativo.*",
                sf.lit(None)
                .cast(df_inativos.schema["price"].dataType)
                .alias("preco_substituto"),
            )

        df_atualizado = (
            df_inativos_com_join.withColumn(
                "op",
                sf.when(
                    sf.col("preco_substituto").isNotNull(), sf.lit("u")
                ).otherwise(sf.lit("d")),
            )
            .withColumn(
                "price",
                sf.when(
                    sf.col("preco_substituto").isNotNull(),
                    sf.col("preco_substituto"),
                ).otherwise(sf.col("inativo.price")),
            )
            .withColumn(
                "campanha_expirada",
                sf.when(
                    sf.col("preco_substituto").isNotNull(), sf.lit(True)
                ).otherwise(sf.lit(False)),
            )
        )

        df_inativos_processados = df_atualizado.select(
            *df_inativos.columns,
            "campanha_expirada",
        )

        df_inativos_sem_seller = df_inativos_processados.filter(
            sf.col("seller_id").isNull()
        )

        # se nao existe nenhum campanha ativa os inativos devem ser marcados com op = 'd'
        # verifica nos dados historicos e enriquece com o sku e o seller_id se o registro for uma deleção do banco
        # if df_inativos_sem_seller.take(1):
        if df_inativos.filter(sf.col("seller_id").isNull()).take(1):

            col_select = df_inativos_processados.columns
            ids_sem_seller = df_inativos_sem_seller.select(
                sf.col("_id.$oid").alias("id_alvo")
            ).distinct()

            df_dados_historicos = (
                df.filter(sf.col("seller_id").isNotNull())
                .join(
                    ids_sem_seller,
                    sf.col("_id.$oid") == ids_sem_seller["id_alvo"],
                    "left_semi",
                )
                .withColumn("rank_actions", sf.row_number().over(window_spec))
                .filter(sf.col("rank_actions") == 1)
                .select(
                    sf.col("_id.$oid").alias("_id_historico"),
                    sf.col("seller_id").alias("seller_historico"),
                    sf.col("sku").alias("sku_historico"),
                    sf.col("channel_id").alias("channel_id_historico"),
                    sf.col("campaign_code").alias("campaign_code_historico"),
                    sf.col("id").alias("id_historico"),
                )
            )

            df_inativos_enriquecidos = df_inativos_processados.filter(
                sf.col("seller_id").isNull()
            ).join(
                df_dados_historicos,
                df_inativos_sem_seller["_id.$oid"]
                == df_dados_historicos["_id_historico"],
                "left",
            )

            df_inativos_enriquecidos = (
                df_inativos_enriquecidos.withColumn(
                    "seller_id",
                    sf.coalesce(
                        sf.col("seller_id"), sf.col("seller_historico")
                    ),
                )
                .withColumn(
                    "sku", sf.coalesce(sf.col("sku"), sf.col("sku_historico"))
                )
                .withColumn(
                    "channel_id",
                    sf.coalesce(
                        sf.col("channel_id"), sf.col("channel_id_historico")
                    ),
                )
                .withColumn(
                    "campaign_code",
                    sf.coalesce(
                        sf.col("campaign_code"),
                        sf.col("campaign_code_historico"),
                    ),
                )
                .withColumn(
                    "id", sf.coalesce(sf.col("id"), sf.col("id_historico"))
                )
                .withColumn("op", sf.lit("d"))
                .withColumn("campanha_expirada", sf.lit(False))
                .select(col_select)
            )
            df_inativos_processados = df_inativos_processados.filter(
                sf.col("seller_id").isNotNull()
            ).unionByName(df_inativos_enriquecidos)

        dataframes_para_unir.append(df_inativos_processados)

    colunas_base = [
        '_id',
        'timestamp_kafka',
        'deleted_at',
        'op',
        'start_at',
        'end_at',
        'origin',
        'price',
        'id',
        'seller_id',
        'sku',
        'channel_id',
        'campaign_code',
        'updated_at',
        'rank_actions',
        'campanha_expirada',
    ]

    colunas_finais = [
        "seller_id",
        "sku",
        "channel_id",
        "campaign_code",
        "price",
        "op",
        "campanha_expirada",
        'updated_at',
        "timestamp_kafka",
        "is_deleted",
    ]

    if not dataframes_para_unir:
        return (
            df.withColumn("campanha_expirada", sf.lit(False))
            .select(*colunas_finais)
            .limit(0)
        )

    final_dfs = [df.select(*colunas_base) for df in dataframes_para_unir]

    df_resultado = reduce(DataFrame.unionByName, final_dfs)

    price_window_spec = Window.partitionBy("id").orderBy(
        sf.col("is_deleted").asc_nulls_last(), sf.desc("timestamp_kafka")
    )

    df_melhor_preco_com_prioridade = (
        df_resultado.withColumn("is_deleted", sf.col("op") == "d")
        .withColumn("price_rank", sf.row_number().over(price_window_spec))
        .filter(sf.col("price_rank") == 1)
    )

    return df_melhor_preco_com_prioridade.select(*colunas_finais)


def base_prices_dedup(df: DataFrame) -> DataFrame:
    """Deduplica base_prices mantendo o evento CDC mais recente por chave.

    Mantém a linha de maior ``timestamp_kafka`` para cada ``(seller_id,
    sku)`` e preserva o ``op`` do evento (c/u/d), necessário para o merge
    condicional em ``base_prices_current`` (derivar ``is_deleted``).
    """

    df = (
        df.select(
            'seller_id',
            'sku',
            'list_price',
            'updated_at',
            'timestamp_kafka',
            'selections',
            'currency',
            'op',
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

    return df.select(
        "seller_id",
        "sku",
        "list_price",
        "op",
        "updated_at",
        "timestamp_kafka",
        "selections",
        "currency",
    )


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


def data_quality(
    ss,
    environment,
    zones,
    namespace,
    dataset,
    date_process,
):
    """Compara a contagem de registros entre duas zonas de dados para um dataset específico
    e envia uma notificação para o Google Chat se houver divergência."""

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
