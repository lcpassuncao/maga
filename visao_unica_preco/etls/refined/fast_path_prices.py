"""
Trilha incremental de precos.

Le os topicos CDC de preco direto do Kafka e compoe preco apenas das
ofertas tocadas, gravando em refined.visao_unica.prices_fast. Nao le nem
escreve nada que o pipeline horario use, e nao altera nenhum DAG dele.

A regra de negocio e integralmente compartilhada com o batch:
dedup_base_prices, select_valid_selling_prices e compose_prices vem de
etls/trusted/visao_precos.py. O que existe aqui e o parsing do evento e o
escopo -- nunca calculo de preco. Duplicar calculo e como as duas trilhas
divergem em producao sem ninguem perceber.
"""

import os
from sness.datalake.sness_spark import SnessSpark
from dataclasses import dataclass
from typing import Callable, Optional, Sequence

import constants_fast as constants
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as sf
from sness.datalake.metadata import Zone
from utils import (
    apply_base_message_schema,
    apply_kafka_key_scheme,
    apply_table_schema,
    config_spark,
    parse_args,
    parse_timestamp_in_micro,
)

# As funcoes de regra de negocio sao importadas em nivel de modulo porque
# os defaults de _process_window referenciam-nas: defaults de argumento sao
# avaliados no momento da definicao da funcao. A regra de negocio continua
# vivendo em visao_precos.py -- aqui so ha parsing, escopo e encadeamento.
from visao_precos import (  # noqa: E402
    PAYMENT_METHODS,
    compose_prices,
    get_arcoiro_campaigns,
    get_arcoiro_discounts,
    get_catalog_remove_inactive,
    get_product_discounts,
    get_robinhood_campaigns,
    dedup_base_prices,
    select_valid_selling_prices,
)

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
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 100000)
        .option("failOnDataLoss", "false")
        .load()
    )

    return df_read_kafka

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
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )

    return df_read_kafka

def _parse_envelope(df: DataFrame, table_schema: str) -> DataFrame:
    """Desembrulha o envelope Debezium ate o documento Mongo.

    Devolve as colunas ``values`` (o documento), ``op``, ``timestamp_kafka``
    e ``obj_id_from_kafka_key``, que e a forma em que os pipelines fry_*
    entregam para a camada raw.
    """
    df = apply_base_message_schema(df, constants.MESSAGE_SCHEMA)
    df = apply_table_schema(df, table_schema)
    df = apply_kafka_key_scheme(df, constants.KAFKA_KEY_SCHEMA)

    return df


def parse_base_price_events(df: DataFrame) -> DataFrame:
    """Evento de fry.fry.base_prices no formato que dedup_base_prices espera.

    Nenhuma coluna consumida por dedup_base_prices passa por conversao de
    tipo entre raw e trusted, entao aqui basta desembrulhar e selecionar.
    """
    df = _parse_envelope(df, constants.BASE_PRICES_SCHEMA)

    df = df.select(
        sf.col("values.seller_id").alias("seller_id"),
        sf.col("values.sku").alias("sku"),
        sf.col("values.list_price").alias("list_price"),
        "timestamp_kafka",
        sf.col("values.selections").alias("selections"),
        sf.col("values.currency").alias("currency"),
    )

    # Delete traz payload.after nulo: sem chave de oferta a linha nao serve
    # para nada e so poluiria o dedup.
    return df.filter(
        sf.col("seller_id").isNotNull() & sf.col("sku").isNotNull()
    )


def resolve_base_prices(events: DataFrame, current: DataFrame) -> DataFrame:
    """Estado corrente do preco_de unindo a trusted com os eventos da janela.

    Ambos os frames chegam no contrato de parse_base_price_events (com
    ``list_price``); o dedup compartilhado fica com o maior timestamp_kafka,
    entao uma oferta que so mudou o preco de venda mantem o preco_de vindo
    da trusted em vez de perde-lo.
    """

    return dedup_base_prices(events.unionByName(current))


def resolve_selling_prices(events: DataFrame, current: DataFrame) -> DataFrame:
    """Estado corrente do preco_por unindo a trusted com os eventos da
    janela e aplicando a regra de validade compartilhada.

    Ambos os frames chegam no contrato de parse_selling_price_events (com
    ``price``, ``start_at``/``end_at`` como timestamp e ``_id`` como struct
    com campo ``$oid``); a regra de validade (current_timestamp entre
    start_at e end_at), o filtro de canal/campanha e o rankeamento de
    origem ficam todos em select_valid_selling_prices, compartilhada com o
    batch. Nenhuma regra de preco e duplicada aqui.
    """    

    return select_valid_selling_prices(events.unionByName(current))


OFFER_KEY = ["seller_id", "sku"]


def changed_offer_keys(*frames: DataFrame) -> DataFrame:
    """Chaves distintas das ofertas tocadas na janela.

    Aceita os frames dos dois topicos porque uma oferta pode ter mudado o
    preco de referencia, o preco de venda, ou os dois. Qualquer um dos
    casos coloca a oferta no escopo do recalculo.
    """
    chaves = None
    for frame in frames:
        selecionado = frame.select(*OFFER_KEY)
        chaves = selecionado if chaves is None else chaves.union(selecionado)

    return chaves.distinct()


def build_scoped_prices(
    catalog: DataFrame,
    base_prices: DataFrame,
    selling_prices: DataFrame,
    changed_keys: DataFrame,
) -> DataFrame:
    """O ``prices`` do batch, restrito as ofertas tocadas.

    Reproduz a montagem de visao_precos.main -- catalogo com base_prices e
    depois com selling_prices, ambos por ``(seller_id, sku)`` -- com o
    escopo aplicado no catalogo antes dos joins de preco, para a reducao
    acontecer no comeco e nao no fim.

    Atencao ao contrato de ``base_prices`` e ``selling_prices``: precisam
    trazer o **estado corrente** da oferta, nao apenas os eventos da
    janela. Se a oferta mudou so o preco de venda, nao existe evento de
    base_price para ela, e passar apenas os eventos deixaria preco_de
    nulo -- o update_base_price entao igualaria preco_de a preco_por e a
    oferta apareceria sem preco de referencia, sem erro nenhum. Quem chama
    e responsavel por unir o estado da trusted com os eventos novos.
    """
    catalogo_escopado = catalog.join(
        sf.broadcast(changed_keys.select(*OFFER_KEY).distinct()),
        on=OFFER_KEY,
        how="inner",
    )

    prices = catalogo_escopado.join(base_prices, on=OFFER_KEY, how="left")
    prices = prices.join(selling_prices, on=OFFER_KEY, how="left")

    return prices


def parse_selling_price_events(df: DataFrame) -> DataFrame:
    """Evento de fry.fry.selling_prices2 no formato que
    select_valid_selling_prices espera.

    Aplica a conversao de epoch em microssegundos para timestamp que o
    trusted_selling_prices2.treat_to_trusted faz antes de gravar. Sem ela o
    filtro de validade compara current_timestamp contra numero cru e
    devolve preco errado silenciosamente.
    """
    df = _parse_envelope(df, constants.SELLING_PRICES_SCHEMA)

    df = df.select(
        "values.*",
        "timestamp_kafka",
        "op",
    )

    for coluna in constants.SELLING_PRICES_TIMESTAMP_COLUMNS:
        df = df.withColumn(coluna, parse_timestamp_in_micro(coluna))

    return df.filter(
        sf.col("seller_id").isNotNull() & sf.col("sku").isNotNull()
    )


def _scope_trusted_by_keys(
    df: DataFrame, changed_keys: DataFrame
) -> DataFrame:
    """Filtra a trusted (semijoin) pelas chaves tocadas.

    O filtro acontece cedo, nao no fim: so as ofertas cuja
    ``(seller_id, sku)`` esta em ``changed_keys`` passam para frente.
    Deduplica as chaves antes do join para nao inflar as linhas quando a
    mesma oferta aparece mais de uma vez na janela.
    """
    return df.join(
        sf.broadcast(changed_keys.select(*OFFER_KEY).distinct()),
        on=OFFER_KEY,
        how="left_semi",
    )


def _scope_support_by_seller(
    df: DataFrame, seller_col: str, changed_sellers: DataFrame
) -> DataFrame:
    """Restringe um frame de apoio (descontos/campanhas) aos sellers tocados.

    Todos os joins de compose_prices casam por seller, entao um seller fora
    do escopo nunca contribuiria. O broadcast semijoin encolhe o lado grande
    antes do shuffle sem alterar o resultado do compose.
    """
    return df.join(
        sf.broadcast(changed_sellers),
        df[seller_col] == changed_sellers.seller_id,
        how="left_semi",
    )


def read_current_base_prices(
    ss, changed_keys: DataFrame, environment
) -> DataFrame:
    """Estado corrente de base_prices restrito as ofertas tocadas.

    Le a trusted ``visao_unica.base_prices`` (mesmo dataset que o batch usa
    em get_base_prices) e aplica o semijoin pelas chaves. Devolve o
    contrato de parse_base_price_events (com ``list_price``), que e o que
    dedup_base_prices consome -- o dedup fica por conta do resolve.

    O ``environment`` e passado explicitamente porque o SnessSpark nao o
    guarda; o ss.read.parquet exige como argumento. Quem chama
    (``_process_window``) recebe o environment do DAG.
    """
    

    df: DataFrame = ss.read.parquet(
        environment=environment,
        zone=Zone.TRUSTED,
        namespace="visao_unica",
        dataset="base_prices",
    )

    return _scope_trusted_by_keys(df, changed_keys)


def read_current_selling_prices(
    ss, changed_keys: DataFrame, environment
) -> DataFrame:
    """Estado corrente de selling_prices2 restrito as ofertas tocadas.

    Le a trusted ``visao_unica.selling_prices2`` (mesmo dataset que o batch
    usa em get_selling_prices) e aplica o semijoin pelas chaves. Devolve o
    contrato de parse_selling_price_events, que e o que
    select_valid_selling_prices consome -- a validade fica por conta do
    resolve.
    """

    df: DataFrame = ss.read.parquet(
        environment=environment,
        zone=Zone.TRUSTED,
        namespace="visao_unica",
        dataset="selling_prices2",
    )

    return _scope_trusted_by_keys(df, changed_keys)


def merge_prices_fast(
    ss, df: DataFrame, environment, table_path: Optional[str] = None
) -> None:
    """MERGE (upsert) em refined.visao_unica.prices_fast por (seller_id, sku).

    Na primeira execucao (path Delta inexistente) cria a tabela com
    ``write.format("delta").mode("overwrite")`` particionada por
    ``price_date``. Nas seguintes aplica o MERGE nativo do Delta Lake com
    ``whenMatchedUpdateAll`` + ``whenNotMatchedInsertAll``.

    A chave do MERGE e ``(seller_id, sku)`` -- a mesma de OFFER_KEY -- e nao
    inclui ``price_date``: a particao e apenas organizacional, a identidade
    da oferta e a chave composta. Usa ``<=>`` para tratar nulo como igual a
    nulo, seguindo o precedente de applying_prices.merge_base.

    O ``table_path`` e injetavel para os testes gravarem em path local
    (tmp_path); em producao o default e o GCS bucket da zona refined.

    Args:
        ss: SnessSpark (ou SparkSession) com suporte a Delta.
        df: DataFrame no contrato de compose_prices (chave seller_id+sku e
            particao price_date obrigatorias).
        environment: ambiente (ex.: ``stg``, ``prd``) que nomeia o bucket.
        table_path: path da tabela Delta. Se omitido, deriva do environment.
    """
    if table_path is None:
        table_path = f"gs://{environment}-lake-refined-visao_unica/prices_fast"

    # Aceita tanto um SnessSpark (com atributo .spark) quanto uma
    # SparkSession direta (caso dos testes, que passam a fixture spark).
    spark = ss.spark if hasattr(ss, "spark") else ss

    if not DeltaTable.isDeltaTable(spark, table_path):
        # Primeira execucao: cria a tabela particionada por price_date.
        df.write.format("delta").mode("overwrite").partitionBy(
            "price_date"
        ).save(table_path)
        return

    merge_keys = (
        "target.seller_id <=> source.seller_id "
        "AND target.sku <=> source.sku "
        "AND target.campaign_code <=> source.campaign_code"
    )
    delta_table = DeltaTable.forPath(spark, table_path)
    delta_table.alias("target").merge(
        df.alias("source"), merge_keys
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def append_prices_fast(
    ss, df: DataFrame, environment, table_path: Optional[str] = None
) -> None:
    """Append em refined.visao_unica.prices_fast por (seller_id, sku).

    Na primeira execucao (path Delta inexistente) cria a tabela com
    ``write.format("delta").mode("overwrite")`` particionada por
    ``price_date``. Nas seguintes faz um append puro com
    ``write.format("delta").mode("append")``, preservando o historico
    completo de cada execucao -- ao contrario de merge_prices_fast, que
    faz upsert por ``(seller_id, sku)`` e mantem apenas o estado corrente.

    A particao por ``price_date`` e apenas organizacional: a identidade da
    oferta e a chave composta ``(seller_id, sku)``, mas aqui nao ha
    deduplicacao -- cada micro-lote grava suas linhas como novas.

    O ``table_path`` e injetavel para os testes gravarem em path local
    (tmp_path); em producao o default e o GCS bucket da zona refined.

    Args:
        ss: SnessSpark (ou SparkSession) com suporte a Delta.
        df: DataFrame no contrato de compose_prices (chave seller_id+sku e
            particao price_date obrigatorias).
        environment: ambiente (ex.: ``stg``, ``prd``) que nomeia o bucket.
        table_path: path da tabela Delta. Se omitido, deriva do environment.
    """
    if table_path is None:
        table_path = f"gs://{environment}-lake-refined-visao_unica/prices_fast"

    # Aceita tanto um SnessSpark (com atributo .spark) quanto uma
    # SparkSession direta (caso dos testes, que passam a fixture spark).
    spark = ss.spark if hasattr(ss, "spark") else ss

    if not DeltaTable.isDeltaTable(spark, table_path):
        # Primeira execucao: cria a tabela particionada por price_date.
        df.write.format("delta").mode("overwrite").partitionBy(
            "price_date"
        ).save(table_path)
        return

    # Execucoes seguintes: append puro, sem upsert.
    df.write.format("delta").mode("append").save(table_path)


# Fator de replicacao usado no anti-skew dos joins de compose_prices. O
# batch usa 100 porque processa o catalogo inteiro; aqui cada micro-lote
# toca poucas ofertas, entao replicar o lado grande (descontos/campanhas)
# x100 so multiplica shuffle sem ganho de balanceamento. 1 desliga o
# anti-skew nesta trilha, mantendo a regra de negocio compartilhada.
SKEW_MULTIPLY = 1


@dataclass(frozen=True)
class WindowReaders:
    """Leituras injetaveis do micro-lote.

    Agrupa as dependencias de leitura de _process_window para manter a
    assinatura pequena (as leituras sao 7; empacota-las num objeto evita
    uma funcao com parametros demais) e para o teste injetar fakes em
    memoria sem GCS. Cada campo e um callable que devolve o DataFrame; os
    defaults apontam para as funcoes reais de visao_precos.py.
    """

    read_base: Callable[..., DataFrame] = read_current_base_prices
    read_selling: Callable[..., DataFrame] = read_current_selling_prices
    read_catalog: Callable[..., DataFrame] = get_catalog_remove_inactive
    read_discounts: Callable[..., DataFrame] = get_arcoiro_discounts
    read_product_discounts: Callable[..., DataFrame] = get_product_discounts
    read_robinhood: Callable[..., DataFrame] = get_robinhood_campaigns
    read_campaigns: Callable[..., DataFrame] = get_arcoiro_campaigns


def _process_window(
    ss,
    base_events: DataFrame,
    selling_events: DataFrame,
    environment,
    *,
    readers: WindowReaders = WindowReaders(),
    compose=compose_prices,
    write=merge_prices_fast,
    skew_multiply: int = SKEW_MULTIPLY,
) -> None:
    """Orquestra o processamento de um micro-lote (janela).

    Encadeia, para as ofertas tocadas na janela: escopo pelas chaves dos
    eventos, leitura da trusted escopada, resolucao do estado corrente
    (dedup/validade compartilhados), montagem do ``prices`` escopado e
    composicao + escrita em prices_fast.

    A regra de negocio nao e duplicada aqui: resolve_* e compose delegam
    para dedup_base_prices / select_valid_selling_prices / compose_prices
    de visao_precos.py, as mesmas funcoes que o batch horario usa. O que
    existe nesta funcao e apenas parsing, escopo e encadeamento.

    As leituras de trusted/catalogo/descontos/campanhas e a escrita sao
    injetaveis (defaults apontam para as funcoes reais) para o micro-lote
    ser testavel em memoria sem GCS: o teste injeta fakes em ``readers``
    que devolvem DataFrames locais e um fake ``write`` que captura o que
    seria gravado.

    Args:
        ss: SnessSpark (ou SparkSession) com suporte a Delta.
        base_events: eventos de base_price da janela (contrato de
            parse_base_price_events).
        selling_events: eventos de selling_prices2 da janela (contrato de
            parse_selling_price_events).
        environment: ambiente (ex.: ``stg``, ``prd``) que nomeia o bucket.
        readers: leituras de trusted/catalogo/descontos/campanhas.
        compose: composicao de preco (default: compose_prices compartilhado).
        write: escrita em prices_fast (default: merge_prices_fast).
        skew_multiply: fator de anti-skew repassado ao compose_prices.
    """
    # 1. Escopo: as chaves das ofertas tocadas na janela (base e/ou selling).
    changed_keys = changed_offer_keys(base_events, selling_events)

    # 2. Estado corrente da trusted restrito as ofertas tocadas.
    current_base = readers.read_base(ss, changed_keys, environment)
    current_selling = readers.read_selling(ss, changed_keys, environment)

    col_base_events = set(base_events.columns)
    col_selling_events = set(selling_events.columns)

    # 3. Resolucao do estado corrente (dedup/validade compartilhados).
    resolved_base = resolve_base_prices(
        base_events, current_base.select(*col_base_events)
    )
    resolved_selling = resolve_selling_prices(
        selling_events, current_selling.select(*col_selling_events)
    )

    # 4. Dados de apoio que alimentam a composicao, restritos as chaves/
    # sellers tocados. O escopo e passado cedo (scope_keys/scope_sellers),
    # antes das windows de dedup dentro de cada leitura -- ler e deduplicar
    # o catalogo/descontos/campanhas inteiros a cada micro-lote e o custo
    # fixo que motivou esta mudanca. _scope_support_by_seller continua como
    # segunda camada de seguranca (barata agora que o volume ja entra
    # pequeno), nao como o unico ponto de corte.
    changed_sellers = changed_keys.select("seller_id").distinct()

    catalog = readers.read_catalog(ss, environment, scope_keys=changed_keys)
    discounts = readers.read_discounts(
        ss, environment, scope_sellers=changed_sellers
    )
    discounts = discounts.union(
        readers.read_product_discounts(ss, environment, discounts)
    )
    discounts = _scope_support_by_seller(discounts, "seller", changed_sellers)
    campanha_clienteouro = _scope_support_by_seller(
        readers.read_robinhood(
            ss, environment, rename=True, scope_sellers=changed_sellers
        ),
        "seller_id_ouro",
        changed_sellers,
    )
    campanha_prazo = _scope_support_by_seller(
        readers.read_campaigns(
            ss,
            environment,
            payment_methods=PAYMENT_METHODS,
            scope_sellers=changed_sellers,
        ),
        "seller",
        changed_sellers,
    )

    # 5. Montagem do prices escopado (catalogo + precos resolvidos).
    scoped = build_scoped_prices(
        catalog, resolved_base, resolved_selling, changed_keys
    )

    # 6. Composicao compartilhada com o batch, restrita as ofertas tocadas.
    composed = compose(
        prices=scoped,
        discounts=discounts,
        campanha_clienteouro=campanha_clienteouro,
        campanha_prazo=campanha_prazo,
        skew_multiply=skew_multiply,
        payment_methods=PAYMENT_METHODS,
    )

    # 7. Escrita em prices_fast.
    write(ss, composed, "stg")

def optimize(ss: SnessSpark) -> None:
    """Otimizacao de refined.visao_unica.prices_fast.

    Executa o comando Delta OPTIMIZE para compactar os arquivos pequenos
    gerados pelos micro-lotes. O OPTIMIZE e agendado como tarefa separada
    do DAG horario, para nao atrasar a trilha rapida de precos.
    """
    table_path = "gs://prd-lake-trusted-visao_unica/base_prices"
    print(f"Otimizando tabela: {table_path}")
    delta_table = DeltaTable.forPath(ss.spark, table_path)
    delta_table.optimize().executeCompaction()

    delta_table.vacuum(retentionHours=48)
    print(f"Concluído: {table_path}\n")


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Fia o Structured Streaming da trilha rapida de precos.

    Abre um unico readStream sobre os dois topicos CDC (base_prices e
    selling_prices2) com ``subscribe`` em lista separada por virgula, e
    dispara ``foreachBatch`` chamando ``_process_window`` com
    ``trigger(availableNow=True)``.

    Decisao de desenho (uma stream so): os dois topicos tem schemas
    diferentes, entao nao podem ser unidos num unico frame antes do
    parsing; mas, como ambos seguem a mesma cadencia de processamento
    (5 min) e o mesmo ``trigger(availableNow=True)``, subscrever os dois
    num unico readStream garante que as micro-batches de base e selling do
    MESMO batch_id cheguem juntas ao callback e sejam orquestradas juntas
    por uma unica chamada a ``_process_window``. Foi essa a alternativa que
    preservou o cenario-armadilha (oferta tocada so por selling) que uma
    stream por topico com callback independente quebraria. Dentro do
    callback, cada frame e filtrado por ``topic`` ANTES do parsing
    (``apply_base_message_schema`` descarta a coluna ``topic``).

    Nenhuma regra de negocio e duplicada aqui: o ``main`` so faz parse de
    args, config do Spark, leitura/parsing das streams e encadeamento do
    ``foreachBatch`` para o ``_process_window`` existente.
    """
    args = parse_args(argv)
    ss = config_spark()

    # O AQE nao vale aqui: dentro de foreachBatch o Spark desliga
    # spark.sql.adaptive.enabled em query de streaming (ResolveWriteToStream).
    # Como cada micro-lote toca poucas ofertas, fixamos um numero baixo de
    # particoes de shuffle para nao gerar centenas de tasks vazias por join.
    # ss.spark.conf.set("spark.sql.shuffle.partitions", "64")

    TARGET_FILE_SIZE_MB = 6
    PARTITION_COLUMN = "timestamp_kafka_day"
    path = "gs://prd-lake-trusted-visao_unica/base_prices"

    print(f"Compactando arquivos pequenos em {path} para {TARGET_FILE_SIZE_MB} MB")

    detail = ss.spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0]
    total_size_mb = detail["sizeInBytes"] / (1024 ** 2)
    num_partitions = max(1, int(total_size_mb / TARGET_FILE_SIZE_MB))

    (
        ss.spark.read.format("delta").load(path)
        .repartition(num_partitions)          # espalha os dados de TODOS os dias entre N tasks
        .write.format("delta")
        .mode("overwrite")
        .partitionBy(PARTITION_COLUMN)         # reafirma o esquema de particionamento explicitamente
        .option("dataChange", "false")
        .save(path)
    )

    print(f"Fim da compactacao de arquivos pequenos em {path} para {TARGET_FILE_SIZE_MB} MB")


    # optimize(ss)

    # env = args.environment.value  # "stg" | "prd"

    # topics = f"{constants.TOPIC_BASE_PRICES},{constants.TOPIC_SELLING_PRICES}"
    # raw = get_messages_from_kafka_mgc(ss, topics)

    # def process_batch(raw_batch: DataFrame, batch_id: int) -> None:
    #     # Filtra por topico ANTES do parsing: o envelope Debezium descarta a
    #     # coluna ``topic`` do frame kafka, entao o filtro precisa vir antes.
    #     base_events = parse_base_price_events(
    #         raw_batch.filter(sf.col("topic") == constants.TOPIC_BASE_PRICES)
    #     )
    #     selling_events = parse_selling_price_events(
    #         raw_batch.filter(sf.col("topic") == constants.TOPIC_SELLING_PRICES)
    #     )
    #     _process_window(ss, base_events, selling_events, env)

    # env_stg = 'stg'

    # checkpoint = f"gs://{env_stg}-lake-refined-visao_unica/checkpoint/prices_fast"

    # query = (
    #     raw.writeStream.foreachBatch(process_batch)
    #     .trigger(availableNow=True)
    #     .outputMode("append")
    #     .option("checkpointLocation", checkpoint)
    #     .start()
    # )
    # query.awaitTermination()

    ss.spark.stop()

    return 0


if __name__ == "__main__":
    main()
