import logging
from datetime import datetime, timedelta
from typing import List, Optional, Sequence

import pyspark.sql.types as st
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as sf
from pyspark.sql.types import DecimalType
from sness.datalake.metadata import Environment, Zone
from sness.datalake.sness_spark import SnessSpark
from utils import (
    config_spark,
    get_catalog,
    get_changed_rows,
    join_solve_imbalancing,
    parse_args,
    read_visao_unica_dataset,
)

logger = logging.getLogger(__name__)


def get_base_prices(ss, environment: Environment) -> DataFrame:

    df: DataFrame = ss.read.parquet(
        environment=environment,
        zone=Zone.REFINED,
        namespace="visao_unica",
        dataset="base_prices_current",
    )

    df = df.filter(~sf.col("is_deleted")).select(
        "seller_id",
        "sku",
        "list_price",
        "timestamp_kafka",
        "selections",
        "currency",
    )

    df = df.withColumnRenamed("list_price", "preco_de")
    # Se o campo currency for nulo, substituir por BRL
    df = df.withColumn(
        "currency", sf.coalesce(sf.col("currency"), sf.lit("BRL"))
    )

    return df


def get_product_discounts(
    ss: SnessSpark, environment: Environment, discounts: DataFrame
) -> DataFrame:
    """a tabela product discounts contém campanhas de sellers que impactaram a
    perfomance quando estavam na tabela discounts.
    para adicionar a campanha desses sellers, a lista de skus são adicionadas
    nas respectivas campanhas da discounts.
    o layout da tabela é o mesmo.
    """

    product_discounts = ss.read.parquet(
        environment, "trusted", "visao_unica", "products_discounts"
    )

    product_discounts = (
        product_discounts.withColumn(
            "id_rank",
            sf.row_number().over(
                Window.partitionBy("_id.$oid").orderBy(
                    sf.desc("timestamp_kafka")
                )
            ),
        )
        .filter(sf.col("id_rank") == 1)
        .drop("id_rank")
    )

    # filtra campanhas ativas.
    product_discounts = product_discounts.filter(
        sf.current_timestamp() <= sf.col("campaign_end_date")
    )

    product_discounts = product_discounts.groupBy("campaign_id").agg(
        sf.collect_set("sku").alias("skus")
    )

    # adiciona  os descontos de produto
    res = product_discounts.join(
        discounts,
        how='inner',
        on=product_discounts["campaign_id"] == discounts["id_discounts"],
    )

    # une os skus que vem da products discounts
    # com os skus que já estavam na campanha referenciada.
    res = res.withColumn(
        "target",
        sf.col("target").withField(
            "include",
            sf.col("target.include").withField(
                "skus",
                sf.array_union(sf.col("target.include.skus"), sf.col("skus")),
            ),
        ),
    )
    res = res.drop("campaign_id", "skus")

    return res


def get_selling_prices(ss: SnessSpark, environment: Environment) -> DataFrame:
    """
    Algoritmo Preço Por sobre o estado corrente
    (refined ``selling_prices2_current``).

    A zona refined já resolveu a montante: dedup CDC por chave composta
    ``(seller_id, sku, channel_id, campaign_code)``, vencedor por origem e
    expiração temporal (expirados viram ``op='d'``/``campanha_expirada``).
    Resta aqui filtrar:
      1. Soft-deletes (``is_deleted``).
      2. Campanhas expiradas (``campanha_expirada``) — comportamento
         preservado do caminho trusted, que validava
         ``current_timestamp().between(start_at, end_at)``.
      3. Campanhas de parceiro (``0`` ou ``8349``) e canais ``*``.

    Expiradas e deletadas não entram no ``preco_por`` da base final, mas
    seguem visíveis na ``selling_prices2_current`` para a ``notificacao_preco``
    (ela lê a corrente diretamente, sem passar por esta função).

    ``campaign_code`` permanece na saída sem rank por ``(seller_id, sku)``:
    a corrente já tem uma linha por (seller, sku, campanha, canal) e colapsar
    por seller/sku eliminaria as campanhas 0 e 8349 do mesmo produto.
    """

    df = ss.read.parquet(
        environment=environment,
        zone=Zone.REFINED,
        namespace="visao_unica",
        dataset="selling_prices2_current",
    )

    # removendo linhas soft-deletadas e com campanha expirada: expiradas e
    # deletadas não entram no preco_por (comportamento preservado do caminho
    # trusted), mas seguem visíveis na _current para a notificacao_preco.
    df = df.filter(~sf.col("is_deleted") & ~sf.col("campanha_expirada"))

    # regra de parceiro zero | 8349 e todos os channels.
    df = df.filter(
        (sf.col("channel_id") == '*')
        & sf.col("campaign_code").isin("0", "8349")
    )

    df = df.drop("op")

    df = df.withColumnRenamed("price", "preco_por")

    return df


def filter_catalog_columns_to_price_and_distinct(df: DataFrame) -> DataFrame:
    """filter only columns that will be used in price etl"""

    columns = [
        "seller_id",
        "sku",
        "navigation_id",
        "brand",
        "category_id",
        "active",
        "stock",
    ]

    # Removendo produtos cujo category_id na base do catalogo sejam iguais a 'TM' para garantir que produtos temporarios nao sejam adicionados ao visao unica.
    df = df.select(columns).filter(sf.col("category_id") != "TM").distinct()

    return df


def get_arcoiro_discounts(
    ss: SnessSpark, environment: Environment
) -> DataFrame:

    df: DataFrame = ss.read.parquet(
        environment,
        zone=Zone.TRUSTED,
        namespace="visao_unica",
        dataset="discounts_v2",
    )

    df = df.select(
        [
            "_id",
            "op",
            "timestamp_kafka",
            "start_date",
            "end_date",
            "id",
            "installment_plans",
            "seller",
            "target",
            "payment_method",  # precisa do metodo de pagamento para diferenciar ex: pix, boleto..
        ]
    )

    # filtra as campanhas que já foram deletadas, caso o _id.$oid tiver sido deletado, ele deve ser retirado.
    df = (
        df.withColumn(
            "rank_actions",
            sf.row_number().over(
                Window.partitionBy("_id.$oid").orderBy(
                    sf.desc("timestamp_kafka")
                )
            ),
        )
        .filter(sf.col("rank_actions") == 1)
        .drop("rank_actions")
    )

    df = df.filter(sf.col("op") != 'd')

    # filtra promoções que já foram apagadas.
    df = df.filter(
        (sf.col("deleted_at").isNull()) & (sf.col("_id").isNotNull())
    )

    # filtra somente as campanhas que são ativas.
    df = df.filter(
        sf.current_timestamp().between(
            sf.col("start_date"), sf.col("end_date")
        )
    )

    # filtra tipos de pagamento, não pode fazer antes de deletar as linhas, pq se não filtra as nulas.
    # as linhas do delete são nulas.
    df = df.filter(
        sf.col("payment_method").isin(
            ["pix", "boleto", "account_balance", "luiza_ouro", "visa"]
        )
    )

    # deduplica caso tenha a campanha repetida pelo último update.
    df = (
        df.withColumn(
            "rank",
            sf.row_number().over(
                Window.partitionBy("id").orderBy(sf.desc("timestamp_kafka"))
            ),
        )
        .filter(sf.col("rank") == 1)
        .drop("rank")
    )
    df = df.drop("rank")

    # só pode existir um seller por promoção.
    df = df.withColumn("seller", sf.explode("seller").alias("seller_id"))

    # renaming id
    df = df.withColumnRenamed("id", "id_discounts")

    return df


def config_spark_parallism(ss: SnessSpark) -> None:
    """To avoid skew it was needed to disable adaptative query execution.
    The AQE made the number of partitions became too low that it had a
    bootleneck in cpu perfomance.
    The configs in this functions disable the AQE to avoid this.
    """
    # desativando o Adaptative query execution
    ss.spark.conf.set("spark.sql.adaptive.enabled", "false")
    ss.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    ss.spark.conf.set("spark.sql.shuffle.partitions", 800)
    ss.spark.conf.set("spark.default.parallelism", 800)
    ss.spark.conf.set(
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "2MB"
    )


def save_penultimate_execution(
    ss: SnessSpark, environment: Environment, zone: Zone, dataset: str
):
    try:
        df = ss.read.parquet(
            environment=environment,
            zone=zone,
            namespace="visao_unica",
            dataset='prices',
        )
    except Exception as e:
        print(f"Dataframe not found: {e}")
        return

    ss.write.parquet(
        df,
        environment=environment,
        zone=zone,
        namespace="visao_unica",
        dataset=dataset,
        mode="overwrite",
        overwriteSchema=True,
    )


def get_robinhood_campaigns(ss: SnessSpark, rename: bool) -> DataFrame:
    df_campaign = (
        ss.read.parquet(
            Environment.PRODUCTION,
            Zone.TRUSTED,
            "visao_unica",
            "robinhood_campaigns",
        )
        .filter(
            (
                sf.current_timestamp().between(
                    sf.col("start_date"), sf.col("end_date")
                )
            )
            & (sf.col("benefit") == 'loyalty')
        )
        .select("campaign_id")
    )

    df_campaign_product = (
        ss.read.parquet(
            Environment.PRODUCTION,
            Zone.TRUSTED,
            "visao_unica",
            "robinhood_campaign_products",
        )
        .filter(sf.col("action") == 'insert')
        .select("seller_id", "sku", "benefit_loyalty_value", "campaign_id")
    )

    df = df_campaign.join(df_campaign_product, 'campaign_id', "inner").select(
        df_campaign_product["seller_id"],
        df_campaign_product["sku"],
        "benefit_loyalty_value",
    )

    df = df.withColumnRenamed("benefit_loyalty_value", "discount")

    df = df.dropDuplicates(["seller_id", "sku", "discount"])

    df = df.select(*[sf.col(c).alias(f"{c}_ouro") for c in df.columns])

    return df


def filter_invalid_campaigns(df, discounts, prices) -> DataFrame:
    """
    Filtra campanhas inválidas que não possuem nenhum filtro e nenhum preço definido.
    """

    # Condição para campanhas inválidas sem filtros e sem preços definidos
    invalid_campaign_condition = (
        (sf.size("target.include.skus") == 0)
        & (sf.size("target.include.brands") == 0)
        & (sf.size("target.include.selections") == 0)
        & (sf.size("target.include.categories") == 0)
        & (sf.col("target.include.price.min").cast("int") == sf.lit(0))
        & (sf.col("target.include.price.max").cast("int") == sf.lit(0))
    )

    # Condição para preços abaixo do mínimo
    minimum_price_condition = sf.col("preco_por") < sf.col(
        "target.exclude.minimum_price"
    )

    # Condição para exclusão por marcas
    exclude_brands_condition = sf.array_contains(
        discounts["target.exclude.brands"], prices.brand
    )

    # Condição para exclusão por categorias
    exclude_categories_condition = sf.array_contains(
        discounts["target.exclude.categories"], prices.category_id
    )

    # Condição para exclusão por seleções
    exclude_selections_condition = (
        sf.size(
            sf.array_intersect(
                discounts["target.exclude.selections"], prices["selections"]
            )
        )
        >= 1
    )

    # Condição para exclusão por SKUs
    exclude_skus_condition = sf.array_contains(
        discounts["target.exclude.skus"], prices.sku
    )

    # Combinação de todas as condições de exclusão
    final_condition = ~(
        invalid_campaign_condition
        | minimum_price_condition
        | exclude_brands_condition
        | exclude_categories_condition
        | exclude_selections_condition
        | exclude_skus_condition
    )

    # Aplicação do filtro
    return df.filter(final_condition)


def filter_valid_campaigns(
    df: DataFrame, discounts: DataFrame, prices: DataFrame
) -> DataFrame:
    """
    Filtra campanhas válidas de acordo com algumas condicoes para definição do preco por
    1) a partir dos valores mínimo e máximo de preço,
    2) condicao para campanhas aplicáveis a todos os produtos do seller
    3) condicao para inclusão por marcas, categorias, selecao,  inclusão por SKUs
    """

    # Condição para valores mínimo e máximo de preço
    # Quando o valor do mínimo ou máximo for diferente de zero
    # significa que eu tenho de usar ele.
    price_range_condition = sf.when(
        (sf.col("target.include.price.min") != sf.lit(0))
        | (sf.col("target.include.price.max") != sf.lit(0)),
        sf.col("preco_por").between(
            sf.col("target.include.price.min"),
            sf.col("target.include.price.max"),
        ),
    ).otherwise(sf.lit(True))

    # Condição para campanhas aplicáveis a todos os produtos do seller
    all_products_condition = (
        (sf.size("target.include.skus") == 0)
        & (sf.size("target.include.brands") == 0)
        & (sf.size("target.include.selections") == 0)
        & (sf.size("target.include.categories") == 0)
    )

    # Condição para inclusão por marcas
    include_brands_condition = sf.array_contains(
        discounts["target.include.brands"], prices.brand
    )

    # Condição para inclusão por categorias
    include_categories_condition = sf.array_contains(
        discounts["target.include.categories"], prices.category_id
    )

    # Condição para inclusão por seleções
    include_selections_condition = (
        sf.size(
            sf.array_intersect(
                discounts["target.include.selections"], prices["selections"]
            )
        )
        >= 1
    )

    # Condição para inclusão por SKUs
    include_skus_condition = sf.array_contains(
        discounts["target.include.skus"], prices.sku
    )

    # Combinação de todas as condições de inclusão
    final_condition = price_range_condition & (
        all_products_condition
        | include_brands_condition
        | include_categories_condition
        | include_selections_condition
        | include_skus_condition
    )

    # Aplicação do filtro
    return df.filter(final_condition)


def update_base_price(df: DataFrame) -> DataFrame:
    """
    Atualiza o valor preco_de se ele for menor que o valor de "preco_por" ou se for nulo.
    """

    df = df.withColumn(
        "price_de",
        sf.when(
            (sf.col("price_de") < sf.col("price_por"))
            | sf.col("price_de").isNull(),
            sf.col("price_por"),
        ).otherwise(sf.col("price_de")),
    )

    return df


def select_and_calculate_columns(df: DataFrame) -> DataFrame:
    """
    Seleciona e calcula as colunas especificadas, incluindo o cálculo de 'discount_perc' e 'price_discount'.
    """
    return df.select(
        "seller_id",
        "sku",
        "navigation_id",
        "active",
        "stock",
        "price_de",
        "price_por",
        "payment_method",
        "campaign_code",
        sf.col("installment_plans")
        .getItem(0)
        .getItem(1)
        .alias("discount_perc"),
        (
            sf.round(
                sf.col("price_por")
                * (
                    sf.lit(1)
                    - (
                        sf.col("installment_plans").getItem(0).getItem(1)
                        / sf.lit(100)
                    )
                ),
                4,
            )
        )
        .cast(st.DecimalType(8, 2))
        .alias("price_discount"),
        "id_discounts",
    )


def create_prices_pivot(df: DataFrame) -> DataFrame:
    """
    Cria um pivot do DataFrame agrupado por várias colunas e agrega o menor preço de desconto
    por metodo de pagamento.
    """
    return (
        df.groupBy(
            "seller_id",
            "sku",
            "navigation_id",
            "active",
            "stock",
            "campaign_code",
            "price_de",
            "price_por",
            "payment_method",
        )
        .pivot(
            "payment_method",
            ["pix", "boleto", "account_balance", "luiza_ouro", "visa"],
        )
        .agg(sf.min("price_discount"))
    )


def aggregate_prices_by_payment_method(df: DataFrame) -> DataFrame:
    """
    Agrupa o DataFrame por várias colunas e agrega os preços por metodo de pagamento usando
    o primeiro valor disponível, além de coletar a lista de métodos de pagamento.
    """
    return df.groupBy(
        "seller_id",
        "sku",
        "navigation_id",
        "active",
        "stock",
        "campaign_code",
        "price_de",
        "price_por",
    ).agg(
        sf.min("pix").alias("price_pix"),
        sf.min("boleto").alias("price_boleto"),
        sf.min("account_balance").alias("price_magalupay"),
        sf.min("luiza_ouro").alias("installment_price_once_luiza"),
        sf.min("visa").alias("installment_price_once_visa"),
    )


def join_prices_with_campaign(
    df_prices: DataFrame, df_campaign: DataFrame
) -> DataFrame:
    """
    Realiza o join entre o DataFrame de preços e o DataFrame de campanha de cliente ouro
    com condições específicas para o relacionamento de 'seller_id' e 'sku'.
    """
    join_condition = (
        (df_prices.seller_id == df_campaign.seller_ouro)
        & (df_prices.sku == df_campaign.sku_ouro)
        & (df_prices.skew == df_campaign.skew)
    ) | (
        (df_prices.seller_id == df_campaign.seller_ouro)
        & (df_campaign.sku_ouro == '*')
        & (df_prices.skew == df_campaign.skew)
    )

    return df_prices.join(df_campaign, how='left', on=join_condition)


def calculate_ouro_columns(df: DataFrame) -> DataFrame:
    """
    Adiciona colunas calculadas para percentuais de desconto e preco com desconto para cliente ouro.
    """
    # Cálculo do percentual de desconto para cliente ouro no Pix
    df = df.withColumn(
        "discount_perc_pix",
        sf.round(
            ((sf.col("price_por") - sf.col("price_pix")) / sf.col("price_por"))
            * 100,
            2,
        ).cast(st.DecimalType(8, 2)),
    )

    # Cálculo do preço com desconto para cliente ouro
    df = df.withColumn(
        "price_por_ouro",
        sf.round(
            sf.col("price_por")
            * (sf.lit(1) - (sf.col("discount_ouro") / sf.lit(100))),
            4,
        ).cast(st.DecimalType(8, 2)),
    )

    # Cálculo do preço com desconto para cliente ouro
    df = df.withColumn(
        "price_pix_ouro",
        sf.round(
            sf.col("price_por_ouro")
            * (sf.lit(1) - (sf.col("discount_perc_pix") / sf.lit(100))),
            4,
        ).cast(st.DecimalType(8, 2)),
    )

    return df


def select_final_columns(df: DataFrame) -> DataFrame:
    """
    Seleciona as colunas finais necessárias do DataFrame, incluindo os preços e descontos calculados.

    Args:
        df (DataFrame): DataFrame com todas as colunas calculadas.

    Returns:
        DataFrame: DataFrame final com as colunas selecionadas.
    """
    return df.select(
        "seller_id",
        "sku",
        "navigation_id",
        "active",
        "stock",
        "campaign_code",
        "price_de",
        "price_por",
        "price_pix",
        "price_boleto",
        "price_magalupay",
        "discount_perc_pix",
        "price_por_ouro",
        "discount_ouro",
        "price_pix_ouro",
        "installment_price_once_luiza",
        "installment_price_once_visa",
    )


def deduplicate_price_ouro(df: DataFrame) -> DataFrame:
    """
    Deduplica o DataFrame mantendo apenas o menor 'price_por_ouro' por 'seller_id' e 'sku',
    criando uma coluna de classificação e filtrando os registros com a menor classificação.
    """
    # Definindo a janela de classificação por 'seller_id' e 'sku'
    window_spec = Window.partitionBy(
        "seller_id", "sku", "campaign_code"
    ).orderBy("price_por_ouro")

    # Adicionando a coluna 'price_row_number' para classificação e filtrando o menor preço
    return (
        df.withColumn("price_row_number", sf.row_number().over(window_spec))
        .filter(sf.col("price_row_number") == 1)
        .drop(
            "price_row_number"
        )  # Removendo a coluna de row_number após o filtro
    )


def get_arcoiro_campaigns(
    ss: SnessSpark, payment_methods: Optional[List[str]] = None
) -> DataFrame:
    df = (
        ss.read.parquet(
            Environment.PRODUCTION,
            Zone.TRUSTED,
            "visao_unica",
            "campaigns_v2",
        )  # filtra campanhas ainda válidas.
        .filter(
            sf.current_timestamp().between(
                sf.col("start_date"), sf.col("end_date")
            )
        )
        .select(
            "timestamp_kafka",
            "seller",
            "max_installment_without_interest",
            "min_installment_amount",
            "payment_method",
            "installment_plans",
            "target",
            "campaign_code",
        )
    ).withColumn("seller", sf.col("seller").getItem(0))

    if payment_methods:
        df = df.filter(sf.col("payment_method").isin(payment_methods))

    return df


def calculate_installment_columns(df: DataFrame) -> DataFrame:
    """
    Seleciona e calcula as colunas especificadas, incluindo o cálculo de 'discount_perc' e 'price_discount'.
    """
    return df.select(
        "seller_id",
        "sku",
        "navigation_id",
        sf.col("active"),
        sf.col("stock"),
        sf.col("campaign_code"),
        sf.col("payment_method"),
        sf.col("preco_de").alias("price_de"),
        sf.col("preco_por").alias("price_por"),
        # Regra max_parcela_semjuros:
        # Caso a divisão entre o preco por e o valor mínimo da parcela (preco_por/min_installment_amount) seja menor que
        # a quantidade máxima de parcelas sem juros ("max_installment_without_interest"), pegamos a parte inteira dessa divisão.
        # Se a divisão do preco por pelo valor mínimo da parcela for maior que a quantidade máxima de parcelas sem juros, mantemos o máximo ("max_installment_without_interest").
        # Ex1:. 100(preco produto)/45 (valor mínimo da parcela na campanha) = 2,222 -> como 2 < 10 -> valor máximo do parcelamento é em 2x sem juros
        # Ex2:. 1000(preco produto)/45 (valor mínimo da parcela na campanha) = 22,222 -> como 22 !< 10 -> valor máximo do parcelamento é em 10x sem juros
        sf.when(
            (sf.col("preco_por") / sf.col("min_installment_amount")).cast(
                "integer"
            )
            <= sf.col("max_installment_without_interest"),
            (sf.col("preco_por") / sf.col("min_installment_amount")).cast(
                "integer"
            ),
        )
        .otherwise(sf.col("max_installment_without_interest"))
        .cast(st.IntegerType())
        .alias("max_parcela_semjuros"),
        # Para saber a quantidade máxima de parcelas precisamos calcular o tamanho desse array de arrays e pegar o primeiro item do último par.
        sf.col("installment_plans")[sf.size(sf.col("installment_plans")) - 1]
        .getItem(0)
        .cast(st.IntegerType())
        .alias("max_parcela"),
        # Para calcular o preco em 1x multiplicamos o preco por pelo primeiro juros do primeiro array.
        (
            sf.col("preco_por")
            * (1 + (sf.col("installment_plans").getItem(0).getItem(1) / 100))
        )
        .cast(st.DecimalType(8, 2))
        .alias("preco_parcelado"),
    )


def installment_prices_pivot(
    df: DataFrame, payment_methods: List[str]
) -> DataFrame:
    key_columns = [
        "seller_id",
        "sku",
        "navigation_id",
        "active",
        "stock",
        "campaign_code",
        "price_de",
        "price_por",
    ]

    columns_to_pivot = [
        "max_parcela_semjuros",
        "max_parcela",
        "preco_parcelado",
    ]

    # Elege a melhor campanha por método de pagamento
    # Prioridade 1: Maior parcela sem juros (desc)
    # Prioridade 2: Maior parcela total (desc)
    # Prioridade 3: Menor preço parcelado (asc)
    window_spec = Window.partitionBy(*key_columns, "payment_method").orderBy(
        sf.desc("max_parcela_semjuros"),
        sf.desc("max_parcela"),
        sf.asc("preco_parcelado"),
    )

    # Filtra mantendo apenas a melhor condição para aquele produto e método de pagamento
    df_best_campaign = (
        df.withColumn("rn", sf.row_number().over(window_spec))
        .filter(sf.col("rn") == 1)
        .drop("rn")
    )

    pivoted_df = df_best_campaign.select(key_columns).distinct()

    for column in columns_to_pivot:
        pivoted = (
            df_best_campaign.groupBy(key_columns)
            .pivot("payment_method", payment_methods)
            .agg(sf.first(column))  # Usa o valor da linha vencedora
        )

        for payment_method in payment_methods:
            new_col_name = f"{column}_{payment_method}"
            pivoted = pivoted.withColumnRenamed(payment_method, new_col_name)

        pivoted_df = pivoted_df.join(pivoted, key_columns, "left")

    return pivoted_df


def join_all_features(dfa: DataFrame, dfb: DataFrame) -> DataFrame:
    """
    Cria o catálogo completo com os precos, unindo os DataFrames de precos e deduplicacao de preco ouro,
    e preenchendo as colunas de preço com valores alternativos usando coalesce.
    """
    return (
        dfa.alias("a")
        .join(
            dfb.alias("b"),
            how='left',
            on=["seller_id", "sku", "navigation_id", "campaign_code"],
        )
        .select(
            sf.current_timestamp().alias("price_timestamp"),
            sf.current_date().alias("price_date"),
            sf.col("a.seller_id"),
            sf.col("a.navigation_id"),
            sf.col("a.sku"),
            sf.col("a.campaign_code"),
            sf.col("a.active"),
            sf.col("a.stock"),
            sf.coalesce(sf.col("a.price_de"), sf.col("b.price_de"))
            .alias("price_de")
            .cast(st.DecimalType(8, 2)),
            sf.coalesce(sf.col("a.price_por"), sf.col("b.price_por"))
            .alias("price_por")
            .cast(st.DecimalType(8, 2)),
            sf.col("a.price_pix").cast(st.DecimalType(8, 2)),
            sf.col("a.price_boleto").cast(st.DecimalType(8, 2)),
            sf.col("a.price_magalupay").cast(st.DecimalType(8, 2)),
            sf.col("a.price_por_ouro").cast(st.DecimalType(8, 2)),
            sf.col("a.price_pix_ouro").cast(st.DecimalType(8, 2)),
            sf.coalesce(
                sf.col("a.installment_price_once_visa"),
                sf.col("b.preco_parcelado_visa"),
                sf.col("a.price_por"),
                sf.col("b.price_por"),
            )
            .cast(st.DecimalType(8, 2))
            .alias("installment_price_once_visa"),
            sf.coalesce(
                sf.col("a.installment_price_once_luiza"),
                sf.col("b.preco_parcelado_luiza_ouro"),
                sf.col("b.price_por"),
                sf.col("a.price_por"),
            )
            .cast(st.DecimalType(8, 2))
            .alias("installment_price_once_luiza"),
            sf.col("b.max_parcela_semjuros_luiza_ouro").alias(
                "max_installment_interestfree_luiza"
            ),
            sf.col("b.max_parcela_semjuros_visa").alias(
                "max_installment_interestfree_visa"
            ),
            sf.col("b.max_parcela_luiza_ouro").alias("max_installment_luiza"),
            sf.col("b.max_parcela_visa").alias("max_installment_visa"),
        )
    )


def full_catalog(dfa: DataFrame, dfb: DataFrame) -> DataFrame:
    """
    Cria o catálogo completo com os precos, unindo os DataFrames de precos e deduplicacao de preco ouro,
    e preenchendo as colunas de preço com valores alternativos usando coalesce.
    """
    return (
        dfa.alias("a")
        .join(
            dfb.alias("b"),
            how='left',
            on=["seller_id", "sku", "navigation_id", "campaign_code"],
        )
        .select(
            sf.current_timestamp().alias("price_timestamp"),
            sf.current_date().alias("price_date"),
            sf.col("a.seller_id"),
            sf.col("a.navigation_id"),
            sf.col("a.sku"),
            sf.col("a.campaign_code"),
            sf.col("a.active"),
            sf.col("a.stock"),
            sf.col("a.currency"),
            sf.coalesce(sf.col("b.price_de"), sf.col("a.preco_de"))
            .cast(DecimalType(8, 2))
            .alias("price_de"),
            sf.coalesce(
                sf.col("b.price_por"),
                sf.col("a.preco_por"),
                sf.col("b.price_de"),
                sf.col("a.preco_de"),
            )
            .cast(DecimalType(8, 2))
            .alias("price_por"),
            sf.coalesce(
                sf.col("b.price_pix"),
                sf.col("b.price_por"),
                sf.col("a.preco_por"),
                sf.col("a.preco_de"),
            )
            .cast(DecimalType(8, 2))
            .alias("price_pix"),
            sf.coalesce(
                sf.col("b.price_boleto"),
                sf.col("b.price_por"),
                sf.col("a.preco_por"),
                sf.col("a.preco_de"),
            )
            .cast(DecimalType(8, 2))
            .alias("price_boleto"),
            sf.coalesce(
                sf.col("b.price_magalupay"),
                sf.col("b.price_por"),
                sf.col("a.preco_por"),
                sf.col("a.preco_de"),
            )
            .cast(DecimalType(8, 2))
            .alias("price_magalupay"),
            sf.coalesce(
                sf.col("b.price_por_ouro"),
                sf.col("b.price_por"),
                sf.col("a.preco_por"),
                sf.col("a.preco_de"),
            )
            .cast(DecimalType(8, 2))
            .alias("price_por_ouro"),
            sf.coalesce(
                sf.col("b.price_pix_ouro"),
                sf.col("b.price_pix"),
                sf.col("b.price_por"),
                sf.col("a.preco_por"),
                sf.col("a.preco_de"),
            )
            .cast(DecimalType(8, 2))
            .alias("price_pix_ouro"),
            sf.coalesce(
                sf.col("b.installment_price_once_visa"),
                sf.col("b.price_por"),
                sf.col("a.preco_por"),
            )
            .cast(DecimalType(8, 2))
            .alias("installment_price_once_visa"),
            sf.coalesce(
                sf.col("b.installment_price_once_luiza"),
                sf.col("b.price_por"),
                sf.col("a.preco_por"),
            )
            .cast(DecimalType(8, 2))
            .alias("installment_price_once_luiza"),
            sf.col("b.max_installment_interestfree_luiza"),
            sf.col("b.max_installment_interestfree_visa"),
            sf.col("b.max_installment_luiza"),
            sf.col("b.max_installment_visa"),
        )
    )


def get_catalog_remove_inactive(ss):
    """
    Filtra produtos inativos cujo campo 'updated_at' seja inferior a 30 dias a partir da data atual.

    Returns:
    DataFrame: DataFrame filtrado com os produtos inativos mais antigos que 30 dias.
    """

    ## busca o catálogo inteiro da tabela catalogo_produto_atual
    df = get_catalog(ss)

    ## filtra os casos que são inativos no catálogo e agrupa por navigation_id, seller_id e sku junto com a maior data de atualização
    df_inactive = (
        df.filter(
            (sf.col("active") == False) & (sf.col("category_id") != "TM")
        )
        .select("navigation_id", "seller_id", "sku", "updated_at")
        .groupBy("navigation_id", "seller_id", "sku")
        .agg(sf.max("updated_at").alias("max_updated_at"))
    )

    ## Filtra as linhas que tem mais de 30 dias sem atualizações, que estão inativos
    df_filter = df_inactive.filter(
        sf.col("max_updated_at")
        < sf.lit(datetime.now().date() - timedelta(days=30))
    )

    columns_catalog = [
        "seller_id",
        "sku",
        "navigation_id",
        "brand",
        "category_id",
        "active",
        "stock",
    ]

    df = df.select(columns_catalog)

    ## faz um left anti join com o catálogo total e retorna a tabela sem os casos que não são atualizados a mais de 30 dias e inativos
    df_result = df.join(
        df_filter,
        on=["navigation_id", "seller_id", "sku"],
        how="left_anti",
    )

    df_result = df_result.distinct()

    return df_result


def main(argv: Optional[Sequence[str]] = None) -> int:

    print(f"started execution at {datetime.now()}")
    args = parse_args(argv)
    logger.info(f"Running with args : {args})")

    ZONE = Zone.REFINED
    NAMESPACE_TO_WRITE = "visao_unica"

    # number of times to multiply dataset to avoid skew
    SKEW_MULTIPLY = 100

    ss = config_spark()
    config_spark_parallism(ss)

    # escrevendo penultima execução
    save_penultimate_execution(
        ss, args.environment, ZONE, 'prices_penultimate_execution'
    )

    print(f"saved last execution at {datetime.now()}")

    base_prices = get_base_prices(ss, "prd")
    base_prices.cache()
    selling_prices = get_selling_prices(ss, "prd")
    selling_prices.cache()
    discounts = get_arcoiro_discounts(ss, "prd")
    discounts.cache()

    # adicionando product discounts
    discounts = discounts.union(get_product_discounts(ss, "prd", discounts))

    catalog = get_catalog_remove_inactive(ss)

    catalog.cache()
    # busca todos os produtos do catálogo na base de preços
    # adding price_de
    prices = catalog.join(base_prices, on=["seller_id", "sku"], how="left")
    # adding price_por
    prices = prices.join(selling_prices, on=["seller_id", "sku"], how="left")
    prices.cache()
    # adding campanhas cliente ouro (robinhood)
    campanha_clienteouro = get_robinhood_campaigns(ss, rename=True)
    # adicionando campanha pagamento a prazo
    campanha_prazo = get_arcoiro_campaigns(
        ss, payment_methods=['visa', 'luiza_ouro']
    )

    prices_join = join_solve_imbalancing(
        df_to_hash=prices,
        df_to_explode=discounts,
        how="left",
        conditions=(prices.seller_id == discounts.seller),
        skew_multiply=SKEW_MULTIPLY,
        columns_for_hashing=[
            "seller_id",
            "sku",
            "navigation_id",
            "active",
            "stock",
            "preco_de",
            "preco_por",
        ],
    )

    """
        Condições que fazem entrar o desconto do preço a vista
        Target.include
        - brands ( Cruzar com catálogo )
        - categories ( cruzar com catálogo )
        - skus ( Catálogo )
        - selections ( Base Prices )
        - price ( preço por Selling Prices2 )

        Também existem casos que o desconto não entra
        Target.exclude

        - brands
        - categories
        - minimum_price
        - selections
        - skus
    """

    # o preço por recebe o preço dê, caso seja nulo.
    prices_renamed = prices_join.drop(
        "channel_id",
        "timestamp_kafka",
    ).withColumn(
        "preco_por", sf.coalesce(sf.col("preco_por"), sf.col("preco_de"))
    )

    # Removendo campanhas invalidas para desconto de pagamento a vista
    prices_invalid_campaigns_removed = filter_invalid_campaigns(
        prices_renamed, discounts, prices
    )

    # Aplicando campanhas validas para desconto de pagamento a vista
    prices_valid_campaigns = filter_valid_campaigns(
        prices_invalid_campaigns_removed, discounts, prices
    )

    # Converte a coluna 'preco_por' para DecimalType e renomeia preco_de e preco_por
    prices_converted = (
        prices_valid_campaigns.withColumn(
            "preco_por", sf.col("preco_por").cast(st.DecimalType(8, 2))
        )
        .withColumnRenamed("preco_de", "price_de")
        .withColumnRenamed("preco_por", "price_por")
    )

    # calcula o desconto para pagamento a vista
    prices_calculated = select_and_calculate_columns(prices_converted)

    # Calcula preco pix, boleto e magalupay com desconto
    prices_pivot = create_prices_pivot(prices_calculated)
    prices_pix_boleto_magapay = aggregate_prices_by_payment_method(
        prices_pivot
    )

    prices_join_jk = join_solve_imbalancing(
        df_to_hash=prices_pix_boleto_magapay,
        df_to_explode=campanha_clienteouro,
        how="left",
        conditions=(
            (
                (
                    prices_pix_boleto_magapay.seller_id
                    == campanha_clienteouro.seller_id_ouro
                )
                & (
                    prices_pix_boleto_magapay.sku
                    == campanha_clienteouro.sku_ouro
                )
            )
            | (
                (
                    prices_pix_boleto_magapay.seller_id
                    == campanha_clienteouro.seller_id_ouro
                )
                & (campanha_clienteouro.sku_ouro == '*')
            )
        ),
        skew_multiply=SKEW_MULTIPLY,
        columns_for_hashing=[
            "seller_id",
            "sku",
            "navigation_id",
        ],
    )

    add_price_ouro = calculate_ouro_columns(prices_join_jk)
    add_price_ouro = select_final_columns(add_price_ouro)

    price_ouro_dedup = deduplicate_price_ouro(add_price_ouro)

    prices_with_installment = join_solve_imbalancing(
        df_to_hash=prices,
        df_to_explode=campanha_prazo,
        how="left",
        conditions=(prices.seller_id == campanha_prazo.seller),
        skew_multiply=SKEW_MULTIPLY,
        columns_for_hashing=[
            "seller_id",
            "sku",
            "navigation_id",
            "active",
            "stock",
            "preco_de",
            "preco_por",
        ],
    ).drop(campanha_prazo["campaign_code"])

    # Removendo campanhas invalidas
    prices_invalid_campaigns_removed = filter_invalid_campaigns(
        prices_with_installment, campanha_prazo, prices
    )

    # Aplicando campanhas validas
    prices_valid_campaigns = filter_valid_campaigns(
        prices_invalid_campaigns_removed, campanha_prazo, prices
    )

    # Calculando preco a prazo
    installment_prices = calculate_installment_columns(prices_valid_campaigns)

    # pivoteando categorias de pagamento a prazo para novas colunas
    prices_pivot = installment_prices_pivot(
        installment_prices, ['visa', 'luiza_ouro']
    )

    # reunindo todas as features num unico dataframe
    all_features_joined = join_all_features(price_ouro_dedup, prices_pivot)

    # Reunindo precos que nao foram encontrados em nenhuma das campanhas acima (desconto a vista, cliente ouro e preco a prazo) com precos de campanha.
    full_catalog_with_price_features = full_catalog(
        prices, all_features_joined
    )

    #  corrigindo o valor da coluna "preco_de"
    full_catalog_base_price_updated = update_base_price(
        full_catalog_with_price_features
    )

    """
    Os datasets de preços estão consideravelmente grandes, o que nos leva a considerar a reparticioná-los.
    """
    full_catalog_with_prices = full_catalog_base_price_updated.repartition(150)

    full_catalog_with_prices.cache()

    ss.write.parquet(
        full_catalog_with_prices,
        args.environment,
        ZONE,
        NAMESPACE_TO_WRITE,
        "prices",
        mode="overwrite",
        overwriteSchema=True,
    )

    df_historico = read_visao_unica_dataset(
        ss, args.environment, ZONE, "prices_historic"
    )

    if (
        df_historico.filter(
            sf.col("price_date") == datetime.now().date()
        ).count()
        == 0
    ):

        ss.write.parquet(
            full_catalog_with_prices,
            args.environment,
            ZONE,
            NAMESPACE_TO_WRITE,
            "prices_historic",
            mode="append",
            partitionBy="price_date",
            mergeSchema=True,
        )

    else:
        fixed_fields = [
            'price_date',
            'price_timestamp',
            'seller_id',
            'navigation_id',
            'sku',
            'campaign_code',
            'active',
            'stock',
            'currency',
        ]
        price_fields = [
            'price_de',
            'price_por',
            'price_pix',
            'price_boleto',
            'price_magalupay',
            'price_por_ouro',
            'price_pix_ouro',
            'max_installment_visa',
            'installment_price_once_visa',
            'max_installment_interestfree_visa',
            'max_installment_luiza',
            'installment_price_once_luiza',
            'max_installment_interestfree_luiza',
        ]

        print(f"before changed prices update {datetime.now()}")
        prices_ultimate = read_visao_unica_dataset(
            ss, args.environment, ZONE, 'prices'
        )

        prices_penultimate = read_visao_unica_dataset(
            ss, args.environment, ZONE, 'prices_penultimate_execution'
        )

        changed_prices = get_changed_rows(
            fixed_fields,
            price_fields,
            ["navigation_id", "sku", "seller_id", "campaign_code"],
            prices_ultimate,
            prices_penultimate,
        )

        changed_prices.cache()

        ss.write.parquet(
            changed_prices.repartition(1),
            args.environment,
            ZONE,
            NAMESPACE_TO_WRITE,
            "prices_historic",
            mode="append",
            partitionBy="price_date",
            mergeSchema=True,
        )


if __name__ == "__main__":
    prices = main()
