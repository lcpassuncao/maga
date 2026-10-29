"""
Converte prices_historic de snapshot diario para SCD tipo 2 numa tabela nova.

Hoje a prices_historic tem pelo menos uma linha por oferta por dia, mude o
preco ou nao: a primeira rodada do dia grava o catalogo inteiro e as demais
gravam o get_changed_rows. Em SCD2 guarda-se uma linha por PERIODO de
preco, com valid_from e valid_to.

Uso:
    experimento_scd2_prices_historic.py \
        --environment prd \
        --start_date 2026-05-12 --end_date 2026-08-09

Sem --start_date varre o historico inteiro (~42 bilhoes de linhas).
"""

import argparse
import logging
import sys
from typing import List, Optional, Sequence

import pyspark.sql.functions as sf
from pyspark.sql import DataFrame, Window
from sness.datalake.metadata import Zone
from utils import config_spark, read_visao_unica_dataset

logger = logging.getLogger()
logger.setLevel(logging.INFO)
_h = logging.StreamHandler(sys.stdout)
_h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(_h)

NAMESPACE = "visao_unica"
DATASET_ORIGEM = "prices_historic"

# Identidade da oferta: mesma chave que get_changed_rows usa no visao_precos.
CHAVE: List[str] = ["seller_id", "navigation_id", "sku", "campaign_code"]

# Mudanca em qualquer uma destas abre um periodo novo.
COLUNAS_VALOR: List[str] = [
    "price_de",
    "price_por",
    "price_pix",
    "price_boleto",
    "price_magalupay",
    "price_por_ouro",
    "price_pix_ouro",
    "installment_price_once_visa",
    "installment_price_once_luiza",
    "max_installment_visa",
    "max_installment_luiza",
    "max_installment_interestfree_visa",
    "max_installment_interestfree_luiza",
    "active",
    "stock",
    "currency",
]

# price_date e price_timestamp ficam de fora do hash de proposito: sao
# carimbo de execucao, nao valor de negocio. Se entrassem, toda linha seria
# diferente da anterior e nada colapsaria -- que e a razao de a tabela atual
# nao conseguir se comprimir sozinha.


def parse_args(argv: Optional[Sequence[str]] = None):
    p = argparse.ArgumentParser()
    p.add_argument("--environment", choices=["prd", "stg"], required=True)
    p.add_argument("--start_date", help="YYYY-MM-DD inclusive")
    p.add_argument("--end_date", help="YYYY-MM-DD inclusive")
    p.add_argument("--dataset_destino", default="prices_historic_scd2")
    return p.parse_args(argv)


def ler(ss, environment, start_date, end_date) -> DataFrame:
    df = read_visao_unica_dataset(
        ss, environment, Zone.REFINED, DATASET_ORIGEM
    )
    if start_date:
        df = df.filter(sf.col("price_date") >= sf.lit(start_date).cast("date"))
    if end_date:
        df = df.filter(sf.col("price_date") <= sf.lit(end_date).cast("date"))
    return df


def _assinatura(colunas: List[str]) -> sf.Column:
    """Hash dos valores da linha.

    O coalesce para sentinela e necessario porque concat_ws ignora nulo:
    sem ele (10, NULL) e (NULL, 10) dariam a mesma assinatura e o SCD2
    colapsaria periodos que sao diferentes.
    """
    partes = [
        sf.coalesce(sf.col(c).cast("string"), sf.lit(" ")) for c in colunas
    ]
    return sf.sha2(sf.concat_ws("", *partes), 256)


def montar_scd2(df: DataFrame) -> DataFrame:
    """Colapsa snapshots consecutivos identicos em periodos de validade."""
    ordem = Window.partitionBy(*CHAVE).orderBy("price_timestamp")

    # Mesma oferta pode ter varias linhas no mesmo timestamp; fica a ultima.
    df = (
        df.withColumn(
            "_rn",
            sf.row_number().over(
                Window.partitionBy(*CHAVE, "price_timestamp").orderBy(
                    sf.col("price_date").desc()
                )
            ),
        )
        .filter(sf.col("_rn") == 1)
        .drop("_rn")
    )

    df = df.withColumn("_h", _assinatura(COLUNAS_VALOR))
    df = df.withColumn("_h_ant", sf.lag("_h").over(ordem))

    # Primeira linha da oferta tambem abre periodo.
    df = df.withColumn(
        "_mudou",
        (sf.col("_h_ant").isNull() | (sf.col("_h") != sf.col("_h_ant"))).cast(
            "int"
        ),
    )

    # Soma cumulativa das mudancas numera os periodos (gaps and islands).
    df = df.withColumn(
        "_periodo",
        sf.sum("_mudou").over(ordem.rowsBetween(Window.unboundedPreceding, 0)),
    )

    periodos = df.groupBy(*CHAVE, "_periodo").agg(
        sf.min("price_timestamp").alias("valid_from"),
        sf.min("price_date").alias("price_date_inicio"),
        sf.max("price_date").alias("price_date_fim"),
        sf.count(sf.lit(1)).alias("snapshots_colapsados"),
        *[sf.first(c).alias(c) for c in COLUNAS_VALOR],
    )

    # valid_to = inicio do proximo periodo (limite superior exclusivo).
    # Periodo corrente fica NULL, o que permite consultar com
    # valid_from <= D AND (valid_to > D OR valid_to IS NULL).
    fecho = Window.partitionBy(*CHAVE).orderBy("valid_from")
    periodos = periodos.withColumn(
        "valid_to", sf.lead("valid_from").over(fecho)
    ).withColumn("is_current", sf.col("valid_to").isNull())

    return periodos.drop("_periodo")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    ss = config_spark()

    logger.info(
        f"janela: {args.start_date or 'inicio'} ate {args.end_date or 'fim'}"
    )

    origem = ler(ss, args.environment, args.start_date, args.end_date)
    linhas_origem = origem.count()
    logger.info(f"linhas na origem: {linhas_origem:,}")

    scd2 = montar_scd2(origem)
    scd2.cache()
    linhas_scd2 = scd2.count()

    ss.write.parquet(
        scd2,
        args.environment,
        Zone.REFINED,
        NAMESPACE,
        args.dataset_destino,
        mode="overwrite",
        partitionBy="price_date_inicio",
        overwriteSchema=True,
    )

    razao = linhas_origem / linhas_scd2 if linhas_scd2 else 0
    logger.info("=" * 62)
    logger.info(f"  linhas origem : {linhas_origem:,}")
    logger.info(f"  linhas SCD2   : {linhas_scd2:,}")
    logger.info(f"  reducao       : {razao:.1f}x")
    logger.info("=" * 62)

    scd2.select(
        sf.mean("snapshots_colapsados").alias("media_colapsada"),
        sf.expr("percentile_approx(snapshots_colapsados, 0.5)").alias(
            "mediana"
        ),
        sf.max("snapshots_colapsados").alias("maximo"),
    ).show(truncate=False)

    logger.info(
        "Comparar bytes com:  "
        f"gsutil du -s gs://{args.environment}-lake-refined-{NAMESPACE}/"
        f"{DATASET_ORIGEM}/  gs://{args.environment}-lake-refined-{NAMESPACE}/"
        f"{args.dataset_destino}/"
    )
    return 0


if __name__ == "__main__":
    main()
