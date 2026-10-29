"""Script para validar na mão os preços da visão de prices em comparação com os preços da api do Catálogo"""

from dataclasses import dataclass
from decimal import Decimal
from multiprocessing import Pool
from typing import Dict, Sequence

import requests
from pyspark.sql import Row
from pyspark.sql import functions as sf
from sness.datalake.sness_spark import SnessSpark

TK = "SUBSTITUIR TOKEN AQUI"  # noqa


@dataclass
class ProdutoFromCatalogo:
    navigation_id: str
    price_de: Decimal
    price_por: Decimal
    price_pix: Decimal
    price_pix_discount: Decimal

    def toRow(self):

        return Row(**self.__dict__)


def get_data_from_navigation_id(navigation_id: str) -> Dict:
    """Fetchs price informations about navigation_id"""
    try:
        URL_CATALOGO = f"http://catalogo-internal.luizalabs.com/product/{navigation_id}/?token={TK}"
        r = requests.get(URL_CATALOGO, timeout=100)
    except Exception as e:
        print(e)
        return (navigation_id, None)
    return (navigation_id, r.json())


def parse_prices_from_data(
    data: Dict, navigation_id: str
) -> Dict[str, Decimal]:

    try:
        data = data["data"]

        price_de = data["price"]
        price_por = None
        price_pix = None
        pix_discount = None

        for variation in data["variations"]:
            if variation["id"] != navigation_id:
                continue

            for seller in variation["sellers"]:
                if "price" in seller:
                    price_por = seller["price"]
                if "best_price" in seller:

                    if seller["best_price"]["payment_method_id"] == "pix":
                        price_pix = seller["best_price"]["total_amount"]
                        pix_discount = seller["best_price"]["discount"]

        prices = {
            "price_de": price_de,
            "price_por": price_por,
            "price_pix": price_pix,
            "price_pix_discount": pix_discount,
            "navigation_id": navigation_id,
        }

        prices = ProdutoFromCatalogo(
            navigation_id=navigation_id,
            price_de=price_de,
            price_por=price_por,
            price_pix=price_pix,
            price_pix_discount=pix_discount,
        )
    except Exception as e:
        print(navigation_id)
        print(e)
        print(data)
        return None

    return prices


def fetch_prices(teste: Sequence[str]) -> Sequence[ProdutoFromCatalogo]:

    # print(teste)

    with Pool(15) as p:
        lis_data = p.map(get_data_from_navigation_id, teste)
    # print(data)
    results = []

    lis_data = [x for x in lis_data if x[1]]

    for idx, (nav_id, data) in enumerate(lis_data):
        produto = parse_prices_from_data(data, navigation_id=nav_id)

        results.append(produto)
        # print(produto)
    return results


def main() -> int:
    # parte do script para validar
    ss = SnessSpark()
    df = ss.read.parquet("prd", "refined", "visao_unica", "prices")
    df = (
        df.filter(sf.col("active") & sf.col("stock"))
        .orderBy(sf.rand())
        .limit(5000)
        .select(
            "navigation_id",
            "sku",
            "seller_id",
            "price_de",
            "price_por",
            "price_pix",
            "discount_perc_pix",
        )
    )
    df.cache()
    nav_ids_to_fetch = [row["navigation_id"] for row in df.collect()]
    nav_enriched = fetch_prices(nav_ids_to_fetch)
    data = [x.toRow() for x in nav_enriched if x]
    api_results = ss.spark.createDataFrame(data)

    for col_name in api_results.columns:
        api_results = api_results.withColumnRenamed(
            col_name, f"api_{col_name}"
        )

    t = df.join(
        api_results,
        how='inner',
        on=sf.col("navigation_id") == sf.col("api_navigation_id"),
    )

    # &
    a = t.withColumn(
        "right", (sf.col("price_pix") == sf.col("api_price_pix"))
    ).filter(
        (sf.col("right") == False)  # noqa
        & (
            sf.abs((sf.col("api_price_pix") - sf.col("price_pix")))
            >= sf.lit(0.1)
        )
    )
    print(a.count())
    a.show(n=100, truncate=False)
