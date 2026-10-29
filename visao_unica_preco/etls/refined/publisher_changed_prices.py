import logging
from typing import List, Optional, Sequence

import pyspark.sql.functions as sf
from google.cloud import pubsub_v1
from pyspark.sql import DataFrame, Row
from sness.datalake.metadata import Environment, Zone
from utils import config_spark, parse_args, read_visao_unica_dataset

logger = logging.getLogger(__name__)


def serialize_message(df: DataFrame) -> DataFrame:
    payload_cols = [
        'navigation_id',
        'sku',
        'seller_id',
        'price_de',
        'price_por',
        'price_pix',
        'price_boleto',
        'price_magalupay',
    ]

    struct_value = sf.struct(
        sf.struct(*payload_cols).alias("payload"),
        sf.lit("prices").alias("tag"),
    )

    df_json = df.withColumn("value", sf.to_json(struct_value)).drop(
        *payload_cols
    )

    return df_json


def publica_mensagem(rows: List[Row]) -> None:
    publisher_options = pubsub_v1.types.PublisherOptions(
        flow_control=pubsub_v1.types.PublishFlowControl(
            message_limit=15000,
            byte_limit=5 * 1024 * 1024,
            limit_exceeded_behavior=pubsub_v1.types.LimitExceededBehavior.BLOCK,
        )
    )
    publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)
    topic_path = publisher.topic_path(
        "magalu-digital-project", "visao-unica-tags"
    )
    futures = []

    for row in rows:
        futures.append(
            publisher.publish(
                topic_path, data=row.value.encode("UTF-8"), tag="prices"
            )
        )

    for future in futures:
        future.result()


def main(argv: Optional[Sequence[str]] = None):

    args = parse_args(argv)
    logger.info(f"Running with args : {args})")

    ZONE = Zone.REFINED

    ss = config_spark()
    prices = read_visao_unica_dataset(ss, args.environment, ZONE, 'prices')
    prices.createOrReplaceTempView("ultimate_price_list")

    prices_penultimate = read_visao_unica_dataset(
        ss, args.environment, ZONE, 'prices_penultimate_execution'
    )
    prices_penultimate.createOrReplaceTempView("penultimate_price_list")

    changed_prices = ss.spark.sql("""
        select  ultimate_price_list.navigation_id, 
                ultimate_price_list.sku, 
                ultimate_price_list.seller_id, 
                ultimate_price_list.price_de, 
                ultimate_price_list.price_por, 
                ultimate_price_list.price_pix,
                ultimate_price_list.price_boleto,
                ultimate_price_list.price_magalupay,
                ultimate_price_list.price_por_ouro,
                ultimate_price_list.price_pix_ouro
                
        from ultimate_price_list 
            left join penultimate_price_list
            on penultimate_price_list.seller_id = ultimate_price_list.seller_id 
            and penultimate_price_list.sku = ultimate_price_list.sku
    
        where   COALESCE(penultimate_price_list.price_de, 0)  != COALESCE(ultimate_price_list.price_de, 0) or
                COALESCE(penultimate_price_list.price_por, 0) != COALESCE(ultimate_price_list.price_por, 0) or
                COALESCE(penultimate_price_list.price_pix, 0) != COALESCE(ultimate_price_list.price_pix, 0) or
                COALESCE(penultimate_price_list.price_boleto, 0) != COALESCE(ultimate_price_list.price_boleto, 0) or
                COALESCE(penultimate_price_list.price_magalupay, 0) != COALESCE(ultimate_price_list.price_magalupay, 0) or 
                COALESCE(penultimate_price_list.price_por_ouro, 0) != COALESCE(ultimate_price_list.price_por_ouro, 0) or 
                COALESCE(penultimate_price_list.price_pix_ouro, 0) != COALESCE(ultimate_price_list.price_pix_ouro, 0) 
    
        """)

    changed_prices_serialized = serialize_message(df=changed_prices)
    changed_prices_serialized.foreachPartition(publica_mensagem)


if __name__ == "__main__":
    main()
