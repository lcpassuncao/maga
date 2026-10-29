from typing import Optional, Sequence

import pyspark.sql.types as st
from pyspark.sql import functions as sf
from sness.datalake.metadata import Zone
from utils import config_spark, parse_args, read_visao_unica_dataset


def main(argv: Optional[Sequence[str]] = None) -> int:

    ss = config_spark()
    args = parse_args()
    ZONE = Zone.REFINED
    NAMESPACE_TO_WRITE = "visao_unica"

    df_prices_historic = read_visao_unica_dataset(
        ss, args.environment, ZONE, "prices_historic"
    ).filter(
        sf.col("price_date")
        >= sf.date_sub(sf.current_date(), args.days_to_process)
    )

    df_prices_historic.cache()

    ss.write.parquet(
        df_prices_historic,
        args.environment,
        ZONE,
        NAMESPACE_TO_WRITE,
        "prices_historic_last_90_days",
        partitionBy="price_date",
        mode="overwrite",
        mergeSchema=True,
    )


if __name__ == "__main__":
    main()
