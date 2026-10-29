
-- BASE_PRICES
SELECT
  seller_id,
  sku,
  list_price,
  timestamp_kafka,
  selections,
  row_number() over (partition by seller_id,sku order by  timestamp_kafka desc)  as pos
FROM `maga-bigdata.visao_unica.base_prices`
QUALIFY pos = 1;


-- SELLING_PRICE2

SELECT
 seller_id,
 sku,
 campaign_code,
 channel_id,
 start_at,
 end_at,
 price,
 timestamp_kafka,
 row_number() over (partition by seller_id,sku order by  timestamp_kafka desc)  as pos
from
 `maga-bigdata.visao_unica.selling_prices2`
QUALIFY pos = 1;

