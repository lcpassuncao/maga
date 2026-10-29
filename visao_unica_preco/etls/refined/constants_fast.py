"""
Schemas dos documentos Mongo que a trilha incremental consome.

Sao propositalmente mais estreitos que os table_schema dos pipelines
fry_base_prices e fry_selling_prices2: o from_json ignora campo ausente no
schema, entao aqui ficam declarados apenas os campos de que
dedup_base_prices e select_valid_selling_prices realmente dependem.

Manter estreito e intencional. Cada campo listado aqui e uma dependencia
da trilha rapida sobre a origem, e o teste de contrato em
tests/visao_unica_preco/test_fast_path_prices.py falha se algum sumir.
"""

# Envelope Debezium e chave do Kafka: identicos nos dois topicos.
MESSAGE_SCHEMA = (
    '{"fields":[{"metadata":{},"name":"payload","nullable":true,"type":'
    '{"fields":[{"metadata":{},"name":"after","nullable":true,"type":"string"},'
    '{"metadata":{},"name":"before","nullable":true,"type":"string"},'
    '{"metadata":{},"name":"op","nullable":true,"type":"string"}],'
    '"type":"struct"}}],"type":"struct"}'
)

KAFKA_KEY_SCHEMA = (
    '{"fields":[{"metadata":{},"name":"payload","nullable":true,"type":'
    '{"fields":[{"metadata":{},"name":"id","nullable":true,"type":"string"}],'
    '"type":"struct"}}],"type":"struct"}'
)

# Colunas que dedup_base_prices consome de fry.fry.base_prices.
BASE_PRICES_SCHEMA = (
    "seller_id string, "
    "sku string, "
    "list_price double, "
    "currency string, "
    "selections array<string>"
)

# Colunas que select_valid_selling_prices consome de fry.fry.selling_prices2.
SELLING_PRICES_SCHEMA = (
    "`_id` struct<`$oid`: string>, "
    "id string, "
    "seller_id string, "
    "sku string, "
    "price double, "
    "channel_id string, "
    "campaign_code string, "
    "price_campaign_id string, "
    "origin string, "
    "start_at bigint, "
    "end_at bigint, "
    "created_at bigint, "
    "updated_at bigint, "
    "deleted_at string"
)

# start_at/end_at/created_at/updated_at chegam como epoch em SEGUNDOS.
# O trusted_selling_prices2.treat_to_trusted faz esta mesma conversao antes
# de gravar; sem ela o filtro de validade de select_valid_selling_prices
# compara current_timestamp contra numero cru e nao levanta erro.
#
# Atencao ao nome do helper: parse_timestamp_in_micro usa from_unixtime, que
# espera segundos. Quem descreve a realidade e o nome da variavel no
# trusted_selling_prices2 (cols_with_timestamp_in_seconds), nao o da funcao.
SELLING_PRICES_TIMESTAMP_COLUMNS = [
    "created_at",
    "end_at",
    "start_at",
    "updated_at",
]

TOPIC_BASE_PRICES = "fry.fry.base_prices"
TOPIC_SELLING_PRICES = "fry.fry.selling_prices2"
