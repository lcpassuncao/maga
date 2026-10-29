# Diagrama de Sequência — Ciclo de Vida de um Update de Preço

Fluxo completo de uma alteração de preço de e-commerce até a base final
`refined.visao_unica.prices`, cobrindo consumo do update, DAGs do Airflow,
transformações que influenciam o preço e a inserção na base final.

```mermaid
sequenceDiagram
    autonumber
    participant SRC as Origem do Update<br/>(FRY / Arcoiro /<br/>João Kleber)
    participant KAFKA as Kafka CDC<br/>(Debezium / MongoDB)
    participant TRU as TRUSTED visao_unica<br/>(base_prices, selling_prices2,<br/>discounts_v2, campaigns_v2,<br/>robinhood_campaigns,<br/>catalogo_produto_atual)
    participant DAG as DAG Airflow<br/>VisaoUnicaPreco<br/>(cron 0 * * * *)
    participant ETL as Task trusted_prices<br/>(visao_precos.py / Spark)
    participant RULES as Regras / Transformações<br/>(compose_prices)
    participant REF as REFINED<br/>refined.visao_unica.prices
    participant PUB as Task publisher<br/>(publisher_changed_prices.py)
    participant SINK as Pub/Sub + BigQuery<br/>(prices_historic)

    Note over SRC,KAFKA: 1. Consumo do update de preço
    SRC->>KAFKA: Altera preço / campanha / desconto<br/>(list_price, price, promoção,<br/>parcelamento, cliente ouro)
    KAFKA->>TRU: CDC grava na camada trusted<br/>(transient → raw → trusted)

    Note over DAG,ETL: 2. Gatilho do Airflow
    DAG->>ETL: Start >> trusted_prices<br/>(schedule horário, max_active_runs=1,<br/>instances=12, retry=1)

    Note over ETL,REF: 3. Snapshot para change detection
    ETL->>REF: save_penultimate_execution()<br/>prices → prices_penultimate_execution

    Note over ETL,TRU: 4. Obtenção + deduplicação das fontes
    ETL->>TRU: get_base_prices() → preço DE<br/>(dedup_base_prices: row_number seller+sku)
    ETL->>TRU: get_selling_prices() → preço POR<br/>(select_valid_selling_prices: origem + price)
    ETL->>TRU: get_arcoiro_discounts() +<br/>get_product_discounts() → descontos à vista
    ETL->>TRU: get_arcoiro_campaigns() → parcelamento<br/>(visa, luiza_ouro)
    ETL->>TRU: get_robinhood_campaigns() → cliente ouro
    ETL->>TRU: get_catalog_remove_inactive() → catálogo<br/>(exclui TM / inativo > 30 dias)
    TRU-->>ETL: DataFrames deduplicados (cache)

    Note over ETL,RULES: 5. Transformações que influenciam o preço
    ETL->>RULES: compose_prices() — junta catálogo +<br/>base_prices + selling_prices (anti-skew)
    RULES->>RULES: filter_valid_campaigns()<br/>✅ skus, brands, categories,<br/>selections, faixa de preço
    RULES->>RULES: filter_invalid_campaigns()<br/>❌ minimum_price, exclusões<br/>de marca/categoria/sku/seleção
    RULES->>RULES: create_prices_pivot +<br/>aggregate_prices_by_payment_method<br/>→ price_pix / price_boleto / price_magalupay
    RULES->>RULES: calculate_ouro_columns +<br/>deduplicate_price_ouro<br/>→ price_por_ouro / price_pix_ouro
    RULES->>RULES: calculate_installment_columns +<br/>installment_prices_pivot<br/>→ parcelas visa / luiza_ouro
    RULES->>RULES: join_all_features + full_catalog<br/>(coalesce fallbacks) + update_base_price<br/>(price_de >= price_por)
    RULES-->>ETL: full_catalog_with_prices<br/>(repartition 150, cache)

    Note over ETL,REF: 6. Inserção na base final
    ETL->>REF: write parquet → refined.visao_unica.prices<br/>(mode=overwrite, overwriteSchema=True)

    Note over DAG,SINK: 7. Histórico e publicação das mudanças
    DAG->>ETL: trusted_prices >> bq_prices >> publisher
    ETL->>REF: append → prices_historic<br/>(partição price_date) ou diff via<br/>get_changed_rows()
    DAG->>PUB: publisher_changed_prices.py
    PUB->>REF: LEFT JOIN prices x<br/>prices_penultimate_execution<br/>(detecta o que realmente mudou)
    PUB->>SINK: publica alterações no Pub/Sub<br/>+ carga BigQuery (bq_prices)
```

## Fluxo resumido

1. **Consumo** — Uma alteração de preço/campanha nasce nas origens (FRY, Arcoiro,
   João Kleber) e chega via CDC (Kafka/Debezium) às tabelas `trusted.visao_unica.*`.
2. **Gatilho** — A DAG `VisaoUnicaPreco` (cron `0 * * * *`) dispara a task
   `trusted_prices` a cada hora.
3. **Snapshot** — Antes de recompor, a execução anterior é salva em
   `prices_penultimate_execution` para detecção de mudança.
4. **Fontes** — São lidas e deduplicadas as bases de preço DE/POR, descontos,
   campanhas de parcelamento, campanhas cliente ouro e o catálogo ativo.
5. **Transformações** — `compose_prices()` aplica as regras de elegibilidade
   (inclusão/exclusão), calcula preços por forma de pagamento, preço ouro e
   parcelamento, e consolida com fallbacks via `coalesce`.
6. **Inserção** — O catálogo final é gravado em `refined.visao_unica.prices`
   (overwrite) e no histórico.
7. **Publicação** — O `publisher` compara com a execução anterior e publica no
   Pub/Sub + BigQuery apenas o que realmente mudou.
