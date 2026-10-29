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

    TARGET_FILE_SIZE_MB = 6
    PARTITION_COLUMN = "timestamp_kafka_day"
    path = "gs://prd-lake-trusted-visao_unica/selling_prices2"

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

    ss.spark.stop()

    return 0


if __name__ == "__main__":
    main()
