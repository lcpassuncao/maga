"""Módulo compartilhado para publicação de mensagens no Google Cloud Pub/Sub.

Fornece funções reutilizáveis para envio de mensagens com retry automático
e Dead Letter Queue (DLQ) para falhas permanentes.
"""

import logging
import time
from concurrent.futures import TimeoutError
from typing import Callable, Iterable, List, Tuple

from google.cloud import pubsub_v1
from pyspark.sql import Row

# ---------------------------------------------------------------------------
# Constantes de controle
# ---------------------------------------------------------------------------
FLUSH_SIZE = 5_000
PUBSUB_MAX_MESSAGES = 1_000
PUBSUB_MAX_BYTES = 5 * 1024 * 1024
PUBSUB_MAX_LATENCY = 0.05
FLOW_MESSAGE_LIMIT = 5_000
FLOW_BYTE_LIMIT = 20 * 1024 * 1024
MAX_RETRIES = 3
RETRY_BACKOFF = 0.3
FUTURE_TIMEOUT = 60
TOPIC_PATH = "projects/magalu-digital-project/topics/visao-unica-tags"


def _create_publisher() -> pubsub_v1.PublisherClient:
    """Cria cliente de publicação do Google Cloud Pub/Sub com configurações conservadoras.

    Configura batch settings e flow control para garantir uso eficiente de memória
    ao publicar mensagens em massa.

    Returns:
        PublisherClient configurado com limites de taxa e memória.
    """
    batch_settings = pubsub_v1.types.BatchSettings(
        max_messages=PUBSUB_MAX_MESSAGES,
        max_bytes=PUBSUB_MAX_BYTES,
        max_latency=PUBSUB_MAX_LATENCY,
    )
    publisher_options = pubsub_v1.types.PublisherOptions(
        flow_control=pubsub_v1.types.PublishFlowControl(
            message_limit=FLOW_MESSAGE_LIMIT,
            byte_limit=FLOW_BYTE_LIMIT,
            limit_exceeded_behavior=pubsub_v1.types.LimitExceededBehavior.BLOCK,
        )
    )
    return pubsub_v1.PublisherClient(
        batch_settings=batch_settings,
        publisher_options=publisher_options,
    )


def _flush_futures(
    futures: List[Tuple],
    publisher: pubsub_v1.PublisherClient,
    exec_logger: logging.Logger,
) -> List[str]:
    """Aguarda conclusão de futures de publicação com retry automático.

    Processa resultados de cada future, tentando reenviar mensagens que
    falharam até MAX_RETRIES vezes. Retorna apenas mensagens que falharam
    permanentemente para processamento em DLQ.

    Args:
        futures: Lista de tuplas (future, json_str, attrs) retornadas por publish().
        publisher: Cliente do Pub/Sub para reenvios.
        exec_logger: Logger para registrar erros e status.

    Returns:
        Lista de strings JSON que falharam permanentemente após MAX_RETRIES.
    """
    to_retry = []
    failed = []

    for future, json_str, attrs in futures:
        try:
            future.result(timeout=FUTURE_TIMEOUT)
        except TimeoutError:
            try:
                future.result()
            except Exception as e:
                to_retry.append((json_str, attrs, e))
        except Exception as e:
            to_retry.append((json_str, attrs, e))

    for json_str, attrs, original_error in to_retry:
        sent = False
        for attempt in range(MAX_RETRIES):
            try:
                retry_future = publisher.publish(
                    TOPIC_PATH,
                    data=json_str.encode("UTF-8"),
                    **attrs,
                )
                retry_future.result(timeout=FUTURE_TIMEOUT)
                sent = True
                break
            except Exception:
                time.sleep(RETRY_BACKOFF * (2**attempt))
        if not sent:
            exec_logger.error(
                "Falha permanente apos %d tentativas: %s",
                MAX_RETRIES,
                original_error,
            )
            failed.append(json_str)

    return failed


def make_pubsub_publisher(
    tag: str,
) -> Callable[[Iterable[Row]], Iterable[str]]:
    """Cria função de publicação no Pub/Sub parametrizada pela tag.

    Retorna uma closure compatível com ``rdd.mapPartitions()`` que publica
    mensagens no Pub/Sub com retry automático e DLQ.

    Args:
        tag: Tag de identificação enviada como atributo das mensagens
            (ex.: ``"price_percentage"`` ou ``"price_percentage_hml"``).

    Returns:
        Função que recebe um iterável de Rows e retorna iterável de JSONs
        que falharam permanentemente.

    Example:
        >>> publisher_fn = make_pubsub_publisher("price_percentage")
        >>> rdd_erros = df.rdd.mapPartitions(publisher_fn)
    """

    def publica_mensagem_com_dlq(rows: Iterable[Row]) -> Iterable[str]:
        """Publica mensagens no Pub/Sub com Dead Letter Queue para falhas.

        Processa uma partição de linhas, publicando-as de forma assíncrona no
        Pub/Sub. A cada FLUSH_SIZE mensagens, aguarda confirmação síncrona.
        Mensagens com falha transitória são reenviadas até MAX_RETRIES vezes.

        Args:
            rows: Iterável de linhas do RDD contendo 'value' (JSON) e 'seller_id'.

        Returns:
            Iterável contendo apenas mensagens JSON que falharam permanentemente.
        """
        exec_logger = logging.getLogger("pubsub_executor")
        publisher = _create_publisher()

        falhas = []  # type: List[str]
        futures = []  # type: List[Tuple]

        try:
            for row in rows:
                json_str = row.value
                attrs = {
                    "tag": tag,
                    "seller_id": str(row.seller_id),
                }
                try:
                    future = publisher.publish(
                        TOPIC_PATH,
                        data=json_str.encode("UTF-8"),
                        **attrs,
                    )
                    futures.append((future, json_str, attrs))
                except Exception as e:
                    exec_logger.error("Erro ao agendar publish: %s", e)
                    falhas.append(json_str)

                if len(futures) >= FLUSH_SIZE:
                    falhas.extend(
                        _flush_futures(futures, publisher, exec_logger)
                    )
                    futures = []

            if futures:
                falhas.extend(_flush_futures(futures, publisher, exec_logger))
        finally:
            try:
                publisher.transport.close()
            except Exception as e:
                exec_logger.warning(
                    "Erro ao fechar transporte do publisher: %s", e
                )

        return iter(falhas)

    return publica_mensagem_com_dlq
