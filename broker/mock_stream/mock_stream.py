#!/usr/bin/env python2.7
# -*- coding: UTF-8 -*-

"""This module provides a Kafa stream of archived ZTF alerts."""

from kafka import KafkaProducer

from .. import ztf_archive as ztfa

if not ztfa.get_local_alert_list() == 0:
    raise RuntimeError('No local ZTF data available.')


def send_alerts(
        max_alerts, bootstrap_servers=[], compression_type='gzip', **kwargs):
    """Load locally available ZTF alerts into the Kafka stream

    Args:
        max_alerts              (int): Maximum alerts to load
        bootstrap_servers (list[str]): List of servers to connect to
        compression_type        (str): Compression algorithm to use (Default: 'gzip')
        Any other arguments for initializing a ``KafkaProducer`` object
    """

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        compression_type=compression_type,
        **kwargs)

    print('Staging messages...')
    for i, alert in enumerate(ztfa.iter_alerts(raw=True)):
        if i >= max_alerts:
            break

        producer.send('ztf-stream', alert[0])

    print('Waiting for messages to be delivered...')
    producer.flush()
