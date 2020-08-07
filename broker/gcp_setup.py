#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""The ``gcp_setup`` module sets up services on the Google Cloud Platform (GCP)
for use by the parent package. Setup tasks are grouped together into functions
that configure resources required to a particular task. Resources configured
by a given function are listed in the documentation.

Usage Example
-------------

.. code-block:: python
   :linenos:

   from broker import gcp_setup

   # Set of logging for the parent package
   gcp_setup.setup_logging_sinks

   # Set up resources used by the test suite
   gcp_setup.setup_testsuite_resources

   # Setup resources used when ingesting ZTF alerts
   gcp_setup.setup_ztf_ingest_resources

Module Documentation
--------------------
"""

import os
from pathlib import Path

if not os.getenv('GPB_OFFLINE', False):
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery, pubsub, logging, storage

_tables = ('alerts', 'test_GCS_to_BQ')
bigquery_client = bigquery.Client()
storage_client = storage.Client()


def _create_bucket(bucket: str) -> None:
    """Create a storage bucket if it does not already exist:

    Args:
       bucket: Name of the bucket it it does not
    """

    try:
        storage_client.get_bucket(bucket)

    except NotFound:  # Create buckets if they do not exist
        storage_client.create_bucket(bucket)


def setup_logging_sinks() -> None:
    """Create sinks for exporting log entries to GCP

    New sinks include:
      - ``broker_logging_sink``
    """

    # Define logging sink
    logging_client = logging.Client()
    sink = logging_client.sink('logging_sink')

    # Create sink if not exists
    if not sink.exists():
        sink.create()


def setup_testsuite_resources() -> None:
    """Set up GCP resources necessary for running the test suite

    New datasets include:
      - ``testing_dataset``

    New buckets [files] include:
      - ``<PROJECT_ID>_testing_bucket``  [``ztf_3.3_validschema_1154446891615015011.avro``]
    """

    bigquery_client.create_dataset('testing_dataset', exists_ok=True)

    testing_bucket = f'{os.getenv("GOOGLE_CLOUD_PROJECT")}_testing_bucket'
    _create_bucket(testing_bucket)

    # Upload any files
    testing_files = ['ztf_3.3_validschema_1154446891615015011.avro']
    for filename in testing_files:
        bucket = storage_client.get_bucket(testing_bucket)
        blob = bucket.blob(filename)
        inpath = Path('tests/test_alerts') / filename
        with inpath.open('rb') as infile:
            blob.upload_from_file(infile)


def setup_ztf_ingest_resources() -> None:
    """Set up GCP resources for consuming ZTF alerts

    New datasets include:
      - ``ztf_alerts``

    New buckets [files] include:
      - ``<PROJECT_ID>_ztf_alert_avro_bucket``

    New topics [subscriptions] include:
        ``ztf_alert_data``
        ``ztf_alert_avro_in_bucket``
        ``ztf_alerts_in_BQ``
        ``test_alerts_in_BQ``
        ``test_alerts_PS_publish`` [``test_alerts_PS_subscribe``]

    """

    bigquery_client.create_dataset('ztf_alerts', exists_ok=True)

    topics = {  # '<topic_name>': ['<subscription_name>', ]
        'ztf_alert_data': [],
        'ztf_alert_avro_in_bucket': [],
        'ztf_alerts_in_BQ': [],
        'test_alerts_in_BQ': [],
        'test_alerts_PS_publish': ['test_alerts_PS_subscribe']
    }

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    publisher = pubsub.PublisherClient()
    subscriber = pubsub.SubscriberClient()
    for topic, subscriptions in topics.items():
        topic_path = publisher.topic_path(project_id, topic)

        # Create the topic
        try:
            publisher.get_topic(topic_path)
        except NotFound:
            publisher.create_topic(topic_path)

        # Create any subscriptions:
        for sub_name in subscriptions:
            sub_path = subscriber.subscription_path(project_id, sub_name)
            try:
                subscriber.get_subscription(sub_path)
            except NotFound:
                subscriber.create_subscription(sub_path, topic_path)
