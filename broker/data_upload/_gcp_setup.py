#!/usr/bin/env python3.7
# -*- coding: UTF-8 -*-

"""The ``gcp_setup`` module sets up a GCP project for use with the ``broker``
package.

This module is used to set up the necessary sinks and BigQuery datasets used by
the parent package. It does not create any BigQuery data tables, as those are
created automatically if / when required.
"""

import os

if 'RTD_BUILD' not in os.environ:
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery, logging, storage

_tables = ('alert', 'candidate')


def _setup_big_query():
    """Create the necessary Big Query datasets if they do not already exist

    New data sets include: ``ztf_alerts``
    """

    bigquery_client = bigquery.Client()
    bigquery_client.create_dataset('ztf_alerts', exists_ok=True)


def _setup_buckets():
    """Create new storage buckets

    New buckets include:
      ``<project_id>_logging_bucket``
      ``<project_id>_ztf_images
    """

    storage_client = storage.Client()

    # Create bucket names
    project_id = os.environ['BROKER_PROJ_ID']
    logging_name = f'{project_id}_logging_bucket'
    ztf_image_name = f'{project_id}_logging_bucket'

    # Create buckets if the do not exist
    for bucket_name in (logging_name, ztf_image_name):
        try:
            storage_client.get_bucket(bucket_name)

        except NotFound:
            storage_client.create_bucket(bucket_name)


def _setup_logging_sinks():
    """Create sinks for exporting log entries to GCP

    This function assumes destination buckets have already been created.

    New Sinks include: ``broker_logging_sink``
    """

    # Create bucket name
    project_id = os.environ['BROKER_PROJ_ID']
    logging_bucket_name = f'{project_id}_logging_bucket'

    # Define logging sink
    logging_client = logging.Client()
    logging_sink_name = 'logging_sink'
    logging_filter = None
    destination = f'storage.googleapis.com/{logging_bucket_name}'
    sink = logging_client.sink(
        logging_sink_name,
        logging_filter,
        destination)

    # Create sink if not exists
    if not sink.exists():
        sink.create()


def setup_gcp():
    """Create and setup GCP products for the ``broker`` package to connect to

    New data sets include: ``ztf_alerts``

    New buckets include: ``<project_id>_logging_bucket``

    New Sinks include: ``broker_logging_sink``
    """

    _setup_big_query()
    _setup_buckets()
    _setup_logging_sinks()


def get_bq_schema(client=None, tables=_tables, data_set='ztf_alerts'):
    """Return the current backend BigQuery schema to as a dictionary

    By default export the schema for all tables used by this package

    Args:
        client    (Client): A BigQuery client (Optional)
        tables (list[str]): Table names to export schemas for (Optional)
        data_set     (str): Name of the GCP data set (Optional)

    Returns:
        A dictionary with the schema for each table in the BigQuery dataset
    """

    if client is None:
        client = bigquery.Client()

    data_set = client.get_dataset(data_set)

    schema_out = {}
    for table_name in tables:
        table = client.get_table(
            f'{data_set.project}.{data_set.dataset_id}.{table_name}')
        table_schema = []
        for field in table.schema:
            field_dict = {
                'name': field.name,
                'field_type': field.field_type,
                'mode': field.mode,
                'description': field.description
            }

            table_schema.append(field_dict)

        schema_out[table_name] = table_schema

    return schema_out
