#!/usr/bin/env python3.7
# -*- coding: UTF-8 -*-

"""General utilities for common tasks performed across the ``broker`` package.
"""

import logging
import os
from pathlib import Path

import google.cloud as gcp
from google.auth.exceptions import DefaultCredentialsError


class RTDSafeImport:
    """Suppress DefaultCredentialsError when ``GPB_OFFLINE`` is defined

    When ``GPB_OFFLINE`` in the environment, suppress any credential errors and
    exit. THis allows Read The Docs to build without GCP authentication.
    """

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, traceback):
        if exc_type == DefaultCredentialsError:
            return 'GPB_OFFLINE' in os.environ


def get_ztf_data_dir():
    """Return the directory path where local ZTF alerts are stored"""

    if 'PGB_DATA_DIR' in os.environ:
        return Path(os.environ['PGB_DATA_DIR']) / 'ztf_archive'

    else:
        return Path(__file__).resolve().parent / 'data'


def setup_log(log_name, level='INFO'):
    """Create a logging object connected to google cloud

    Args:
        log_name (str): Name of the logger
        level    (str): Reporting level for the log

    Returns:
        A Logger object, or None
    """

    log = logging.Logger(log_name)
    log.setLevel(level)

    # Connect to GCP
    logging_client = gcp.logging.Client()
    handler = logging_client.get_default_handler()
    log.addHandler(handler)

    return log
