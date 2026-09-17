# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e5e6e605-c114-4d28-b79b-15b0d0196e4a",
# META       "default_lakehouse_name": "lh_Bronze_Proto",
# META       "default_lakehouse_workspace_id": "03539bd8-5c87-4267-b2bc-eefbcf50de5d",
# META       "known_lakehouses": [
# META         {
# META           "id": "e5e6e605-c114-4d28-b79b-15b0d0196e4a"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# ## Global Imports

# CELL ********************

# %pip install PyPDF2 bs4 cachetools

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F, Column, Row, DataFrame, Window as Window

# -- Removed from Global Imports and moved to US Gov - Full Text Extraction, where they are used 
# from tenacity import retry, stop_after_attempt, wait_exponential
# from tqdm.notebook import tqdm as progressbar
# from cachetools import TTLCache
# from bs4 import BeautifulSoup
# from PyPDF2 import PdfReader

from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from functools import reduce, wraps
import xml.etree.ElementTree as ET
from pyspark.sql.types import *
from operator import add
import numpy as np
import unicodedata
import requests
import fsspec
import delta
import time
import json
import math
import ast
import logging, traceback
import io
import re

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Global Variables

# CELL ********************

# Current date and time for partioning and timestamping
year = datetime.today().strftime('%Y')
month = datetime.today().strftime('%m')
day = datetime.today().strftime('%d')
timestamp = datetime.now().strftime('%Y%m%dT%H%M%SZ')
date_path = f"{year}/{month}/{day}"

# -- Synapse - Infrastructure configuration
# storage_account_name = "ketmew1synapsedevomcdl"
# container_name = "ketmew1synapsedevomcfsn"
# linked_service_storage = "ketmew1synapsedev-WorkspaceDefaultStorage"
# keyvault_name = "ketmew1synapsedev-kv"
# linked_service_keyvault = "ls_keyvault"

# -- Fabric - Workspace / lakehouse configuration
# Prefer GUIDs over names: they survive renames and avoid the space in "Sensing Test".
WORKSPACE    = "Sensing Test"        # or the workspace GUID
LAKEHOUSE    = "lh_Bronze_Proto"     # or the lakehouse item GUID
KEYVAULT_URL = "https://kvket05muw1dev-krsensing.vault.azure.net/"   # migrated Dev vault

# -- Synapse - Data lake paths
# abfss_base_url = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"
# master_Path = f"{abfss_base_url}/synapse/workspaces/ketmew1synapsedev/warehouse/master"
# test_Path = f"{abfss_base_url}/synapse/workspaces/ketmew1synapsedev/warehouse/master"

# -- Fabric - OneLake roots for the lakehouse
# NOTE: building the abfss authority from the workspace NAME breaks when the name
# contains a space (e.g. "Sensing Test" -> "abfss://Sensing has invalid authority").
# -- Fabric (name-based, breaks on spaces - kept for reference):
# onelake     = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE}.Lakehouse"
# -- Fabric (GUID-based, space-safe): resolve the workspace + lakehouse GUIDs from the
#    lakehouse name (which has no space) at runtime, so paths survive renames too.
_lh = notebookutils.lakehouse.get(LAKEHOUSE)
WORKSPACE_ID = _lh["workspaceId"]
LAKEHOUSE_ID = _lh["id"]
onelake     = (_lh.get("properties", {}) or {}).get("abfsPath") \
    or f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}.Lakehouse"
files_root  = f"{onelake}/Files"     # unmanaged files (raw drops, archive)
tables_root = f"{onelake}/Tables"    # managed Delta tables (normally referenced by name, not path)

# -- Synapse - Toggle for testing
# Switches to test_path and prevents moving CSV files to Archive folder
# debug_mode = False
# master_Path = test_Path if debug_mode else master_Path

# -- Fabric - Toggle for testing
# Prevents moving CSV files to Archive folder (no path swap needed with managed tables)
debug_mode = False

# -- Synapse - Layer paths
# meta_Path = f"{master_Path}/meta"
# landing_Path = f"{master_Path}/landing"
# bronze_Path = f"{master_Path}/bronze"
# silver_Path = f"{master_Path}/silver"
# gold_Path = f"{master_Path}/gold"
# risk_Path = f"{master_Path}/risk"
# extendi_Path = f"{master_Path}/extendi"

# -- Fabric - Layer destinations
# Medallion layers are now MANAGED TABLES in the schema-enabled lakehouse.
# Reference by name, e.g.  spark.table(f"{BRONZE}.talkwalker")
#                          df.write.mode("overwrite").saveAsTable(f"{BRONZE}.talkwalker")
META    = "meta"
BRONZE  = "bronze"
SILVER  = "silver"
GOLD    = "gold"
RISK    = "risk"
EXTENDI = "extendi"

# File-based zones (raw drops, archive) stay as OneLake Files paths
landing_Path = f"{files_root}/landing"
archive_Path = f"{files_root}/archive"

# Logging level
log_level = "INFO"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## get_source_paths

# CELL ********************

def get_source_paths(source_name: str, topic_name: str) -> dict:
    """
    Get standardized paths for a data source

    Args:
        source_name: Name of the source (e.g. 'glimpse', 'talkwalker')
        topic_name: Name of the specific topic

    Returns:
        Dictionary containing all relevant paths for the source:
        - All sources get 'json_path'
        - File-based sources get 'csv_path' and 'archive_path'
        - Talkwalker gets 'raw_path' and 'formatted_path' with date-based subdirectories
    """
    # -- Synapse - Base paths (parquet_path pointed at the bronze Delta *folder*)
    # paths = {
    #     'json_path': f'{landing_Path}/{source_name}/{topic_name}',
    #     'parquet_path': f'{bronze_Path}/{source_name}',
    # }

    # -- Fabric - Base paths (bronze is now a managed table; raw JSON still lands in Files)
    paths = {
        'json_path': f'{landing_Path}/{source_name}/{topic_name}',
        'bronze_table': f'{BRONZE}.{source_name}',   # write with df.write.saveAsTable(paths['bronze_table'])
    }

    # Add file-based paths if source type requires them (unchanged - OneLake Files)
    if source_name in ['glimpse', 'polimonitor']:
        paths.update({
            'csv_path': f"{landing_Path}/{source_name}/csvs",
            'archive_path': f"{landing_Path}/{source_name}/archive"
        })

    # Add API-specific paths if needed (unchanged - OneLake Files)
    if source_name == 'talkwalker':
        paths.update({
            'raw_path': f"{paths['json_path']}/raw/{date_path}",
            'formatted_path': f"{paths['json_path']}/formatted/{date_path}"
        })

    return paths

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Logging Configuration

# CELL ********************

# Update logging configuration
logger = logging.getLogger()
logger.setLevel(getattr(logging, log_level))

# Update formatter for all handlers
formatter = logging.Formatter(
    fmt='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
for handler in logger.handlers:
    handler.setFormatter(formatter)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
