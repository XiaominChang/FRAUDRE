"""
Model Pipeline Configuration
-----------------------------------
This configuration file sets up parameters and paths for running model.
Designed for reusability and collaboration across teams.
"""

import datetime
import pytz
import pathlib
import os

# ------------------------------------------------------------------------------
# Syndicate Detection Configurations
# ------------------------------------------------------------------------------

SYNDICATE_CONFIG = {
        #project metadata
        'principle': 'service',  # 0 'user' or 'service'
        's_number': 's123815',
        'project_id': 'dia-frd-0586',
        'model_name': 'claim_mr1298_ctp_syndicate_detection',
        #scoring model config
        'model_id': 'AMM-395',
        'local_write': True,
        'local_path': os.path.join(pathlib.Path(__file__).parent.parent.parent, 'data'),
        'data_path': 'amm-395/scoring_pipeline/data',
        'model_path': os.path.join(pathlib.Path(__file__).parent.parent.parent, 'trained_models'),
        'start_date': "2018-01-01",  # Default to 30 days ago
        'end_date': datetime.datetime.now(pytz.timezone('Australia/Sydney')).strftime('%Y-%m-%d'),
        'iadpprod_write': True,
        'input_env': 'prod',
        'output_env': 'prod_test'
}

# ------------------------------------------------------------------------------
# Database configurations for different environments
# ------------------------------------------------------------------------------

DBENVS = {
    'dev': {
        'db': 'EDH_PROD',
        'dbase': 'iadpprod',
        'schema': 'dl_aai_sqd_claim',
        'read_role': 'dl_aai_sqd_claim_read_role',
        'wrt_role': 'dl_aai_sqd_claim_wrt_role',
        'crt_role': 'dl_aai_sqd_claim_crt_role',
    },
    'prod': {
        'db': 'EDH_PROD',
        'dbase': 'iadpprod',
        'schema': 'mod_analytics',
        'read_role': 'mod_analytics_read_role',
        'wrt_role': 'mod_analytics_wrt_role',
        'crt_role': 'mod_analytics_crt_role',
    },
    'prod_test': {
        'db': 'EDH_PROD',
        'dbase': 'iadpprod',
        'schema': 'dl_aai_sqd_claim',
        'read_role': 'dl_aai_sqd_claim_read_role',
        'wrt_role': 'dl_aai_sqd_claim_wrt_role',
        'crt_role': 'dl_aai_sqd_claim_crt_role',
    },
    'sas_prod': {
        'db': 'EDH_PROD',
        'dbase': 'iadpsas',
        'schema': 'sas_analytics_ama',
        'read_role': 'sas_analytics_ama_read_role',
        'wrt_role': 'sas_analytics_ama_wrt_role',
        'crt_role': 'sys_sas_di',
    },
    'sas_preprod': {
        'db': 'EDH_NONPROD',
        'dbase': 'iapdsas_integration',
        'schema': 'sas_analytics_ama',
        'read_role': 'iapdsas_integration_sas_analytics_ama_read_role',
        'wrt_role': 'iapdsas_integration_sas_analytics_ama_wrt_role',
        'crt_role': 'iapdsas_integration_sas_analytics_ama_crt_role',
    },
    'sas_test': {
        'db': 'EDH_NONPROD',
        'dbase': 'iadpsas',
        'schema': 'sas_analytics_ama',
        'read_role': 'iadpsas_sas_analytics_ama_read_role',
        'wrt_role': 'iadpsas_sas_analytics_ama_wrt_role',
        'crt_role': 'iadpsas_sas_analytics_ama_crt_role',
    },
    'sas_dev': {
        'db': 'EDH_NONPROD',
        'dbase': 'iadpsas',
        'schema': 'sas_analytics_ama_dev',
        'read_role': 'iadpsas_sas_analytics_ama_dev_read_role',
        'wrt_role': 'iadpsas_sas_analytics_ama_dev_wrt_role',
        'crt_role': 'iadpsas_sas_analytics_ama_dev_crt_role',
    },
}