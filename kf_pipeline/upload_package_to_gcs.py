# %%

# -- -------------------------------------------------------------------------
# -- Author:      Behzad Asadi                                                                                  
# -- Description: Creating Python Package for Vertex AI Custom Job   
# -- -------------------------------------------------------------------------
# -- VERSIONS       DATE            WHO                      DESCRIPTION                                                
# -- 1.00           02/04/2024      Behzad Asadi             Initial release        
# -- -------------------------------------------------------------------------

# %% importing libraries

import os
from datetime import datetime
from config import config
from google.cloud import aiplatform
from google.cloud import storage
from gcp_utils import download_blob, upload_blob, list_blobs

# %% parameters

params = config(principle='service', model_id='amm-363')
#params = config(principle='user', s_number='s125591', model_id='amm-344')

# download_blob(params['bucket_name'], params["model_python_package_gcs_blob_name"], os.path.dirname(os.path.realpath('__file__')) + '/test.tar.gz')
# %%

upload_blob(params['bucket_name'], params["model_python_package_local_file_path"], params["model_python_package_gcs_blob_name"])
list_blobs(params['bucket_name'], prefix='amm-344')

#download_blob(params['bucket_name'],'amm-317/code/package/model_scoring/model_scoring-0.0.1.tar.gz','./model_scoring-0.0.1.tar.gz')
# %%
