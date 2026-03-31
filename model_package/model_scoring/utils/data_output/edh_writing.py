import numpy as np
import pandas as pd
from utils.data_postgres.data_postgres import data_postgres
from utils.shared_repo.gen_utils import load_data
from utils.model_config.input_config import SYNDICATE_CONFIG, DBENVS
from utils.model_logging.model_logging import log_utility

logger = log_utility(model_id = 'AMM-395', component = 'data_output')

# extract data from edh
def extract_scored_data(db, dbase, schema, obj):

    logger.info('Started Sub-task to Extract Scored Data Latest from EDH')

    exists_latest = data_postgres(db, dbase, fn='exists',  query= None, schema=schema, name = obj)

    if exists_latest:
        scored_data_latest = data_postgres(db, dbase, fn='get',  query = ("select * from %s.%s" %(schema, obj)))
    else: 
        logger.info('Latest data does not exist!')
        scored_data_latest = None

    return scored_data_latest


# create output for model scoring
def create_output(scored_current, scored_latest, model_name, compare_cols):
    
    logger.info('Started task to Create Output Tables for Upload to EDH')
    
    if scored_latest is None:
        df_diff = scored_current
        scored_latest_new = scored_current
    else:
        scored_current['community_anomaly_score'] = round(scored_current['community_anomaly_score'],3)
        scored_latest['community_anomaly_score'] = round(scored_latest['community_anomaly_score'],3)
        df = pd.merge(scored_current, scored_latest, on=compare_cols, how='left', indicator='Exist', suffixes=('', '_y'))
        df_diff = df.loc[df['Exist'] != 'both', scored_current.columns]
        df_diff.reset_index(drop=True, inplace=True)

    scored_latest_new = scored_current

    audit = pd.DataFrame({'created_timestamp':[scored_current.loc[0,'insert_timestamp']], 'row_count':[df_diff.shape[0]]})

    df_execution = pd.DataFrame({'model_name':model_name, 'last_successful_runtime':[scored_current.loc[0,'insert_timestamp']]})

    return scored_latest_new, df_diff, audit, df_execution

# upload scored data to edh
def upload_data_to_edh(db, dbase, schema, tables, objs):
  
    logger.info('Started Uploading Data to EDH - iadpprod')

    logger.info('Started sub-task to upload data to %s.%s' %(schema, objs['scores'] ))
    out_main = data_postgres(db, dbase, fn='trunc_write',  query= None, schema=schema, name = objs['scores'],  value = tables['ctp_communities_scored'] , method='append')


    logger.info('Started sub-task to upload data to %s.%s' %(schema, objs['scores_history']))
    out_hist = data_postgres(db, dbase, fn='write',  query= None, schema=schema, name = objs['scores_history'],  value = tables['ctp_communities_scored_history_diff'] , method='append')


    logger.info('Started sub-task to upload data to %s.%s' %(schema, objs['scores_audit']))
    out_audit = data_postgres(db, dbase, fn='write', query= None, schema=schema, name=objs['scores_audit'], value = tables['audit'], method='append')

   

def main (scoring_config: dict, scored_data_out: pd.DataFrame = None):
    """
    Upload scored data to EDH if iadpprod_write is True.

    Args:
        db_config (dict): Database configuration dictionary.
        scored_data_out (pd.DataFrame): DataFrame containing the scored data to be uploaded.
    """
    logger = log_utility(model_id = scoring_config.get("model_id", "unknown_model"), component = 'data_output')
    logger.info("Starting data output process")
    # Extract configuration parameters

    data_path = scoring_config['data_path']
    model_name = scoring_config['model_name']
    iadpprod_write = scoring_config['iadpprod_write']
    input_env =  scoring_config['input_env']
    output_env =  scoring_config['output_env']
    db_config = DBENVS[output_env]
    
    # Bucket and path setup
    principle = scoring_config.get('principle', 'user')
    s_number = scoring_config.get('s_number', 's745998')
    project_id = scoring_config.get('project_id', 'ria-vul-bbcc')
    if principle == 'user':
        bucket_name = f"{project_id}-aap-{input_env}-{s_number}-bucket"
    elif principle == "service":
        bucket_name = f"{project_id}-aap-{input_env}-model-bucket"

    objs_iadpprod = {'scores':model_name+'_reporting', 
                    'scores_history':model_name+'_history', 
                    'scores_audit':model_name +'_audit'}

    if scored_data_out is None:
        scored_data_out = load_data(data_path, 'ctp_communities_scored', bucket_name, data_extension='pkl')
    scored_current = scored_data_out.copy()

    if scored_current.shape[0] != 0:
        try:
            scored_latest  = extract_scored_data(db_config['db'], db_config['dbase'], db_config['schema'], objs_iadpprod['scores'])

            compare_cols =  [
            'community_id',
            'claim_exposure_1',  'exposure_1_status',
            'exposure_1_contact_name', 'exposure_1_contact_number', 'exposure_1_investigation_flag',
            'claim_exposure_2', 'exposure_2_status',
            'exposure_2_contact_name', 'exposure_2_contact_number', 'exposure_2_investigation_flag',
            'relationship_type', 'relationship_party', 'party_contact',
            'rank_by_community_anomaly_score', 'investigation_rate','community_size'
            ]

            scored_latest_new, df_diff, audit, df_execution = create_output(scored_current, scored_latest, model_name, compare_cols)

            output= {'ctp_communities_scored':scored_latest_new,
                    'ctp_communities_scored_history_diff':df_diff,
                    'audit':audit}
            logger.info(f"Creating Model Outputs Completed")
        except(Exception) as error:
            logger.error(f"Creating Model Outputs Failed: {error}")
            raise

    if iadpprod_write:
        try:
            upload_data_to_edh( db    = db_config['db'], 
                            dbase     = db_config['dbase'],
                            schema    = db_config['schema'], 
                            tables    = output,
                            objs = objs_iadpprod)
      # Logs
            logger.info(f"Model Outputs Exporting to IADPPROD Completed")
        except(Exception) as error:
            logger.error(f"Model Outputs Exporting to IADPPROD Failed: {error}")
        
# Example usage:
if __name__ == "__main__":
        score_config = SYNDICATE_CONFIG
        scored_data_out = main(score_config)

