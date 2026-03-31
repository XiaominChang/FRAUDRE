
import datetime
import pytz
import os
from pathlib import Path
from utils.data_postgres.data_postgres import data_postgres
from utils.shared_repo.gen_utils import save_data
from utils.model_config.input_config import SYNDICATE_CONFIG, DBENVS
from utils.model_logging.model_logging import log_utility


# Get absolute path to the `queries` folder
ROOT_DIR = Path(__file__).resolve().parents[2]

def data_extraction(scoring_config :dict):
    # Logging setup
    logger = log_utility(model_id = scoring_config.get("model_id", "unknown_model"), component = 'data_extraction')
    logger.info("Starting data extraction process")

    local_write = scoring_config.get('local_write', False)
    data_path = scoring_config['data_path'] 
    input_env =  scoring_config['input_env']
    # query lodgement date bounds
    start_time = scoring_config.get('start_date', "2018-01-01")
    end_time = scoring_config.get('end_date', datetime.datetime.now(pytz.timezone('Australia/Sydney')).strftime('%Y-%m-%d'))
    logger.info(f"Data extraction date range: {start_time} to {end_time}")
    # Bucket and path setup
    principle = scoring_config.get('principle', 'user')
    s_number = scoring_config.get('s_number', 's745998')
    project_id = scoring_config.get('project_id', 'ria-vul-bbcc')
    if principle == 'user':
        bucket_name = f"{project_id}-aap-{input_env}-{s_number}-bucket"
    elif principle == "service":
        bucket_name = f"{project_id}-aap-{input_env}-model-bucket"

    # load queries
    try:
        query_file = "ctp_node_exp.sql" 
        file_path = os.path.join(ROOT_DIR, "queries", query_file)
        # file_path = r"./queries/ctp_node_exp.sql"
        with open(file_path, "r") as fd:
                query = fd.read().format(START_DATE= start_time, END_DATE= end_time)

        # Execute the SQL statement to retrieve data from the temporary table
        node_data = data_postgres(db = DBENVS[input_env]['db'], dbase=DBENVS[input_env]['dbase'], fn = "get_new", query = query)
        
        # doctor-lawyer connection
        query_file = "ctp_doctor_lawyer_pair.sql"
        file_path = os.path.join(ROOT_DIR, "queries", query_file)  
        with open(file_path, "r") as fd:
            query = fd.read().format()
        doctor_lawyer_data = data_postgres(db = "EDH_PROD", dbase='iadpprod', fn = "get_new", query = query)
        
        #doctor-psychologist connection
        query_file = "ctp_doctor_psych_pair.sql"
        file_path = os.path.join(ROOT_DIR, "queries", query_file)
        with open(file_path, "r") as fd:
            query = fd.read().format()
        doctor_psych_data = data_postgres(db = "EDH_PROD", dbase='iadpprod', fn = "get_new", query = query)

        #doctor-repairer connection
        query_file = "ctp_doctor_repairer_pair.sql"
        file_path = os.path.join(ROOT_DIR, "queries", query_file)
        with open(file_path, "r") as fd:
            query = fd.read().format()
        doctor_repairer_data = data_postgres(db = "EDH_PROD", dbase='iadpprod', fn = "get_new", query = query)

        # vehicle connection
        query_file = "ctp_vehicle_connection.sql"
        file_path = os.path.join(ROOT_DIR, "queries", query_file)
        with open(file_path, "r") as fd:
            query = fd.read().format()
        vehicle_data = data_postgres(db = "EDH_PROD", dbase='iadpprod', fn = "get_new", query = query)
        logger.info(f"SQL execution completed")

    except Exception as e:
        logger.error(f"Failed to execute SQL query: {e}")
        raise
    
    # Save to local if specified in config
    if local_write:
       try: 
            save_data (node_data, data_path, 'node_data', bucket_name, data_extension="csv")          
            save_data(doctor_lawyer_data,  data_path, 'doctor_lawyer_data', bucket_name, data_extension="csv")
            save_data(doctor_psych_data,   data_path, 'doctor_psych_data',  bucket_name, data_extension="csv")
            save_data(doctor_repairer_data, data_path,'doctor_repairer_data', bucket_name, data_extension="csv")
            save_data(vehicle_data, data_path, 'vehicle_data', bucket_name,  data_extension="csv")

            logger.info(f"Data saved locally")
       except Exception as e:
            logger.error(f"Failed to save data locally: {e}")
            raise 
    
    logger.info("Data extraction completed")

    # Return the retrieved data
    return node_data, doctor_lawyer_data, doctor_psych_data, doctor_repairer_data, vehicle_data

if __name__ == "__main__":
    score_config = SYNDICATE_CONFIG
    data_extraction(score_config)
