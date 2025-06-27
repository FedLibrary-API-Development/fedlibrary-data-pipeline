import pyodbc
import logging
from fedpipeline.config import DB_CONFIG

conn_str = (
    f"DRIVER={{{DB_CONFIG['DRIVER']}}};"
    f"SERVER={DB_CONFIG['SERVER']};"
    f"DATABASE={DB_CONFIG['DATABASE']};"
    f"UID={DB_CONFIG['UID']};"
    f"PWD={DB_CONFIG['PWD']}"
)

def insert_records(query, records, entity_name):
    logging.info(f"Inserting {len(records)} {entity_name} records to DB.")
    if not records:
        logging.warning(f"No {entity_name} records to insert.")
        return
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            for record in records:
                try:
                    logging.info(record)
                    cursor.execute(query, record)
                except Exception as rec_err:
                    logging.error(f"Failed to insert record into {entity_name}: {record} | Error: {rec_err}")
            conn.commit()                
        logging.info(f"{len(records)} {entity_name} records insertion ended.")
    except Exception as e:
        logging.error(f"Failed to insert {entity_name} records: {e}")
