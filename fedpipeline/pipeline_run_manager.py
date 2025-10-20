import logging
import pyodbc
from datetime import datetime
from typing import Optional, Dict
from fedpipeline.config import DB_CONFIG

class PipelineRunHistoryManager:
    
    def __init__(self):
        self.conn_str = (
            f"DRIVER={{{DB_CONFIG['DRIVER']}}};"
            f"SERVER={DB_CONFIG['SERVER']};"
            f"DATABASE={DB_CONFIG['DATABASE']};"
            f"UID={DB_CONFIG['UID']};"
            f"PWD={DB_CONFIG['PWD']}"
        )
        self.current_run_id = None
    
    def is_first_run(self) -> bool:
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME = 'PipelineRunHistory'
                """)
                table_exists = cursor.fetchone()[0] > 0
                if not table_exists:
                    logging.warning("PipelineRunHistory table does not exist. Assuming first run.")
                    return True
                
                # Check for any successful runs
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM PipelineRunHistory 
                    WHERE status = 'SUCCESS'
                """)
                successful_runs = cursor.fetchone()[0]
                
                if successful_runs == 0:
                    logging.info("No successful pipeline runs found. This is the FIRST RUN.")
                    return True
                else:
                    logging.info(f"Found {successful_runs} successful pipeline run(s). This is a SUBSEQUENT RUN.")
                    return False
                    
        except Exception as e:
            logging.error(f"Error checking pipeline run history: {e}")
            logging.warning("Assuming first run due to error.")
            return True
    
    def start_run(self, is_initial_load: bool = False) -> Optional[int]:
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO PipelineRunHistory 
                    (run_start_time, status, is_initial_load)
                    VALUES (?, 'IN_PROGRESS', ?)
                """, (datetime.now(), 1 if is_initial_load else 0))
                
                # Get inserted run_id
                cursor.execute("SELECT @@IDENTITY")
                run_id = int(cursor.fetchone()[0])
                conn.commit()
                self.current_run_id = run_id
                logging.info(f"Pipeline run started with run_id: {run_id} (initial_load: {is_initial_load})")
                return run_id
                
        except Exception as e:
            logging.error(f"Failed to record pipeline run start: {e}")
            return None
    
    def end_run_success(self, run_id: int, metrics: Dict = None) -> bool:
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE PipelineRunHistory
                    SET 
                        run_end_time = ?,
                        status = 'SUCCESS'
                    WHERE run_id = ?
                """, (datetime.now(), run_id))
                conn.commit()  
                logging.info(f"Pipeline run {run_id} completed successfully!")
                if metrics:
                    logging.info(f"  Metrics: {metrics}")
                
                return True
                
        except Exception as e:
            logging.error(f"Failed to record pipeline run success: {e}")
            return False
    
    def end_run_failure(self, run_id: int, error_message: str, error_details: str = None) -> bool:
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE PipelineRunHistory
                    SET 
                        run_end_time = ?,
                        status = 'FAILED'
                    WHERE run_id = ?
                """, (datetime.now(), run_id))
                
                conn.commit()
                logging.error(f"Pipeline run {run_id} marked as FAILED: {error_message}")
                if error_details:
                    logging.error(f"  Error details: {error_details}")
                
                return True
                
        except Exception as e:
            logging.error(f"Failed to record pipeline run failure: {e}")
            return False
    
    def get_run_statistics(self) -> Dict:
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        COUNT(*) AS total_runs,
                        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
                        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
                        SUM(CASE WHEN status = 'IN_PROGRESS' THEN 1 ELSE 0 END) AS in_progress_runs,
                        MAX(CASE WHEN status = 'SUCCESS' THEN run_end_time END) AS last_success_time
                    FROM PipelineRunHistory
                """)
                row = cursor.fetchone()
                return {
                    'total_runs': row[0] or 0,
                    'successful_runs': row[1] or 0,
                    'failed_runs': row[2] or 0,
                    'in_progress_runs': row[3] or 0,
                    'last_success_time': row[4]
                }
                
        except Exception as e:
            logging.error(f"Failed to get run statistics: {e}")
            return {}
