"""
-------------------------------------------------------------------------------
Author: Tharu Geethz
Created: 2025-04-27
Description:
    Entry point for the eReserve Data Pipeline.

    This script initializes logging and starts the job scheduler that periodically
    fetches data from the eReserve API and inserts it into the SQL Server database.
-------------------------------------------------------------------------------
"""
import logging
import logger
from fedpipeline.job_scheduler import start_scheduler

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logging.info("Pipeline starting...")
    start_scheduler()


