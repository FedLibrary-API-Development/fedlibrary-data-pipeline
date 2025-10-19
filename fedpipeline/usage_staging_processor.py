import logging
import pyodbc
import time
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from contextlib import contextmanager
from fedpipeline.api_handler import fetch_data_from_api
from fedpipeline.config import API_CONFIG, PAGE_SIZE, DATE_FILTER_CONFIG, DB_CONFIG

class UsageStagingProcessor:
    def __init__(self):
        self.conn_str = (
            f"DRIVER={{{DB_CONFIG['DRIVER']}}};"
            f"SERVER={DB_CONFIG['SERVER']};"
            f"DATABASE={DB_CONFIG['DATABASE']};"
            f"UID={DB_CONFIG['UID']};"
            f"PWD={DB_CONFIG['PWD']}"
        )
        self.batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.metrics = {
            'start_time': None,
            'end_time': None,
            'records_processed': 0,
            'records_inserted': 0,
            'records_updated': 0,
            'records_skipped': 0,
            'duplicates_skipped': 0,
            'orphaned_records': 0,
            'total_missing_dependencies': 0,
            'missing_dependency_breakdown': {},
            'errors': []
        }
    
    def calculate_date_range(self) -> Tuple[Optional[str], Optional[str]]:
        # Calculate start and end dates based on config
        use_years_back = DATE_FILTER_CONFIG.get("USE_YEARS_BACK", False)
        
        if use_years_back:
            # Rolling window based on YEARS_BACK
            years_back = DATE_FILTER_CONFIG.get("YEARS_BACK", 0.5)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=int(365 * years_back))
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
            logging.info(f"Date range calculated (rolling window): {start_date_str} to {end_date_str} ({years_back} years back)")
            return start_date_str, end_date_str
        else:
            # Fixed date range from config
            start_date = DATE_FILTER_CONFIG.get("START_DATE")
            end_date = DATE_FILTER_CONFIG.get("END_DATE")
            logging.info(f"Date range from config (fixed): {start_date} to {end_date}")
            return start_date, end_date
    
    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = pyodbc.connect(self.conn_str)
            conn.autocommit = False
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def create_staging_tables(self, conn) -> bool:
        staging_ddl = {
            'ProcessingLog': f"""
                CREATE TABLE #ProcessingLog_{self.batch_id} (
                    log_id INT IDENTITY(1,1) PRIMARY KEY,
                    batch_id VARCHAR(50),
                    table_name VARCHAR(100),
                    operation VARCHAR(50),
                    record_count INT,
                    execution_time_ms INT,
                    created_at DATETIME DEFAULT GETDATE()
                )
            """,
            'ReadingListUsage': f"""
                CREATE TABLE #STAGE_ReadingListUsage_{self.batch_id} (
                    ereserve_id INT PRIMARY KEY,
                    list_id INT,
                    integration_user_id INT,
                    item_usage_count BIGINT,
                    created_at DATETIME,
                    updated_at DATETIME
                );
                CREATE INDEX IX_Stage_RLU_ListId ON #STAGE_ReadingListUsage_{self.batch_id}(list_id);
                CREATE INDEX IX_Stage_RLU_UserId ON #STAGE_ReadingListUsage_{self.batch_id}(integration_user_id);
            """,
            'ReadingListItemUsage': f"""
                CREATE TABLE #STAGE_ReadingListItemUsage_{self.batch_id} (
                    ereserve_id INT PRIMARY KEY,
                    item_id INT,
                    list_usage_id INT,
                    integration_user_id INT,
                    utilisation_count BIGINT,
                    created_at DATETIME,
                    updated_at DATETIME
                );
                CREATE INDEX IX_Stage_RLIU_ItemId ON #STAGE_ReadingListItemUsage_{self.batch_id}(item_id);
                CREATE INDEX IX_Stage_RLIU_ListUsageId ON #STAGE_ReadingListItemUsage_{self.batch_id}(list_usage_id);
            """,
            'ReadingUtilisation': f"""
                CREATE TABLE #STAGE_ReadingUtilisation_{self.batch_id} (
                    ereserve_id INT PRIMARY KEY,
                    integration_user_id INT,
                    item_id INT,
                    item_usage_id INT,
                    created_at DATETIME,
                    updated_at DATETIME
                );
                CREATE INDEX IX_Stage_RU_ItemUsageId ON #STAGE_ReadingUtilisation_{self.batch_id}(item_usage_id);
                CREATE INDEX IX_Stage_RU_ItemId ON #STAGE_ReadingUtilisation_{self.batch_id}(item_id);
            """
        }
        
        try:
            cursor = conn.cursor()
            for table_name, ddl in staging_ddl.items():
                start_time = time.time()
                for statement in ddl.split(';'):
                    if statement.strip():
                        cursor.execute(statement.strip())
                
                execution_time = int((time.time() - start_time) * 1000)
                logging.info(f"Created staging table: {table_name} in {execution_time}ms")
                
                cursor.execute(f"""
                    INSERT INTO #ProcessingLog_{self.batch_id} 
                    (batch_id, table_name, operation, record_count, execution_time_ms)
                    VALUES (?, ?, ?, ?, ?)
                """, (self.batch_id, table_name, 'CREATE_STAGING', 0, execution_time))
            
            conn.commit()
            return True
                
        except Exception as e:
            logging.error(f"Failed to create staging tables: {e}")
            self.metrics['errors'].append(f"Staging table creation: {e}")
            return False
    
    def fetch_all_pages_with_retry(self, url: str, max_retries: int = 3) -> List[Dict]:
        all_items = []
        retry_count = 0
        
        while url and retry_count <= max_retries:
            try:
                response = fetch_data_from_api(url)
                if not response:
                    retry_count += 1
                    if retry_count <= max_retries:
                        wait_time = 2 ** retry_count
                        logging.warning(f"API call failed, retrying in {wait_time}s... (attempt {retry_count}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        break
                
                data = response.json().get("data", [])
                links = response.json().get("links", {})
                
                if not data:
                    break
                    
                all_items.extend(data)
                url = links.get("next")
                retry_count = 0
                logging.info(f"Fetched {len(data)} items, total: {len(all_items)}")
                
            except Exception as e:
                retry_count += 1
                if retry_count <= max_retries:
                    wait_time = 2 ** retry_count
                    logging.warning(f"Error fetching data: {e}. Retrying in {wait_time}s... (attempt {retry_count}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Failed to fetch data after {max_retries} retries: {e}")
                    self.metrics['errors'].append(f"API fetch error: {e}")
                    break
        
        return all_items
    
    def build_filtered_url(self, base_url: str, start_date: str = None, end_date: str = None) -> str:
        url = f"{base_url}?page[size]={PAGE_SIZE}"
        
        if start_date and end_date:
            date_filter = f"BETWEEN {start_date} AND {end_date}"
            url += f"&filter[updated_at]={date_filter}"
            logging.info(f"Applied date filter: filter[updated_at]={date_filter}")
        elif start_date:
            date_filter = f">= {start_date}"
            url += f"&filter[updated_at]={date_filter}"
            logging.info(f"Applied date filter: filter[updated_at]={date_filter}")
        elif end_date:
            date_filter = f"<= {end_date}"
            url += f"&filter[updated_at]={date_filter}"
            logging.info(f"Applied date filter: filter[updated_at]={date_filter}")
        
        return url
    
    def bulk_load_to_staging(self, table_name: str, data: List[Tuple], conn, batch_size: int = 1000) -> bool:
        if not data:
            logging.warning(f"No data to load into {table_name}")
            return True
        
        table_queries = {
            'ReadingListUsage': f"INSERT INTO #STAGE_ReadingListUsage_{self.batch_id} (ereserve_id, list_id, integration_user_id, item_usage_count, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            'ReadingListItemUsage': f"INSERT INTO #STAGE_ReadingListItemUsage_{self.batch_id} (ereserve_id, item_id, list_usage_id, integration_user_id, utilisation_count, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            'ReadingUtilisation': f"INSERT INTO #STAGE_ReadingUtilisation_{self.batch_id} (ereserve_id, integration_user_id, item_id, item_usage_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)"
        }
        
        query = table_queries.get(table_name)
        if not query:
            logging.error(f"Unknown table name: {table_name}")
            return False
        
        try:
            cursor = conn.cursor()
            start_time = time.time()
            
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                cursor.executemany(query, batch)
                logging.info(f"Loaded batch {i//batch_size + 1} ({len(batch)} records) into {table_name}")
            
            execution_time = int((time.time() - start_time) * 1000)
            cursor.execute(f"""
                INSERT INTO #ProcessingLog_{self.batch_id} 
                (batch_id, table_name, operation, record_count, execution_time_ms)
                VALUES (?, ?, ?, ?, ?)
            """, (self.batch_id, table_name, 'BULK_LOAD', len(data), execution_time))
            
            conn.commit()
            logging.info(f"Successfully loaded {len(data)} records into {table_name} in {execution_time}ms")
            return True
                
        except Exception as e:
            logging.error(f"Failed to bulk load data into {table_name}: {e}")
            self.metrics['errors'].append(f"Bulk load {table_name}: {e}")
            return False
    
    def validate_dependencies_from_db(self, conn) -> Dict[str, int]:
        # Check if required parent records exist in database
        validation_queries = {
            'ReadingListUsage_missing_lists': f"""
                SELECT COUNT(DISTINCT s.list_id)
                FROM #STAGE_ReadingListUsage_{self.batch_id} s
                WHERE NOT EXISTS (SELECT 1 FROM ReadingList WHERE ereserve_id = s.list_id)
            """,
            'ReadingListUsage_missing_users': f"""
                SELECT COUNT(DISTINCT s.integration_user_id)
                FROM #STAGE_ReadingListUsage_{self.batch_id} s
                WHERE NOT EXISTS (SELECT 1 FROM IntegrationUser WHERE ereserve_id = s.integration_user_id)
            """,
            'ReadingListItemUsage_missing_items': f"""
                SELECT COUNT(DISTINCT s.item_id)
                FROM #STAGE_ReadingListItemUsage_{self.batch_id} s
                WHERE NOT EXISTS (SELECT 1 FROM ReadingListItem WHERE ereserve_id = s.item_id)
            """,
            'ReadingListItemUsage_missing_list_usage': f"""
                SELECT COUNT(DISTINCT s.list_usage_id)
                FROM #STAGE_ReadingListItemUsage_{self.batch_id} s
                WHERE NOT EXISTS (SELECT 1 FROM ReadingListUsage WHERE ereserve_id = s.list_usage_id)
            """,
            'ReadingUtilisation_missing_item_usage': f"""
                SELECT COUNT(DISTINCT s.item_usage_id)
                FROM #STAGE_ReadingUtilisation_{self.batch_id} s
                WHERE NOT EXISTS (SELECT 1 FROM ReadingListItemUsage WHERE ereserve_id = s.item_usage_id)
            """
        }
        
        missing_counts = {}
        cursor = conn.cursor()
        total_missing = 0
        
        for check_name, sql in validation_queries.items():
            cursor.execute(sql)
            count = cursor.fetchone()[0]
            missing_counts[check_name] = count
            total_missing += count
            
            if count > 0:
                logging.warning(f"{check_name}: {count} missing parent IDs")
        
        if total_missing == 0:
            logging.info("All parent records exist in database - no dependencies missing")
        else:
            logging.warning(f"Total missing parent records: {total_missing}")
            logging.warning("Affected usage records will be skipped during transfer")
        
        return missing_counts
    
    def finalize_staging_to_main(self, conn) -> bool:
        # Transfer data from staging to main tables
        transfer_queries = [
            {
                'name': 'ReadingListUsage',
                'merge_query': f"""
                MERGE ReadingListUsage AS target
                USING (
                    SELECT s.ereserve_id, s.list_id, s.integration_user_id, 
                           s.item_usage_count, s.created_at, s.updated_at
                    FROM #STAGE_ReadingListUsage_{self.batch_id} s
                    -- Only merge if parent records exist in DB
                    WHERE EXISTS (SELECT 1 FROM ReadingList WHERE ereserve_id = s.list_id)
                      AND EXISTS (SELECT 1 FROM IntegrationUser WHERE ereserve_id = s.integration_user_id)
                ) AS source
                ON target.ereserve_id = source.ereserve_id
                WHEN MATCHED THEN
                    UPDATE SET 
                        list_id = source.list_id,
                        integration_user_id = source.integration_user_id,
                        item_usage_count = source.item_usage_count,
                        created_at = source.created_at,
                        updated_at = source.updated_at
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (ereserve_id, list_id, integration_user_id, 
                            item_usage_count, created_at, updated_at)
                    VALUES (source.ereserve_id, source.list_id, source.integration_user_id,
                            source.item_usage_count, source.created_at, source.updated_at);
                """,
                'count_existing_query': f"""
                SELECT COUNT(*)
                FROM #STAGE_ReadingListUsage_{self.batch_id} s
                WHERE EXISTS (
                    SELECT 1 FROM ReadingListUsage r 
                    WHERE r.ereserve_id = s.ereserve_id
                )
                """
            },
            {
                'name': 'ReadingListItemUsage',
                'merge_query': f"""
                MERGE ReadingListItemUsage AS target
                USING (
                    SELECT s.ereserve_id, s.item_id, s.list_usage_id, 
                           s.integration_user_id, s.utilisation_count, s.created_at, s.updated_at
                    FROM #STAGE_ReadingListItemUsage_{self.batch_id} s
                    WHERE EXISTS (SELECT 1 FROM ReadingListItem WHERE ereserve_id = s.item_id)
                      AND EXISTS (SELECT 1 FROM ReadingListUsage WHERE ereserve_id = s.list_usage_id)
                      AND EXISTS (SELECT 1 FROM IntegrationUser WHERE ereserve_id = s.integration_user_id)
                ) AS source
                ON target.ereserve_id = source.ereserve_id
                WHEN MATCHED THEN
                    UPDATE SET 
                        item_id = source.item_id,
                        list_usage_id = source.list_usage_id,
                        integration_user_id = source.integration_user_id,
                        utilisation_count = source.utilisation_count,
                        created_at = source.created_at,
                        updated_at = source.updated_at
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (ereserve_id, item_id, list_usage_id, 
                            integration_user_id, utilisation_count, created_at, updated_at)
                    VALUES (source.ereserve_id, source.item_id, source.list_usage_id,
                            source.integration_user_id, source.utilisation_count, source.created_at, source.updated_at);
                """,
                'count_existing_query': f"""
                SELECT COUNT(*)
                FROM #STAGE_ReadingListItemUsage_{self.batch_id} s
                WHERE EXISTS (
                    SELECT 1 FROM ReadingListItemUsage r 
                    WHERE r.ereserve_id = s.ereserve_id
                )
                """
            },
            {
                'name': 'ReadingUtilisation',
                'merge_query': f"""
                MERGE ReadingUtilisation AS target
                USING (
                    SELECT s.ereserve_id, s.integration_user_id, s.item_id, 
                           s.item_usage_id, s.created_at, s.updated_at
                    FROM #STAGE_ReadingUtilisation_{self.batch_id} s
                    WHERE EXISTS (SELECT 1 FROM ReadingListItem WHERE ereserve_id = s.item_id)
                      AND EXISTS (SELECT 1 FROM ReadingListItemUsage WHERE ereserve_id = s.item_usage_id)
                      AND (s.integration_user_id IS NULL 
                           OR EXISTS (SELECT 1 FROM IntegrationUser WHERE ereserve_id = s.integration_user_id))
                ) AS source
                ON target.ereserve_id = source.ereserve_id
                WHEN MATCHED THEN
                    UPDATE SET 
                        integration_user_id = source.integration_user_id,
                        item_id = source.item_id,
                        item_usage_id = source.item_usage_id,
                        created_at = source.created_at,
                        updated_at = source.updated_at
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (ereserve_id, integration_user_id, item_id, 
                            item_usage_id, created_at, updated_at)
                    VALUES (source.ereserve_id, source.integration_user_id, source.item_id,
                            source.item_usage_id, source.created_at, source.updated_at);
                """,
                'count_existing_query': f"""
                SELECT COUNT(*)
                FROM #STAGE_ReadingUtilisation_{self.batch_id} s
                WHERE EXISTS (
                    SELECT 1 FROM ReadingUtilisation r 
                    WHERE r.ereserve_id = s.ereserve_id
                )
                """
            }
        ]
        
        try:
            cursor = conn.cursor()
            
            for transfer_info in transfer_queries:
                table_name = transfer_info['name']
                merge_query = transfer_info['merge_query']
                count_existing_query = transfer_info['count_existing_query']
                start_time = time.time()
                
                # Count records in staging
                cursor.execute(f"SELECT COUNT(*) FROM #STAGE_{table_name}_{self.batch_id}")
                staging_count = cursor.fetchone()[0]
                
                # Count existing records (will be updated)
                cursor.execute(count_existing_query)
                existing_count = cursor.fetchone()[0]
                
                cursor.execute(merge_query)
                rows_affected = cursor.rowcount
                rows_inserted = max(0, rows_affected - existing_count)
                rows_updated = min(rows_affected, existing_count)
                rows_skipped = staging_count - rows_affected  # Records with missing parent FKs
                
                execution_time = int((time.time() - start_time) * 1000)
                
                self.metrics['records_inserted'] += rows_inserted
                self.metrics['records_skipped'] += rows_skipped
                self.metrics['orphaned_records'] += rows_skipped

                if 'records_updated' not in self.metrics:
                    self.metrics['records_updated'] = 0
                self.metrics['records_updated'] += rows_updated
                logging.info(f"Merged {rows_affected}/{staging_count} records to {table_name} in {execution_time}ms")
                
                if rows_inserted > 0:
                    logging.info(f"  - {rows_inserted} new records inserted")
                if rows_updated > 0:
                    logging.info(f"  - {rows_updated} existing records updated")
                if rows_skipped > 0:
                    logging.warning(f"  - {rows_skipped} records skipped (missing parent records)")
                
                cursor.execute(f"""
                    INSERT INTO #ProcessingLog_{self.batch_id} 
                    (batch_id, table_name, operation, record_count, execution_time_ms)
                    VALUES (?, ?, ?, ?, ?)
                """, (self.batch_id, table_name, 'UPSERT_MERGE', rows_affected, execution_time))
            
            conn.commit()
            logging.info(f"Staging transfer complete: {self.metrics['records_inserted']} inserted, {self.metrics.get('records_updated', 0)} updated, {self.metrics['records_skipped']} skipped")
            return True
                
        except Exception as e:
            logging.error(f"Failed to transfer staging data to main tables: {e}")
            self.metrics['errors'].append(f"Staging transfer: {e}")
            return False
    
    def process_with_staging(self) -> Dict:
        self.metrics['start_time'] = datetime.now()
        
        try:
            with self.get_connection() as conn:
                if not self.create_staging_tables(conn):
                    raise Exception("Failed to create staging tables")
                
                start_date, end_date = self.calculate_date_range()
                self.metrics['date_range'] = {'start': start_date, 'end': end_date}
                
                # Fetch ReadingListUsage
                rlu_url = self.build_filtered_url(
                    API_CONFIG['READING_LIST_USAGE_URL'], 
                    start_date, 
                    end_date
                )
                
                rlu_data = self.fetch_all_pages_with_retry(rlu_url)
                rlu_formatted = [(
                    item.get("id"), item["attributes"].get("list-id"),
                    item["attributes"].get("integration-user-id"), item["attributes"].get("item-usage-count"),
                    item["attributes"].get("created-at"), item["attributes"].get("updated-at")
                ) for item in rlu_data]
                
                if not self.bulk_load_to_staging('ReadingListUsage', rlu_formatted, conn):
                    raise Exception("Failed to load ReadingListUsage data")
                
                # Fetch ReadingListItemUsage
                rliu_url = self.build_filtered_url(
                    API_CONFIG['READING_LIST_ITEM_USAGE_URL'], 
                    start_date, 
                    end_date
                )
                
                rliu_data = self.fetch_all_pages_with_retry(rliu_url)
                rliu_formatted = [(
                    item.get("id"), item["attributes"].get("item-id"), item["attributes"].get("list-usage-id"),
                    item["attributes"].get("integration-user-id"), item["attributes"].get("utilisation-count"),
                    item["attributes"].get("created-at"), item["attributes"].get("updated-at")
                ) for item in rliu_data]
                
                if not self.bulk_load_to_staging('ReadingListItemUsage', rliu_formatted, conn):
                    raise Exception("Failed to load ReadingListItemUsage data")
                
                # Fetch ReadingUtilisation
                ru_url = self.build_filtered_url(
                    API_CONFIG['READING_UTILISATION_URL'], 
                    start_date, 
                    end_date
                )
                
                ru_data = self.fetch_all_pages_with_retry(ru_url)
                ru_formatted = [(
                    item.get("id"), item["attributes"].get("integration-user-id"), item["attributes"].get("item-id"),
                    item["attributes"].get("item-usage-id"), item["attributes"].get("created-at"), item["attributes"].get("updated-at")
                ) for item in ru_data]
                
                if not self.bulk_load_to_staging('ReadingUtilisation', ru_formatted, conn):
                    raise Exception("Failed to load ReadingUtilisation data")
                
                self.metrics['records_processed'] = len(rlu_formatted) + len(rliu_formatted) + len(ru_formatted)
                
                # Validate dependencies exist in database
                logging.info("Validating parent records exist in database...")
                missing_deps = self.validate_dependencies_from_db(conn)
                
                # Store validation for monitoring
                if missing_deps:
                    total_missing = sum(missing_deps.values())
                    self.metrics['total_missing_dependencies'] = total_missing
                    self.metrics['missing_dependency_breakdown'] = missing_deps
                
                # Transfer to main tables
                if not self.finalize_staging_to_main(conn):
                    raise Exception("Failed to transfer staging data to main tables")
            
            self.metrics['end_time'] = datetime.now()
            
            logging.info(f"Usage data processing completed successfully:")
            logging.info(f"  - Date range: {self.metrics['date_range']['start']} to {self.metrics['date_range']['end']}")
            logging.info(f"  - Records processed: {self.metrics['records_processed']}")
            logging.info(f"  - Records inserted: {self.metrics['records_inserted']}")
            logging.info(f"  - Records updated: {self.metrics['records_updated']}")
            logging.info(f"  - Records skipped: {self.metrics['records_skipped']}")
            if self.metrics['duplicates_skipped'] > 0:
                logging.info(f"  - Duplicates (already exist): {self.metrics['duplicates_skipped']}")
            if self.metrics['orphaned_records'] > 0:
                logging.info(f"  - Orphaned (missing parents): {self.metrics['orphaned_records']}")
            logging.info(f"  - Duration: {self.metrics['end_time'] - self.metrics['start_time']}")
            
            return self.metrics
            
        except Exception as e:
            self.metrics['end_time'] = datetime.now()
            self.metrics['errors'].append(f"Main process error: {e}")
            logging.error(f"Usage data processing failed: {e}")
            raise

def process_usage_data():
    processor = UsageStagingProcessor()
    return processor.process_with_staging()
