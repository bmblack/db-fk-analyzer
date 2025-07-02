"""
Database connection and utility functions for SQL Server.
"""
import os
import logging
from typing import Optional, List, Dict, Any
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database connections and operations for SQL Server."""
    
    def __init__(self, connection_string: str):
        """Initialize database manager with connection string."""
        self.connection_string = connection_string
        self.engine: Optional[Engine] = None
        self._connect()
    
    def _connect(self) -> None:
        """Establish database connection."""
        try:
            self.engine = create_engine(
                self.connection_string,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection established successfully")
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test if database connection is working."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute a SELECT query and return results as DataFrame."""
        try:
            with self.engine.connect() as conn:
                result = pd.read_sql(query, conn, params=params)
            logger.debug(f"Query executed successfully, returned {len(result)} rows")
            return result
        except SQLAlchemyError as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def execute_non_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> int:
        """Execute a non-SELECT query and return affected rows count."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                conn.commit()
                affected_rows = result.rowcount
            logger.debug(f"Non-query executed successfully, affected {affected_rows} rows")
            return affected_rows
        except SQLAlchemyError as e:
            logger.error(f"Non-query execution failed: {e}")
            raise
    
    def get_table_list(self) -> List[str]:
        """Get list of all tables in the database."""
        query = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        try:
            result = self.execute_query(query)
            return result['TABLE_NAME'].tolist()
        except Exception as e:
            logger.error(f"Failed to get table list: {e}")
            return []
    
    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """Get schema information for a specific table."""
        query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = :table_name
        ORDER BY ORDINAL_POSITION
        """
        try:
            return self.execute_query(query, {'table_name': table_name})
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            return pd.DataFrame()
    
    def get_foreign_keys(self) -> pd.DataFrame:
        """Get all foreign key constraints in the database."""
        query = """
        SELECT 
            fk.name AS constraint_name,
            tp.name AS parent_table,
            cp.name AS parent_column,
            tr.name AS referenced_table,
            cr.name AS referenced_column,
            fk.delete_referential_action_desc AS delete_action,
            fk.update_referential_action_desc AS update_action
        FROM sys.foreign_keys fk
        INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        INNER JOIN sys.tables tp ON fkc.parent_object_id = tp.object_id
        INNER JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
        INNER JOIN sys.tables tr ON fkc.referenced_object_id = tr.object_id
        INNER JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
        ORDER BY tp.name, cp.name
        """
        try:
            return self.execute_query(query)
        except Exception as e:
            logger.error(f"Failed to get foreign keys: {e}")
            return pd.DataFrame()
    
    def get_table_relationships(self) -> pd.DataFrame:
        """Get potential table relationships based on column naming patterns."""
        query = """
        WITH PotentialFKs AS (
            SELECT DISTINCT
                t1.TABLE_NAME as source_table,
                c1.COLUMN_NAME as source_column,
                t2.TABLE_NAME as target_table,
                c2.COLUMN_NAME as target_column,
                CASE 
                    WHEN c1.COLUMN_NAME = c2.COLUMN_NAME THEN 'EXACT_MATCH'
                    WHEN c1.COLUMN_NAME LIKE '%' + t2.TABLE_NAME + '%' THEN 'TABLE_NAME_PATTERN'
                    WHEN c1.COLUMN_NAME LIKE '%ID' AND c2.COLUMN_NAME LIKE '%ID' THEN 'ID_PATTERN'
                    ELSE 'OTHER'
                END as match_type
            FROM INFORMATION_SCHEMA.TABLES t1
            CROSS JOIN INFORMATION_SCHEMA.TABLES t2
            INNER JOIN INFORMATION_SCHEMA.COLUMNS c1 ON t1.TABLE_NAME = c1.TABLE_NAME
            INNER JOIN INFORMATION_SCHEMA.COLUMNS c2 ON t2.TABLE_NAME = c2.TABLE_NAME
            WHERE t1.TABLE_TYPE = 'BASE TABLE' 
                AND t2.TABLE_TYPE = 'BASE TABLE'
                AND t1.TABLE_NAME != t2.TABLE_NAME
                AND c1.DATA_TYPE = c2.DATA_TYPE
                AND (
                    c1.COLUMN_NAME = c2.COLUMN_NAME
                    OR c1.COLUMN_NAME LIKE '%' + t2.TABLE_NAME + '%'
                    OR (c1.COLUMN_NAME LIKE '%ID' AND c2.COLUMN_NAME LIKE '%ID')
                )
        )
        SELECT * FROM PotentialFKs
        WHERE match_type IN ('EXACT_MATCH', 'TABLE_NAME_PATTERN')
        ORDER BY source_table, match_type
        """
        try:
            return self.execute_query(query)
        except Exception as e:
            logger.error(f"Failed to get table relationships: {e}")
            return pd.DataFrame()
    
    def get_orphaned_records(self, parent_table: str, parent_column: str, 
                           child_table: str, child_column: str) -> pd.DataFrame:
        """Find orphaned records between two tables."""
        query = f"""
        SELECT COUNT(*) as orphaned_count
        FROM [{child_table}] c
        LEFT JOIN [{parent_table}] p ON c.[{child_column}] = p.[{parent_column}]
        WHERE c.[{child_column}] IS NOT NULL 
            AND p.[{parent_column}] IS NULL
        """
        try:
            return self.execute_query(query)
        except Exception as e:
            logger.error(f"Failed to get orphaned records: {e}")
            return pd.DataFrame()
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get general database statistics."""
        try:
            table_count = len(self.get_table_list())
            fk_count = len(self.get_foreign_keys())
            
            # Get database size
            size_query = """
            SELECT 
                SUM(size * 8.0 / 1024) as size_mb
            FROM sys.master_files
            WHERE database_id = DB_ID()
            """
            size_result = self.execute_query(size_query)
            db_size_mb = size_result['size_mb'].iloc[0] if not size_result.empty else 0
            
            return {
                'table_count': table_count,
                'foreign_key_count': fk_count,
                'database_size_mb': round(db_size_mb, 2),
                'connection_status': 'Connected'
            }
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return {
                'table_count': 0,
                'foreign_key_count': 0,
                'database_size_mb': 0,
                'connection_status': 'Error'
            }
    
    def close(self) -> None:
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")


def get_database_manager() -> DatabaseManager:
    """Factory function to create DatabaseManager instance."""
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv('config/settings.env')
    
    connection_string = os.getenv('DB_CONNECTION_STRING')
    if not connection_string:
        raise ValueError("DB_CONNECTION_STRING not found in environment variables")
    
    return DatabaseManager(connection_string)
