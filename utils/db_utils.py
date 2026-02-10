"""
Database Utilities Module

Reusable functions for database operations across the data platform.
"""

# Standard Librady
import logging
from typing import Dict, List, Optional, Any
from contextlib import contextmanager

# Third Party
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Project

class DatabaseManager:
    """
    Manages database connections and common operations.
    Provides a consistent interface for database interactions.
    """
    
    def __init__(self, conn_id: str = 'postgres_default'):
        """
        Initialize database manager.
        
        Args:
            conn_id: Airflow connection ID
        """
        self._engine = None
        self.conn_id = conn_id
        self.hook = PostgresHook(postgres_conn_id=conn_id)

    @property
    def engine(self):
        """Lazy-load SQLAlchemy engine."""
        if self._engine is None:
            self._engine = self.hook.get_sqlalchemy_engine()
        return self._engine
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections.
        Ensures proper connection cleanup.
        """
        conn = self.hook.get_conn()
        try:
            yield conn
        finally:
            conn.close()
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[tuple]:
        """
        Execute a SELECT query and return results.
        
        Args:
            query: SQL query to execute
            params: Query parameters (optional)
            
        Returns:
            List of result tuples
        """
        with self.engine.connect() as conn:
            result = conn.execute(query, params or ())
            return result.fetchall()
    
    def execute_ddl(self, query: str) -> None:
        """
        Execute a DDL statement (CREATE, DROP, ALTER, etc.).
        
        Args:
            query: DDL statement to execute
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                conn.commit()
                logging.info(f"DDL executed successfully: {query[:100]}...")
            except Exception as e:
                conn.rollback()
                logging.error(f"DDL execution failed: {str(e)}")
                raise
            finally:
                cursor.close()
    
    def table_exists(self, schema: str, table: str) -> bool:
        """
        Check if a table exists.
        
        Args:
            schema: Schema name
            table: Table name
            
        Returns:
            True if table exists, False otherwise
        """
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            );
        """
        result = self.execute_query(query, (schema, table))
        return result[0][0] if result else False
    
    def get_row_count(self, schema: str, table: str) -> int:
        """
        Get row count for a table.
        
        Args:
            schema: Schema name
            table: Table name
            
        Returns:
            Number of rows
        """
        query = f"SELECT COUNT(*) FROM {schema}.{table};"
        result = self.execute_query(query)
        return result[0][0] if result else 0
    
    def get_table_columns(self, schema: str, table: str) -> List[str]:
        """
        Get column names for a table.
        
        Args:
            schema: Schema name
            table: Table name
            
        Returns:
            List of column names
        """
        query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s 
            AND table_name = %s
            ORDER BY ordinal_position;
        """
        results = self.execute_query(query, (schema, table))
        return [row[0] for row in results]
    
    def truncate_table(self, schema: str, table: str) -> None:
        """
        Truncate a table.
        
        Args:
            schema: Schema name
            table: Table name
        """
        query = f"TRUNCATE TABLE {schema}.{table};"
        self.execute_ddl(query)
        logging.info(f"Truncated table: {schema}.{table}")
    
    def bulk_insert_dataframe(
        self, 
        df: pd.DataFrame, 
        schema: str, 
        table: str,
        if_exists: str = 'append',
        chunk_size: int = 1000
    ) -> int:
        """
        Bulk insert DataFrame to database.
        
        Args:
            df: DataFrame to insert
            schema: Target schema
            table: Target table
            if_exists: What to do if table exists ('fail', 'replace', 'append')
            chunk_size: Number of rows per chunk
            
        Returns:
            Number of rows inserted
        """
        # Use SQLAlchemy engine for pandas to_sql (avoids SQLite error)
        rows_inserted = df.to_sql(
            name=table,
            schema=schema,
            con=self.engine,  # ← Use SQLAlchemy engine, not raw connection!
            if_exists=if_exists,
            index=False,
            method='multi',
            chunksize=chunk_size
        )
        
        logging.info(
            f"Inserted {len(df)} rows to {schema}.{table} "
            f"in chunks of {chunk_size}"
        )
        
        return len(df) 


class DataQualityChecker:
    """
    Performs data quality checks on database tables.
    Implements common validation patterns.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize data quality checker.
        
        Args:
            db_manager: DatabaseManager instance
        """
        self.db = db_manager
    
    def check_not_null(self, schema: str, table: str, column: str) -> Dict[str, Any]:
        """
        Check for null values in a column.
        
        Args:
            schema: Schema name
            table: Table name
            column: Column name
            
        Returns:
            Dictionary with check results
        """
        query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT({column}) as non_null_rows,
                COUNT(*) - COUNT({column}) as null_rows,
                ROUND(100.0 * (COUNT(*) - COUNT({column})) / COUNT(*), 2) as null_percentage
            FROM {schema}.{table};
        """
        
        result = self.db.execute_query(query)[0]
        
        check_result = {
            'check_type': 'not_null',
            'schema': schema,
            'table': table,
            'column': column,
            'total_rows': result[0],
            'non_null_rows': result[1],
            'null_rows': result[2],
            'null_percentage': float(result[3]),
            'passed': result[2] == 0
        }
        
        logging.info(
            f"NULL Check - {schema}.{table}.{column}: "
            f"{check_result['null_rows']} null values "
            f"({check_result['null_percentage']}%)"
        )
        
        return check_result
    
    def check_unique(self, schema: str, table: str, column: str) -> Dict[str, Any]:
        """
        Check for duplicate values in a column.
        
        Args:
            schema: Schema name
            table: Table name
            column: Column name
            
        Returns:
            Dictionary with check results
        """
        query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT {column}) as unique_values,
                COUNT(*) - COUNT(DISTINCT {column}) as duplicate_rows
            FROM {schema}.{table};
        """
        
        result = self.db.execute_query(query)[0]
        
        check_result = {
            'check_type': 'unique',
            'schema': schema,
            'table': table,
            'column': column,
            'total_rows': result[0],
            'unique_values': result[1],
            'duplicate_rows': result[2],
            'passed': result[2] == 0
        }
        
        logging.info(
            f"UNIQUE Check - {schema}.{table}.{column}: "
            f"{check_result['unique_values']} unique values, "
            f"{check_result['duplicate_rows']} duplicates"
        )
        
        return check_result
    
    def check_range(
        self, 
        schema: str, 
        table: str, 
        column: str, 
        min_value: Optional[float] = None,
        max_value: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Check if values are within an accepted range.
        
        Args:
            schema: Schema name
            table: Table name
            column: Column name
            min_value: Minimum acceptable value (optional)
            max_value: Maximum acceptable value (optional)
            
        Returns:
            Dictionary with check results
        """
        conditions = []
        if min_value is not None:
            conditions.append(f"{column} < {min_value}")
        if max_value is not None:
            conditions.append(f"{column} > {max_value}")
        
        where_clause = " OR ".join(conditions) if conditions else "FALSE"
        
        query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN {where_clause} THEN 1 END) as out_of_range_rows
            FROM {schema}.{table};
        """
        
        result = self.db.execute_query(query)[0]
        
        check_result = {
            'check_type': 'range',
            'schema': schema,
            'table': table,
            'column': column,
            'min_value': min_value,
            'max_value': max_value,
            'total_rows': result[0],
            'out_of_range_rows': result[1],
            'passed': result[1] == 0
        }
        
        logging.info(
            f"RANGE Check - {schema}.{table}.{column}: "
            f"{check_result['out_of_range_rows']} out of range"
        )
        
        return check_result
    
    def check_referential_integrity(
        self,
        child_schema: str,
        child_table: str,
        child_column: str,
        parent_schema: str,
        parent_table: str,
        parent_column: str
    ) -> Dict[str, Any]:
        """
        Check referential integrity between two tables.
        
        Args:
            child_schema: Child table schema
            child_table: Child table name
            child_column: Foreign key column
            parent_schema: Parent table schema
            parent_table: Parent table name
            parent_column: Primary key column
            
        Returns:
            Dictionary with check results
        """
        query = f"""
            SELECT COUNT(*) as orphaned_rows
            FROM {child_schema}.{child_table} c
            WHERE c.{child_column} IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 
                FROM {parent_schema}.{parent_table} p
                WHERE p.{parent_column} = c.{child_column}
            );
        """
        
        result = self.db.execute_query(query)[0]
        
        check_result = {
            'check_type': 'referential_integrity',
            'child_table': f"{child_schema}.{child_table}",
            'child_column': child_column,
            'parent_table': f"{parent_schema}.{parent_table}",
            'parent_column': parent_column,
            'orphaned_rows': result[0],
            'passed': result[0] == 0
        }
        
        logging.info(
            f"REFERENTIAL INTEGRITY Check - "
            f"{child_schema}.{child_table}.{child_column} -> "
            f"{parent_schema}.{parent_table}.{parent_column}: "
            f"{check_result['orphaned_rows']} orphaned rows"
        )
        
        return check_result
    
    def run_quality_suite(
        self, 
        schema: str, 
        table: str, 
        checks_config: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Run a suite of data quality checks.
        
        Args:
            schema: Schema name
            table: Table name
            checks_config: List of check configurations
            
        Returns:
            Dictionary with all check results
        """
        results = {
            'schema': schema,
            'table': table,
            'checks': [],
            'total_checks': 0,
            'passed_checks': 0,
            'failed_checks': 0
        }
        
        for check_config in checks_config:
            check_type = check_config.get('type')
            
            if check_type == 'not_null':
                result = self.check_not_null(
                    schema, table, check_config['column']
                )
            elif check_type == 'unique':
                result = self.check_unique(
                    schema, table, check_config['column']
                )
            elif check_type == 'range':
                result = self.check_range(
                    schema, table, check_config['column'],
                    check_config.get('min_value'),
                    check_config.get('max_value')
                )
            else:
                logging.warning(f"Unknown check type: {check_type}")
                continue
            
            results['checks'].append(result)
            results['total_checks'] += 1
            
            if result['passed']:
                results['passed_checks'] += 1
            else:
                results['failed_checks'] += 1
        
        logging.info(
            f"Quality Suite Complete - {schema}.{table}: "
            f"{results['passed_checks']}/{results['total_checks']} checks passed"
        )
        
        return results


# --- CONVENIENCE FUNCTIONS ---

def get_db_manager(conn_id: str = 'postgres_default') -> DatabaseManager:
    """Get DatabaseManager instance."""
    return DatabaseManager(conn_id)


def get_quality_checker(conn_id: str = 'postgres_default') -> DataQualityChecker:
    """Get DataQualityChecker instance."""
    db_manager = DatabaseManager(conn_id)
    return DataQualityChecker(db_manager)
