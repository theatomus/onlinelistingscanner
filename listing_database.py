"""
eBay Listing Processing System - SQLite Database Layer
High-performance compressed storage with thread-safe operations
"""

import sqlite3
import json
import threading
import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any, Tuple, Union
from pathlib import Path
import queue
import concurrent.futures
from datetime import datetime


# =====================================================
# DATABASE CONFIGURATION
# =====================================================

class DatabaseConfig:
    """Database configuration and optimization settings"""
    
    # Database file settings
    DB_PATH = "listings.db"
    BACKUP_PATH = "backups"
    
    # Performance settings
    WAL_MODE = True
    PAGE_SIZE = 65536
    CACHE_SIZE_MB = 256
    CONNECTION_POOL_SIZE = 10
    QUERY_TIMEOUT = 30
    
    # Data storage settings
    JSON_SEPARATORS = (',', ':')  # Compact JSON format
    ENABLE_COMPRESSION = False    # Can be enabled later with ZSTD
    
    # Logging
    LOG_LEVEL = logging.INFO
    LOG_RETENTION_DAYS = 30


# =====================================================
# DATA SERIALIZATION UTILITIES
# =====================================================

class DataManager:
    """Handles JSON serialization and future compression capabilities"""
    
    def __init__(self):
        self._lock = threading.Lock()
        self.logger = logging.getLogger(__name__)
        
    def serialize_json(self, data: Dict, data_type: str = "default") -> str:
        """Serialize dictionary to compact JSON string"""
        if not data:
            return ''
            
        try:
            return json.dumps(data, separators=DatabaseConfig.JSON_SEPARATORS, ensure_ascii=False)
        except (TypeError, ValueError) as e:
            self.logger.error(f"Error serializing {data_type} data: {e}")
            return '{}'
    
    def deserialize_json(self, json_str: str, data_type: str = "default") -> Dict:
        """Deserialize JSON string to dictionary"""
        if not json_str:
            return {}
            
        try:
            return json.loads(json_str)
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error(f"Error deserializing {data_type} data: {e}")
            return {}
            
    def compress(self, data: str, dict_id: str = "default") -> str:
        """Compress data - currently just returns the data (placeholder for future ZSTD)"""
        return data
    
    def decompress(self, data: str, dict_id: str = "default") -> str:
        """Decompress data - currently just returns the data (placeholder for future ZSTD)"""
        return data


# =====================================================
# CONNECTION POOL
# =====================================================

class ConnectionPool:
    """Thread-safe SQLite connection pool"""
    
    def __init__(self, db_path: str, pool_size: int = DatabaseConfig.CONNECTION_POOL_SIZE):
        self.db_path = db_path
        self.pool_size = pool_size
        self.pool = queue.Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self.logger = logging.getLogger(__name__)
        
        # Initialize connections
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize the connection pool"""
        for _ in range(self.pool_size):
            conn = self._create_connection()
            self.pool.put(conn)
    
    def _create_connection(self) -> sqlite3.Connection:
        """Create a new optimized SQLite connection"""
        conn = sqlite3.connect(
            self.db_path,
            timeout=DatabaseConfig.QUERY_TIMEOUT,
            check_same_thread=False,
            isolation_level=None  # Autocommit mode
        )
        
        # Optimize connection
        conn.execute(f"PRAGMA page_size = {DatabaseConfig.PAGE_SIZE}")
        conn.execute(f"PRAGMA cache_size = -{DatabaseConfig.CACHE_SIZE_MB * 1024}")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA temp_store = MEMORY")
        
        if DatabaseConfig.WAL_MODE:
            conn.execute("PRAGMA journal_mode = WAL")
        
        # Row factory for dict access
        conn.row_factory = sqlite3.Row
        
        return conn
    
    @contextmanager
    def get_connection(self):
        """Get a connection from the pool"""
        conn = None
        try:
            conn = self.pool.get(timeout=5)
            yield conn
        except queue.Empty:
            # Pool exhausted, create temporary connection
            self.logger.warning("Connection pool exhausted, creating temporary connection")
            conn = self._create_connection()
            yield conn
        finally:
            if conn:
                try:
                    # Return to pool or close if pool is full
                    self.pool.put_nowait(conn)
                except queue.Full:
                    conn.close()
    
    def close_all(self):
        """Close all connections in the pool"""
        while not self.pool.empty():
            try:
                conn = self.pool.get_nowait()
                conn.close()
            except queue.Empty:
                break


# =====================================================
# MAIN DATABASE CLASS
# =====================================================

class ListingDatabase:
    """High-performance SQLite database for eBay listing processing"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or DatabaseConfig.DB_PATH
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.data_manager = DataManager()
        self.pool = ConnectionPool(self.db_path)
        
        # Ensure database exists and is initialized
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize database with schema if it doesn't exist"""
        schema_path = Path("database_schema.sql")
        
        with self.pool.get_connection() as conn:
            # Check if database is already initialized
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='listings'"
            )
            
            if not cursor.fetchone():
                self.logger.info("Initializing database schema...")
                
                if schema_path.exists():
                    with open(schema_path, 'r') as f:
                        schema_sql = f.read()
                    conn.executescript(schema_sql)
                else:
                    raise FileNotFoundError("database_schema.sql not found")
                
                # Create backup directory
                Path(DatabaseConfig.BACKUP_PATH).mkdir(exist_ok=True)
                
                self.logger.info("Database schema initialized successfully")
    

    
    # =====================================================
    # CORE CRUD OPERATIONS
    # =====================================================
    
    def insert_listing(self, item_number: str, listing_data: Dict) -> bool:
        """Insert a new listing with JSON data"""
        try:
            with self.pool.get_connection() as conn:
                # Serialize JSON data
                title_json = self.data_manager.serialize_json(
                    listing_data.get('title', {}), 'title'
                ) if listing_data.get('title') else ''
                
                metadata_json = self.data_manager.serialize_json(
                    listing_data.get('metadata', {}), 'metadata'
                ) if listing_data.get('metadata') else ''
                
                specifics_json = self.data_manager.serialize_json(
                    listing_data.get('specifics', {}), 'specifics'
                ) if listing_data.get('specifics') else ''
                
                description_json = self.data_manager.serialize_json(
                    listing_data.get('description', {}), 'description'
                ) if listing_data.get('description') else ''
                
                # Extract quick access fields
                title_dict = listing_data.get('title', {})
                brand = title_dict.get('brand', '')
                device_type = title_dict.get('device_type', '')
                title_text = title_dict.get('title_title_key', '')
                
                # Insert main record
                conn.execute("""
                    INSERT OR REPLACE INTO listings 
                    (item_number, category, title_data, metadata_data, specifics_data, description_data,
                     brand, device_type, title_text)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    item_number,
                    listing_data.get('category', ''),
                    title_json,
                    metadata_json,
                    specifics_json,
                    description_json,
                    brand,
                    device_type,
                    title_text
                ))
                
                # Insert table specifications
                self._insert_table_specifications(conn, item_number, listing_data.get('table_data', []))
                
                # Log the operation
                self._log_operation(conn, item_number, 'INSERT', 'Successfully inserted listing')
                
                return True
                
        except Exception as e:
            self.logger.error(f"Error inserting listing {item_number}: {e}", exc_info=True)
            return False
    
    def get_listing(self, item_number: str, decompress: bool = True) -> Optional[Dict]:
        """Retrieve a listing with optional JSON parsing"""
        try:
            with self.pool.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT * FROM listings WHERE item_number = ?
                """, (item_number,))
                
                row = cursor.fetchone()
                if not row:
                    return None
                
                result = dict(row)
                
                if decompress:
                    # Parse JSON fields using data manager
                    result['title_json'] = self.data_manager.deserialize_json(
                        result.get('title_data', ''), 'title'
                    )
                    result['metadata_json'] = self.data_manager.deserialize_json(
                        result.get('metadata_data', ''), 'metadata'
                    )
                    result['specifics_json'] = self.data_manager.deserialize_json(
                        result.get('specifics_data', ''), 'specifics'
                    )
                    result['description_json'] = self.data_manager.deserialize_json(
                        result.get('description_data', ''), 'description'
                    )
                
                # Get table specifications
                result['table_data'] = self._get_table_specifications(conn, item_number)
                
                return result
                
        except Exception as e:
            self.logger.error(f"Error retrieving listing {item_number}: {e}", exc_info=True)
            return None
    
    def _insert_table_specifications(self, conn: sqlite3.Connection, item_number: str, table_data: List):
        """Insert table specifications for a listing"""
        # Clear existing specifications
        conn.execute("DELETE FROM table_specifications WHERE item_number = ?", (item_number,))
        
        # Insert new specifications
        for i, entry in enumerate(table_data):
            if isinstance(entry, dict):
                for key, value in entry.items():
                    if value and str(value).strip():
                        conn.execute("""
                            INSERT INTO table_specifications 
                            (item_number, spec_key, spec_value, spec_order)
                            VALUES (?, ?, ?, ?)
                        """, (item_number, key, str(value), i))
    
    def _get_table_specifications(self, conn: sqlite3.Connection, item_number: str) -> List[Dict]:
        """Retrieve table specifications for a listing"""
        cursor = conn.execute("""
            SELECT spec_key, spec_value, spec_order
            FROM table_specifications 
            WHERE item_number = ? 
            ORDER BY spec_order, spec_key
        """, (item_number,))
        
        # Group by spec_order to reconstruct original table_data structure
        specs_by_order = {}
        for row in cursor:
            order = row['spec_order']
            if order not in specs_by_order:
                specs_by_order[order] = {}
            specs_by_order[order][row['spec_key']] = row['spec_value']
        
        return [specs_by_order[order] for order in sorted(specs_by_order.keys())]
    
    def _log_operation(self, conn: sqlite3.Connection, item_number: str, operation: str, message: str, details: Dict = None):
        """Log database operations"""
        details_json = self.data_manager.serialize_json(details or {}, 'logs') if details else ''
        
        conn.execute("""
            INSERT INTO processing_logs
            (item_number, log_level, component, message, details)
            VALUES (?, ?, ?, ?, ?)
        """, (item_number, 'INFO', 'database', f"{operation}: {message}", details_json))
    
    # =====================================================
    # QUERY OPERATIONS
    # =====================================================
    
    def search_listings(self, query: str, limit: int = 100) -> List[Dict]:
        """Full-text search across listings"""
        try:
            with self.pool.get_connection() as conn:
                if not query or query.strip() == "":
                    # If empty query, return all listings
                    cursor = conn.execute("""
                        SELECT l.item_number, l.brand, l.device_type, l.title_text, l.category,
                               l.created_date, l.processing_status
                        FROM listings l
                        ORDER BY l.last_updated DESC
                        LIMIT ?
                    """, (limit,))
                else:
                    # Use FTS5 search for non-empty queries
                    cursor = conn.execute("""
                        SELECT l.item_number, l.brand, l.device_type, l.title_text, l.category,
                               fts.rank
                        FROM listings_fts fts
                        JOIN listings l ON l.rowid = fts.rowid
                        WHERE listings_fts MATCH ?
                        ORDER BY fts.rank
                        LIMIT ?
                    """, (query, limit))
                
                return [dict(row) for row in cursor]
                
        except Exception as e:
            self.logger.error(f"Error searching listings: {e}", exc_info=True)
            return []
    
    def get_listings_by_status(self, status: int, limit: int = None) -> List[Dict]:
        """Get listings by processing status"""
        try:
            with self.pool.get_connection() as conn:
                sql = """
                    SELECT item_number, brand, device_type, title_text, category, 
                           processing_status, last_updated
                    FROM listings 
                    WHERE processing_status = ?
                    ORDER BY last_updated DESC
                """
                
                params = [status]
                if limit:
                    sql += " LIMIT ?"
                    params.append(limit)
                
                cursor = conn.execute(sql, params)
                return [dict(row) for row in cursor]
                
        except Exception as e:
            self.logger.error(f"Error retrieving listings by status: {e}", exc_info=True)
            return []
    
    def get_database_stats(self) -> Dict:
        """Get comprehensive database statistics"""
        try:
            with self.pool.get_connection() as conn:
                stats = {}
                
                # Basic counts
                cursor = conn.execute("SELECT * FROM listing_stats")
                stats['status_breakdown'] = [dict(row) for row in cursor]
                
                # Database size info
                cursor = conn.execute("PRAGMA page_count")
                page_count = cursor.fetchone()[0]
                
                cursor = conn.execute("PRAGMA page_size")
                page_size = cursor.fetchone()[0]
                
                stats['database_size_bytes'] = page_count * page_size
                stats['database_size_mb'] = stats['database_size_bytes'] / (1024 * 1024)
                
                # Data storage statistics
                cursor = conn.execute("""
                    SELECT 
                        COUNT(*) as total_records,
                        SUM(LENGTH(title_data)) as title_data_bytes,
                        SUM(LENGTH(metadata_data)) as metadata_data_bytes,
                        SUM(LENGTH(specifics_data)) as specifics_data_bytes,
                        SUM(LENGTH(description_data)) as description_data_bytes,
                        AVG(LENGTH(title_data)) as avg_title_size,
                        AVG(LENGTH(metadata_data)) as avg_metadata_size
                    FROM listings
                    WHERE title_data IS NOT NULL AND title_data != ''
                """)
                
                storage_stats = dict(cursor.fetchone())
                stats['storage'] = storage_stats
                
                return stats
                
        except Exception as e:
            self.logger.error(f"Error retrieving database stats: {e}", exc_info=True)
            return {}
    
    # =====================================================
    # MAINTENANCE OPERATIONS
    # =====================================================
    
    def vacuum_and_optimize(self):
        """Perform database maintenance"""
        try:
            with self.pool.get_connection() as conn:
                self.logger.info("Starting database optimization...")
                
                # Update statistics
                conn.execute("ANALYZE")
                
                # Rebuild FTS index
                conn.execute("INSERT INTO listings_fts(listings_fts) VALUES('rebuild')")
                
                # Vacuum database
                conn.execute("VACUUM")
                
                self.logger.info("Database optimization completed")
                
        except Exception as e:
            self.logger.error(f"Error during database optimization: {e}", exc_info=True)
    
    def backup_database(self, backup_path: str = None) -> bool:
        """Create a backup of the database"""
        try:
            backup_path = backup_path or f"{DatabaseConfig.BACKUP_PATH}/listings_backup_{int(time.time())}.db"
            
            with self.pool.get_connection() as conn:
                backup = sqlite3.connect(backup_path)
                conn.backup(backup)
                backup.close()
                
                self.logger.info(f"Database backed up to {backup_path}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error creating backup: {e}", exc_info=True)
            return False
    
    def close(self):
        """Close database and cleanup resources"""
        if hasattr(self, 'pool'):
            self.pool.close_all()


# =====================================================
# SINGLETON INSTANCE
# =====================================================

# Global database instance
_db_instance = None
_db_lock = threading.Lock()

def get_database() -> ListingDatabase:
    """Get singleton database instance"""
    global _db_instance
    
    if _db_instance is None:
        with _db_lock:
            if _db_instance is None:
                _db_instance = ListingDatabase()
    
    return _db_instance


# =====================================================
# CONVENIENCE FUNCTIONS
# =====================================================

def insert_listing(item_number: str, listing_data: Dict) -> bool:
    """Convenience function to insert a listing"""
    return get_database().insert_listing(item_number, listing_data)

def get_listing(item_number: str, decompress: bool = True) -> Optional[Dict]:
    """Convenience function to get a listing"""
    return get_database().get_listing(item_number, decompress)

def search_listings(query: str, limit: int = 100) -> List[Dict]:
    """Convenience function to search listings"""
    return get_database().search_listings(query, limit)


if __name__ == "__main__":
    # Test the database functionality
    import sys
    
    logging.basicConfig(level=logging.INFO)
    
    db = ListingDatabase("test_listings.db")
    
    # Test data
    test_listing = {
        "title": {"title_title_key": "Dell Laptop", "brand": "Dell", "device_type": "Laptop"},
        "metadata": {"condition": "Used", "price": "299.99"},
        "category": "PC Laptops & Netbooks",
        "specifics": {"color": "Black", "screen_size": "15.6"},
        "description": {"description_text": "Great condition laptop"},
        "table_data": [
            {"brand": "Dell", "model": "Inspiron", "cpu": "Intel i5"},
            {"ram": "8GB", "storage": "256GB SSD"}
        ]
    }
    
    # Test insert
    success = db.insert_listing("123456789", test_listing)
    print(f"Insert success: {success}")
    
    # Test retrieve
    retrieved = db.get_listing("123456789")
    print(f"Retrieved listing: {retrieved['item_number'] if retrieved else 'None'}")
    
    # Test search
    results = db.search_listings("Dell Laptop")
    print(f"Search results: {len(results)}")
    
    # Test stats
    stats = db.get_database_stats()
    print(f"Database stats: {stats}")
    
    db.close()