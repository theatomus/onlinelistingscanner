-- =====================================================
-- eBay Listing Processing System - SQLite Schema
-- Optimized for high-performance compressed storage
-- =====================================================

-- Enable modern SQLite features
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = -256000;  -- 256MB cache
PRAGMA page_size = 65536;     -- Optimal for compression
PRAGMA foreign_keys = ON;

-- =====================================================
-- CORE LISTINGS TABLE
-- =====================================================
CREATE TABLE listings (
    item_number TEXT PRIMARY KEY NOT NULL,
    created_date INTEGER NOT NULL DEFAULT (unixepoch()),
    last_updated INTEGER NOT NULL DEFAULT (unixepoch()),
    
    -- Core data (JSON text - will be compressed via application layer)
    title_data TEXT,        -- JSON title components
    metadata_data TEXT,     -- JSON metadata fields  
    category TEXT NOT NULL DEFAULT '',
    specifics_data TEXT,    -- JSON eBay specifics
    description_data TEXT,  -- JSON description content
    
    -- Processing status tracking
    processing_status INTEGER DEFAULT 0,  -- 0=new, 1=processed, 2=error, 3=archived
    processing_errors TEXT DEFAULT '',
    
    -- Quick access fields (extracted for indexing)
    brand TEXT,
    device_type TEXT,
    title_text TEXT
);

-- =====================================================
-- TABLE SPECIFICATIONS (Normalized for fast queries)
-- =====================================================
CREATE TABLE table_specifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_number TEXT NOT NULL,
    spec_key TEXT NOT NULL,
    spec_value TEXT NOT NULL,
    spec_order INTEGER DEFAULT 0,
    created_date INTEGER DEFAULT (unixepoch()),
    
    FOREIGN KEY (item_number) REFERENCES listings(item_number) ON DELETE CASCADE,
    UNIQUE(item_number, spec_key)
);

-- =====================================================
-- COMPARISON RESULTS & VALIDATION
-- =====================================================
CREATE TABLE comparison_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_number TEXT NOT NULL,
    comparison_type TEXT NOT NULL,
    comparison_status TEXT NOT NULL,  -- 'pass', 'warning', 'error'
    issues_data BLOB,                 -- Compressed JSON of issues
    validation_score REAL DEFAULT 0.0,
    created_date INTEGER DEFAULT (unixepoch()),
    
    FOREIGN KEY (item_number) REFERENCES listings(item_number) ON DELETE CASCADE
);

-- =====================================================
-- PROCESSING LOGS (Centralized logging)
-- =====================================================
CREATE TABLE processing_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_number TEXT,
    log_level TEXT NOT NULL,  -- DEBUG, INFO, WARNING, ERROR
    component TEXT NOT NULL,  -- process_description, runit, comparison, etc.
    message TEXT NOT NULL,
    details BLOB,             -- Compressed additional details
    session_id TEXT,
    created_date INTEGER DEFAULT (unixepoch()),
    
    FOREIGN KEY (item_number) REFERENCES listings(item_number) ON DELETE SET NULL
);

-- =====================================================
-- PERFORMANCE INDEXES
-- =====================================================

-- Primary query indexes
CREATE INDEX idx_listings_status ON listings(processing_status);
CREATE INDEX idx_listings_brand ON listings(brand) WHERE brand IS NOT NULL;
CREATE INDEX idx_listings_device_type ON listings(device_type) WHERE device_type IS NOT NULL;
CREATE INDEX idx_listings_category ON listings(category);
CREATE INDEX idx_listings_updated ON listings(last_updated);

-- Table specifications indexes
CREATE INDEX idx_specs_item ON table_specifications(item_number);
CREATE INDEX idx_specs_key_value ON table_specifications(spec_key, spec_value);
CREATE INDEX idx_specs_key ON table_specifications(spec_key);

-- Comparison results indexes  
CREATE INDEX idx_comparison_item_type ON comparison_results(item_number, comparison_type);
CREATE INDEX idx_comparison_status ON comparison_results(comparison_status);
CREATE INDEX idx_comparison_score ON comparison_results(validation_score);

-- Logging indexes
CREATE INDEX idx_logs_item ON processing_logs(item_number) WHERE item_number IS NOT NULL;
CREATE INDEX idx_logs_level ON processing_logs(log_level);
CREATE INDEX idx_logs_component ON processing_logs(component);
CREATE INDEX idx_logs_date ON processing_logs(created_date);

-- =====================================================
-- FULL-TEXT SEARCH
-- =====================================================
CREATE VIRTUAL TABLE listings_fts USING fts5(
    item_number UNINDEXED,
    title_text,
    description_text,
    brand,
    device_type,
    content=listings,
    content_rowid=rowid
);

-- FTS triggers for automatic updates
CREATE TRIGGER listings_fts_insert AFTER INSERT ON listings BEGIN
    INSERT INTO listings_fts(rowid, item_number, title_text, description_text, brand, device_type)
    VALUES (NEW.rowid, NEW.item_number, NEW.title_text, 
            COALESCE(json_extract(NEW.description_data, '$.description_text'), ''),
            NEW.brand, NEW.device_type);
END;

CREATE TRIGGER listings_fts_update AFTER UPDATE ON listings BEGIN
    UPDATE listings_fts SET
        title_text = NEW.title_text,
        description_text = COALESCE(json_extract(NEW.description_data, '$.description_text'), ''),
        brand = NEW.brand,
        device_type = NEW.device_type
    WHERE rowid = NEW.rowid;
END;

CREATE TRIGGER listings_fts_delete AFTER DELETE ON listings BEGIN
    DELETE FROM listings_fts WHERE rowid = OLD.rowid;
END;

-- =====================================================
-- VIEWS FOR EASY ACCESS
-- =====================================================

-- Full data view with JSON parsing
CREATE VIEW listings_full AS
SELECT 
    l.item_number,
    l.created_date,
    l.last_updated,
    l.category,
    l.processing_status,
    l.brand,
    l.device_type,
    l.title_text,
    
    -- JSON data (already stored as text)
    CASE WHEN l.title_data IS NOT NULL 
         THEN json(l.title_data) 
         ELSE json('{}') END as title_json,
    
    CASE WHEN l.metadata_data IS NOT NULL 
         THEN json(l.metadata_data) 
         ELSE json('{}') END as metadata_json,
         
    CASE WHEN l.specifics_data IS NOT NULL 
         THEN json(l.specifics_data) 
         ELSE json('{}') END as specifics_json,
         
    CASE WHEN l.description_data IS NOT NULL 
         THEN json(l.description_data) 
         ELSE json('{}') END as description_json
         
FROM listings l;

-- Quick stats view
CREATE VIEW listing_stats AS
SELECT 
    processing_status,
    COUNT(*) as count,
    COUNT(DISTINCT brand) as unique_brands,
    COUNT(DISTINCT device_type) as unique_device_types,
    COUNT(DISTINCT category) as unique_categories,
    MIN(created_date) as earliest_date,
    MAX(last_updated) as latest_update
FROM listings 
GROUP BY processing_status;

-- =====================================================
-- UTILITY FUNCTIONS (Custom SQLite Functions)
-- =====================================================

-- Note: These will be implemented in Python database layer
-- get_listing_data(item_number) -> Returns full decompressed listing
-- compress_json(json_text) -> Returns ZSTD compressed blob
-- search_listings(query) -> Full-text search with ranking