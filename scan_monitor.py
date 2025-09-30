import time
import os
import re
import json
import subprocess
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import threading
import sys
import tkinter as tk
from tkinter import scrolledtext, ttk
import queue
import sqlite3
import gzip
import tarfile
import zipfile
import argparse

# Import standardized SKU utilities if available
try:
    from sku_utils import (extract_sku_number as std_extract_sku_number,
                          extract_sku_prefix as std_extract_sku_prefix,
                          ENABLE_STANDARDIZED_SKU_HANDLING)
    USE_STANDARDIZED_SKU_HANDLING = ENABLE_STANDARDIZED_SKU_HANDLING
except ImportError:
    USE_STANDARDIZED_SKU_HANDLING = False
try:
    from name_utils import format_initial_with_name, annotate_sku_with_name
except ImportError:
    def format_initial_with_name(initials, mapping=None):
        return initials
    def annotate_sku_with_name(sku, mapping=None):
        return sku

# Add these after imports
# Safety: Keep BASE_DIR constant for resource paths, but use os.getcwd() for
# watchdog deletion scope and self-delete script root to ensure we only touch
# files under the active working directory.
BASE_DIR = os.path.dirname(__file__)
EBAY_DATA_DIR = os.path.join(BASE_DIR, 'eBayListingData')
REPORTS_DIR = os.path.join(BASE_DIR, 'reports')
BACKUPS_DIR = os.path.join(BASE_DIR, 'backups')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
MONITORING_LOGS_DIR = os.path.join(LOGS_DIR, 'monitoring')
STATE_DIR = os.path.join(BASE_DIR, 'state')
HIGHEST_SKU_FILE = os.path.join(STATE_DIR, 'highest_sku_number.txt')  # moved to state
# Legacy fallback (pre-state move)
HIGHEST_SKU_FILE_FALLBACK = os.path.join(BASE_DIR, 'highest_sku_number.txt')
SKU_LOG_FILE = os.path.join(MONITORING_LOGS_DIR, 'sku_tracking.log')  # New dedicated log file for SKU debugging
# Track every report that scan monitor sends (manual and scheduled)
REPORTS_SENT_LOG = os.path.join(MONITORING_LOGS_DIR, 'reports_sent.log')
ZSCRAPE_DEFAULT_AHK = os.path.join(BASE_DIR, 'zscrape_process_new_auto_shutdown_at_350pm_new.ahk')

# Watchdog defaults (used when watchdog is OFF)
WATCHDOG_DEFAULT_WORK_START = "08:00"
WATCHDOG_DEFAULT_WORK_END = "15:30"
WATCHDOG_CONTROL_FILE = os.path.join(STATE_DIR, 'watchdog_control.txt')
WATCHDOG_STATUS_FILE = os.path.join(STATE_DIR, 'watchdog_status.txt')
AHK_HEARTBEAT_FILE = os.path.join(STATE_DIR, 'ahk_heartbeat.txt')
AHK_RESTART_SIGNAL_FILE = os.path.join(STATE_DIR, 'ahk_restart_request.txt')

os.makedirs(REPORTS_DIR, exist_ok=True)
os.makedirs(BACKUPS_DIR, exist_ok=True)
os.makedirs(MONITORING_LOGS_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)

class DailyStatsDB:
    def __init__(self, date_str=None):
        """Initialize daily statistics database"""
        if date_str is None:
            date_str = datetime.now().strftime('%Y%m%d')
        
        # Create database directory under backups if it doesn't exist
        try:
            self.db_dir = os.path.join(BACKUPS_DIR, "daily_databases")
        except Exception:
            self.db_dir = os.path.join(BASE_DIR, "backups", "daily_databases")
        try:
            os.makedirs(self.db_dir, exist_ok=True)
        except Exception as e:
            print(f"Warning: Could not create database directory {self.db_dir}: {e}")
            # Fall back to current directory
            self.db_dir = "."
        
        self.db_path = os.path.join(self.db_dir, f"daily_stats_{date_str}.db")
        
        try:
            self.init_database()
        except Exception as e:
            print(f"Warning: Failed to initialize database {self.db_path}: {e}")
    
    def get_connection(self):
        """Get a new database connection (thread-safe)"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn
    
    def init_database(self):
        """Initialize database with optimized schema"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            # Daily summary table - one record per report
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS daily_summary (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER NOT NULL,  -- Unix timestamp (smaller than TEXT)
                    report_type TEXT NOT NULL,   -- 'mini' or 'full'
                    runtime_hours REAL NOT NULL,
                    total_processed INTEGER NOT NULL,  -- NEW listings only (for backward compatibility)
                    total_all_listings INTEGER DEFAULT 0,  -- ALL listings scanned (new + existing)
                    scraping_issues INTEGER NOT NULL,     -- ALL listings (for backward compatibility)
                    data_quality_issues INTEGER NOT NULL, -- ALL listings (for backward compatibility)
                    total_issues INTEGER NOT NULL,        -- ALL listings (for backward compatibility)
                    new_scraping_issues INTEGER DEFAULT 0,     -- NEW listings only
                    new_data_quality_issues INTEGER DEFAULT 0, -- NEW listings only  
                    new_total_issues INTEGER DEFAULT 0,        -- NEW listings only
                    estimated_savings INTEGER NOT NULL,  -- In dollars
                    processing_rate REAL NOT NULL
                )
            ''')
            
            # Prefix statistics - normalized to avoid duplication
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS prefix_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    summary_id INTEGER NOT NULL,
                    prefix TEXT NOT NULL,
                    total_listings INTEGER NOT NULL,  -- NEW listings only (for backward compatibility)
                    all_listings INTEGER DEFAULT 0,   -- ALL listings scanned (new + existing)
                    issues_found INTEGER NOT NULL,    -- ALL listings (for backward compatibility)
                    new_issues_found INTEGER DEFAULT 0, -- NEW listings only
                    FOREIGN KEY (summary_id) REFERENCES daily_summary (id),
                    UNIQUE(summary_id, prefix)
                )
            ''')
            
            # Store categories - compressed JSON for space efficiency
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS store_categories (
                    summary_id INTEGER PRIMARY KEY,
                    categories_json_gz BLOB,  -- Gzip compressed JSON for NEW listings
                    all_categories_json_gz BLOB,  -- Gzip compressed JSON for ALL listings
                    total_categories INTEGER NOT NULL,
                    total_all_categories INTEGER DEFAULT 0,
                    FOREIGN KEY (summary_id) REFERENCES daily_summary (id)
                )
            ''')
            
            # Hourly activity - only store non-zero hours
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS hourly_activity (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    summary_id INTEGER NOT NULL,
                    hour_24 INTEGER NOT NULL,  -- 0-23
                    activity_count INTEGER NOT NULL,
                    FOREIGN KEY (summary_id) REFERENCES daily_summary (id),
                    UNIQUE(summary_id, hour_24)
                )
            ''')
            
            # Peak times analysis
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS peak_times (
                    summary_id INTEGER PRIMARY KEY,
                    peak_hour_start INTEGER,      -- 24-hour format
                    peak_hour_count INTEGER,
                    peak_interval_start INTEGER,  -- Hour
                    peak_interval_minute INTEGER, -- 0 or 30
                    peak_interval_count INTEGER,
                    FOREIGN KEY (summary_id) REFERENCES daily_summary (id)
                )
            ''')
            
            # Create indexes for better query performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_summary_timestamp ON daily_summary(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_prefix_summary ON prefix_stats(summary_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_hourly_summary ON hourly_activity(summary_id)')
            
            conn.commit()
        finally:
            conn.close()
    
    def compress_json(self, data):
        """Compress JSON data to save space"""
        json_str = json.dumps(data, separators=(',', ':'))  # Compact JSON
        return gzip.compress(json_str.encode('utf-8'))
    
    def decompress_json(self, compressed_data):
        """Decompress JSON data"""
        if compressed_data is None:
            return {}
        json_str = gzip.decompress(compressed_data).decode('utf-8')
        return json.loads(json_str)
    
    def save_report_stats(self, monitor_instance, is_full_report=False):
        """Save current statistics to database"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            # Calculate values
            current_time = datetime.now()
            timestamp = int(current_time.timestamp())
            runtime_hours = (current_time - monitor_instance.script_start_time).total_seconds() / 3600
            report_type = 'full' if is_full_report else 'mini'
            
            # Get counts for both new and all listings
            total_from_categories = sum(monitor_instance.store_category_stats.values()) if hasattr(monitor_instance, 'store_category_stats') else 0
            total_count = max(monitor_instance.total_scanned_today, total_from_categories)  # NEW listings
            
            total_all_from_categories = sum(monitor_instance.all_store_category_stats.values()) if hasattr(monitor_instance, 'all_store_category_stats') else 0
            total_all_count = max(monitor_instance.total_all_listings_today, total_all_from_categories)  # ALL listings
            
            # Calculate processing rate using an 8:00 AM baseline
            baseline = _get_baseline_8am(current_time)
            elapsed_seconds = (current_time - baseline).total_seconds()
            if is_full_report:
                hours_since_baseline = elapsed_seconds / 3600 if elapsed_seconds > 0 else 0
                processing_rate = (total_count / hours_since_baseline) if hours_since_baseline > 0 else 0
            else:
                intervals_since_baseline = elapsed_seconds / 1800 if elapsed_seconds > 0 else 0
                processing_rate = (total_count / intervals_since_baseline) if intervals_since_baseline > 0 else 0
            
            # Insert daily summary with both new and all listings data
            cursor.execute('''
                INSERT INTO daily_summary 
                (timestamp, report_type, runtime_hours, total_processed, total_all_listings, 
                 scraping_issues, data_quality_issues, total_issues,
                 new_scraping_issues, new_data_quality_issues, new_total_issues,
                 estimated_savings, processing_rate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                timestamp, report_type, runtime_hours, total_count, total_all_count,
                monitor_instance.total_scraping_issues, monitor_instance.total_detailed_issues, monitor_instance.total_issues_found,
                monitor_instance.new_total_scraping_issues, monitor_instance.new_total_detailed_issues, monitor_instance.new_total_issues_found,
                monitor_instance.total_issues_found * 10, processing_rate
            ))
            
            summary_id = cursor.lastrowid
            
            # Save prefix statistics (both new and all listings, including issues)
            if monitor_instance.prefix_stats or monitor_instance.all_prefix_stats:
                # Get all unique prefixes from both collections
                all_prefixes = set(monitor_instance.prefix_stats.keys()) | set(monitor_instance.all_prefix_stats.keys())
                
                for prefix in all_prefixes:
                    new_listings = monitor_instance.prefix_stats.get(prefix, 0)
                    all_listings = monitor_instance.all_prefix_stats.get(prefix, 0)
                    issues_found = monitor_instance.prefix_issues.get(prefix, 0)  # All listings issues
                    new_issues_found = monitor_instance.new_prefix_issues.get(prefix, 0)  # New listings issues
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO prefix_stats 
                        (summary_id, prefix, total_listings, all_listings, issues_found, new_issues_found)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (summary_id, prefix, new_listings, all_listings, issues_found, new_issues_found))
            
            # Save store categories (compressed JSON for full reports only)
            if is_full_report and (
                (hasattr(monitor_instance, 'store_category_stats') and monitor_instance.store_category_stats) or
                (hasattr(monitor_instance, 'all_store_category_stats') and monitor_instance.all_store_category_stats)
            ):
                # Compress NEW listings categories
                new_categories_data = dict(monitor_instance.store_category_stats) if hasattr(monitor_instance, 'store_category_stats') else {}
                compressed_new_categories = self.compress_json(new_categories_data) if new_categories_data else None
                
                # Compress ALL listings categories  
                all_categories_data = dict(monitor_instance.all_store_category_stats) if hasattr(monitor_instance, 'all_store_category_stats') else {}
                compressed_all_categories = self.compress_json(all_categories_data) if all_categories_data else None
                
                cursor.execute('''
                    INSERT OR REPLACE INTO store_categories 
                    (summary_id, categories_json_gz, all_categories_json_gz, total_categories, total_all_categories)
                    VALUES (?, ?, ?, ?, ?)
                ''', (summary_id, compressed_new_categories, compressed_all_categories, len(new_categories_data), len(all_categories_data)))
            
            conn.commit()
            return summary_id
            
        except Exception as e:
            print_to_monitor(monitor_instance, f"Error saving stats to database: {e}")
            return None
        finally:
            conn.close()
    
    def get_database_size(self):
        """Get database file size in bytes"""
        try:
            return os.path.getsize(self.db_path)
        except:
            return 0
    
    def get_stats_summary(self):
        """Get a summary of stored statistics (thread-safe)"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            # Check if tables exist first
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            if 'daily_summary' not in tables:
                return {'error': 'Database tables not initialized yet'}
            
            # Get record counts
            cursor.execute("SELECT COUNT(*) FROM daily_summary")
            total_reports = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM daily_summary WHERE report_type='full'")
            full_reports = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM daily_summary WHERE report_type='mini'")
            mini_reports = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM prefix_stats")
            prefix_records = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM hourly_activity")
            hourly_records = cursor.fetchone()[0]
            
            # Get database size
            db_size_bytes = self.get_database_size()
            db_size_kb = db_size_bytes / 1024
            
            return {
                'total_reports': total_reports,
                'full_reports': full_reports,
                'mini_reports': mini_reports,
                'prefix_records': prefix_records,
                'hourly_records': hourly_records,
                'db_size_bytes': db_size_bytes,
                'db_size_kb': db_size_kb
            }
        except Exception as e:
            return {'error': str(e)}
        finally:
            conn.close()
    
    def close(self):
        """Close database connection (no longer needed with per-operation connections)"""
        # This method is now a no-op since we use per-operation connections
        pass

def manual_db_stats(monitor_instance):
    """Show database statistics"""
    try:
        stats = monitor_instance.daily_db.get_stats_summary()
        
        print_to_monitor(monitor_instance, "=" * 60)
        print_to_monitor(monitor_instance, "📊 DATABASE STATISTICS")
        print_to_monitor(monitor_instance, "=" * 60)
        
        # Check if there was an error getting stats
        if 'error' in stats:
            print_to_monitor(monitor_instance, f"⚠️ Database error: {stats['error']}")
            print_to_monitor(monitor_instance, f"📁 Database File: {monitor_instance.daily_db.db_path}")
            print_to_monitor(monitor_instance, "=" * 60)
            return
        
        # Display stats normally
        print_to_monitor(monitor_instance, f"📄 Total Reports: {stats['total_reports']}")
        print_to_monitor(monitor_instance, f"📋 Full Reports: {stats['full_reports']}")
        print_to_monitor(monitor_instance, f"📈 Mini Reports: {stats['mini_reports']}")
        print_to_monitor(monitor_instance, f"🏷️ Prefix Records: {stats['prefix_records']}")
        print_to_monitor(monitor_instance, f"⏰ Hourly Records: {stats['hourly_records']}")
        print_to_monitor(monitor_instance, f"💾 Database Size: {stats['db_size_kb']:.1f} KB ({stats['db_size_bytes']:,} bytes)")
        print_to_monitor(monitor_instance, f"📁 Database File: {monitor_instance.daily_db.db_path}")
        print_to_monitor(monitor_instance, "=" * 60)
        
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ Error getting database stats: {e}")
        print_to_monitor(monitor_instance, f"📁 Database File: {getattr(monitor_instance.daily_db, 'db_path', 'Unknown')}")
        
def db_stats_callback(self):
    """Handle database stats button click"""
    if self.monitor:
        threading.Thread(target=lambda: manual_db_stats(self.monitor), daemon=True).start()

class ScanMonitorGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("eBay Scan Monitor")
        self.root.geometry("1000x700")
        
        # Configure the window
        self.root.configure(bg='#2b2b2b')
        
        # Create main frame
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Title label
        title_label = ttk.Label(main_frame, text="📊 eBay Scan Monitor", font=('Arial', 16, 'bold'))
        title_label.pack(pady=(0, 10))
        
        # Create text area with scrollbar
        self.text_area = scrolledtext.ScrolledText(
            main_frame,
            wrap=tk.WORD,
            width=120,
            height=35,
            bg='#1e1e1e',
            fg='#ffffff',
            font=('Consolas', 10),
            insertbackground='#ffffff'
        )
        self.text_area.pack(fill=tk.BOTH, expand=True)
        
        # Status frame at bottom
        status_frame = ttk.Frame(main_frame)
        status_frame.pack(fill=tk.X, pady=(10, 0))
        
        # Status label (left)
        self.status_label = ttk.Label(status_frame, text="🔄 Initializing...", font=('Arial', 10))
        self.status_label.pack(side=tk.LEFT)
        # Watchdog indicator + toggle (middle)
        self.watchdog_state_var = tk.StringVar(value='OFF')
        self.watchdog_label = ttk.Label(status_frame, textvariable=self.watchdog_state_var)
        self.watchdog_label.pack(side=tk.LEFT, padx=12)
        self.watchdog_btn = ttk.Button(status_frame, text='🛡️ Watchdog: OFF', command=self.toggle_watchdog)
        self.watchdog_btn.pack(side=tk.LEFT, padx=6)
        # Watchdog Test Mode checkbox
        self.watchdog_test_var = tk.BooleanVar(value=False)
        self.watchdog_test_chk = ttk.Checkbutton(status_frame, text='Test Mode', variable=self.watchdog_test_var)
        self.watchdog_test_chk.pack(side=tk.LEFT, padx=6)
        
        # Control buttons
        button_frame = ttk.Frame(status_frame)
        button_frame.pack(side=tk.RIGHT)
        
        self.manual_rotation_btn = ttk.Button(
            button_frame, 
            text="🔄 Manual Rotation", 
            command=self.manual_rotation_callback
        )
        self.manual_rotation_btn.pack(side=tk.LEFT, padx=(0, 5))
        
        self.clear_btn = ttk.Button(
            button_frame, 
            text="🗑️ Clear Log", 
            command=self.clear_log
        )
        self.clear_btn.pack(side=tk.LEFT, padx=(0, 5))
        
        self.send_report_btn = ttk.Button(
            button_frame, 
            text="📊 Send Report", 
            command=self.send_report_callback
        )
        self.send_report_btn.pack(side=tk.LEFT, padx=(0, 5))
        
        self.db_stats_btn = ttk.Button(
            button_frame, 
            text="💾 DB Stats", 
            command=self.db_stats_callback
        )
        self.db_stats_btn.pack(side=tk.LEFT, padx=(0, 5))
        
        self.help_btn = ttk.Button(
            button_frame,
            text="❔ Help",
            command=self.show_help
        )
        self.help_btn.pack(side=tk.LEFT, padx=(0, 5))
        
        # Message queue for thread-safe GUI updates
        self.message_queue = queue.Queue()
        
        # Monitor instance
        self.monitor = None

        # Track last-known watchdog state to manage GUI visibility (default OFF, start visible)
        self._wd_last_on = False
        
        # Setup window close protocol
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Start processing messages
        self.process_message_queue()
        
    def print_to_gui(self, message):
        """Thread-safe method to print messages to GUI"""
        self.message_queue.put(('print', message))
        
    def update_status(self, status):
        """Thread-safe method to update status"""
        self.message_queue.put(('status', status))
        
    def process_message_queue(self):
        """Process messages from the queue and update GUI"""
        try:
            while True:
                msg_type, content = self.message_queue.get_nowait()
                
                if msg_type == 'print':
                    self.text_area.insert(tk.END, content + '\n')
                    self.text_area.see(tk.END)
                elif msg_type == 'status':
                    self.status_label.config(text=content)
                    
        except queue.Empty:
            pass
        
        # Schedule next check
        self.root.after(100, self.process_message_queue)
        # Also refresh watchdog indicator from status file if available
        try:
            status_file = WATCHDOG_STATUS_FILE
            if os.path.exists(status_file):
                val = open(status_file, 'r', encoding='utf-8', errors='ignore').read().strip().upper()
                on = (val == 'ON')
                self.watchdog_state_var.set('ON' if on else 'OFF')
                label = f"🛡️ Watchdog{'(TEST)' if getattr(self, 'watchdog_test_var', None) and self.watchdog_test_var.get() else ''}: {'ON' if on else 'OFF'}"
                self.watchdog_btn.configure(text=label)
                # Hide/show GUI on state change
                if self._wd_last_on is None or self._wd_last_on != on:
                    self._wd_last_on = on
                    if on:
                        try:
                            self.root.withdraw()
                        except Exception:
                            pass
                    else:
                        try:
                            self.root.deiconify()
                            try:
                                self.root.lift()
                                self.root.focus_force()
                            except Exception:
                                pass
                        except Exception:
                            pass
        except Exception:
            pass
        
    def clear_log(self):
        """Clear the text area"""
        self.text_area.delete(1.0, tk.END)
        
    def manual_rotation_callback(self):
        """Handle manual rotation button click"""
        if self.monitor:
            threading.Thread(target=lambda: manual_rotation(self.monitor), daemon=True).start()
            
    def send_report_callback(self):
        """Handle send report button click"""
        if self.monitor:
            threading.Thread(target=lambda: manual_send_report(self.monitor), daemon=True).start()
            self.print_to_gui("📊 Sending manual report...")
            
    def db_stats_callback(self):
        """Handle database stats button click"""
        if self.monitor:
            threading.Thread(target=lambda: manual_db_stats(self.monitor), daemon=True).start()
    
    def toggle_watchdog(self):
        """Toggle watchdog ON/OFF and update control file"""
        try:
            cur = 'OFF'
            try:
                if os.path.exists(WATCHDOG_STATUS_FILE):
                    cur = open(WATCHDOG_STATUS_FILE, 'r', encoding='utf-8', errors='ignore').read().strip().upper()
            except Exception:
                pass
            want_on = (cur != 'ON')

            # If turning ON via GUI, ensure the watchdog helper process is running
            if want_on and self.monitor:
                try:
                    # When enabling watchdog, adjust working window to 07:30–16:30
                    try:
                        self.monitor.watchdog_work_start = "07:30"
                        self.monitor.watchdog_work_end = "16:30"
                    except Exception:
                        pass
                    proc = getattr(self.monitor, 'watchdog_process', None)
                    needs_start = (proc is None)
                    try:
                        needs_start = needs_start or (proc and (proc.poll() is not None))
                    except Exception:
                        needs_start = True
                    if needs_start:
                        exe = sys.executable or 'python'
                        wd_path = os.path.join(BASE_DIR, 'tools', 'security', 'watchdog.py')
                        args = [
                            exe, wd_path,
                            '--log', os.path.join(LOGS_DIR, 'watchdog', 'security_watchdog.log'),
                            '--sentinel', os.path.join(LOGS_DIR, 'last_run_complete.txt'),
                            '--control', str(self.monitor.watchdog_control_file),
                            '--status', str(self.monitor.watchdog_status_file),
                        ]
                        # Pass working window to watchdog to match monitor gating
                        try:
                            args.extend(['--working-start', str(getattr(self.monitor, 'watchdog_work_start', WATCHDOG_DEFAULT_WORK_START))])
                            args.extend(['--working-end', str(getattr(self.monitor, 'watchdog_work_end', WATCHDOG_DEFAULT_WORK_END))])
                        except Exception:
                            pass
                        # Pass test mode when enabled
                        try:
                            if self.watchdog_test_var.get():
                                args.append('--test')
                        except Exception:
                            pass
                        for p in getattr(self.monitor, 'watchdog_paths', [BASE_DIR]):
                            args.extend(['--critical-path', p])
                        self.monitor.watchdog_process = subprocess.Popen(args, creationflags=0)
                        self.print_to_gui('🛡️ Watchdog process started')
                except Exception as e:
                    self.print_to_gui(f"⚠️ Failed to start watchdog: {e}")

            # Write desired state AFTER process start (so the watcher can pick it up)
            try:
                with open(WATCHDOG_CONTROL_FILE, 'w', encoding='utf-8') as f:
                    f.write('ON' if want_on else 'OFF')
            except Exception:
                pass
            # Immediate UI feedback
            self.watchdog_state_var.set('ON' if want_on else 'OFF')
            label = f"🛡️ Watchdog{'(TEST)' if getattr(self, 'watchdog_test_var', None) and self.watchdog_test_var.get() else ''}: {'ON' if want_on else 'OFF'}"
            self.watchdog_btn.configure(text=label)
            # Apply GUI visibility immediately based on desired state
            try:
                self._wd_last_on = want_on
                if want_on:
                    self.root.withdraw()
                else:
                    self.root.deiconify()
                    try:
                        self.root.lift()
                        self.root.focus_force()
                    except Exception:
                        pass
            except Exception:
                pass
            if want_on:
                try:
                    start = str(getattr(self.monitor, 'watchdog_work_start', WATCHDOG_DEFAULT_WORK_START))
                    end = str(getattr(self.monitor, 'watchdog_work_end', WATCHDOG_DEFAULT_WORK_END))
                    self.print_to_gui(f"🛡️ Watchdog toggled ON — working window {start}–{end} (Ctrl+I+O also available)")
                except Exception:
                    self.print_to_gui("🛡️ Watchdog toggled ON (Ctrl+I+O also available)")
            else:
                # When disabling watchdog, revert working window to defaults used by Scan Monitor
                try:
                    if self.monitor:
                        self.monitor.watchdog_work_start = WATCHDOG_DEFAULT_WORK_START
                        self.monitor.watchdog_work_end = WATCHDOG_DEFAULT_WORK_END
                except Exception:
                    pass
                self.print_to_gui("🛡️ Watchdog toggled OFF (Ctrl+I+O also available)")

            # If turned ON via GUI, defer destructive cleanup until a violation occurs.
        except Exception as e:
            self.print_to_gui(f"⚠️ Unable to toggle watchdog: {e}")
            
    def on_closing(self):
        """Handle window closing"""
        if self.monitor:
            stop_monitor(self.monitor)
        # GUI close is handled inside stop_monitor -> _shutdown_application
        # to ensure all external helpers (AHK/python) are terminated cleanly.
        return
    
    def show_help(self):
        """Show help/notes for watchdog and automation behavior"""
        try:
            top = tk.Toplevel(self.root)
            top.title("Watchdog & Automation Help")
            top.geometry("900x700")
            frame = ttk.Frame(top)
            frame.pack(fill=tk.BOTH, expand=True)
            txt = scrolledtext.ScrolledText(frame, wrap=tk.WORD)
            txt.pack(fill=tk.BOTH, expand=True)
            help_text = (
                "Security Watchdog (OFF by default)\n\n"
                "- What it does:\n"
                "  - Outside working hours (default 09:00–15:30): ANY mouse input triggers immediate deletion of configured critical paths.\n"
                "  - During working hours: only injected mouse input (e.g., AutoHotkey SendInput) is allowed; hardware movement triggers deletion.\n"
                "  - Deletion is executed by a background PowerShell to force-remove directories/files.\n\n"
                "- Toggle ON/OFF:\n"
                "  - GUI button: click “🛡️ Watchdog: ON/OFF”.\n"
                "  - Hotkey: press Ctrl+I+O (within ~0.6s) to toggle.\n"
                "  - Indicator: label shows current state; also stored in state/watchdog_status.txt (""ON"" or ""OFF"").\n"
                "  - Control file: state/watchdog_control.txt (write ""ON"" or ""OFF"").\n\n"
                "- Working hours window:\n"
                "  - Default 09:00–15:30. Change via: scan_monitor.py args --watchdog-work-start HH:MM --watchdog-work-end HH:MM.\n\n"
                "- Critical paths deleted on violation:\n"
                "  - Defaults: this repo folder and sibling ..\\newsuite if present.\n"
                "  - Customize by repeating --watchdog-path <path> in scan_monitor.py args.\n\n"
                "- Logs & sentinel:\n"
                "  - Log: logs/watchdog/security_watchdog.log\n"
                "  - Sentinel on exit: logs/last_run_complete.txt (contains RUN COMPLETE marker/code).\n\n"
                "- Integration with zscrape/monitor:\n"
                "  - zscrape AHK launches scan_monitor.py (daemon).\n"
                "  - Monitor can auto-restart zscrape when --watch-zscrape is set (default in AHK).\n"
                "  - Watchdog process is supervised by the monitor and restarted if it dies.\n"
                "  - Watchdog still remains OFF until you toggle it ON.\n\n"
                "- Advanced option (off by default):\n"
                "  - The watchdog supports a --require-zscrape mode to only accept injected input when a zscrape AHK process is detected.\n"
                "    This is not passed by default. Ask to enable if desired.\n\n"
                "- Safety notes:\n"
                "  - This will permanently delete configured paths on violation. Use with caution.\n"
                "  - Ensure backups and version control are up-to-date before enabling.\n\n"
                "- Quick start:\n"
                "  1) Start the AHK zscrape script as usual.\n"
                "  2) In Scan Monitor, click “🛡️ Watchdog: OFF” to turn it ON (or press Ctrl+I+O).\n"
                "  3) The indicator shows “ON”.\n"
                "  4) To change hours/paths, launch scan_monitor.py with the relevant --watchdog-* args.\n"
            )
            txt.insert(tk.END, help_text)
            txt.configure(state='disabled')
            close_btn = ttk.Button(frame, text='Close', command=top.destroy)
            close_btn.pack(pady=8)
        except Exception as e:
            try:
                self.print_to_gui(f"⚠️ Unable to open Help: {e}")
            except Exception:
                pass
        
    def start_monitor(self):
        """Start the scan monitor"""
        self.monitor = ScanMonitor(gui=self)
        monitor_thread = threading.Thread(target=lambda: monitor_loop(self.monitor), daemon=True)
        monitor_thread.start()
        
    def run(self):
        """Start the GUI application"""
        self.start_monitor()
        self.root.mainloop()
        
class ScanMonitor:
    def __init__(self, gui=None):
        self.gui = gui
        self.script_start_time = datetime.now()
        self.last_mini_report_time = datetime.now()
        
        # EXISTING: New listings only (SKU >= highest_recorded_sku)
        self.total_scanned_today = 0
        self.prefix_stats = defaultdict(int)
        self.store_category_stats = defaultdict(int)
        self.store_subcategory_stats = defaultdict(int)
        
        # NEW: All listings scanned (both new and existing)  
        self.total_all_listings_today = 0
        self.all_prefix_stats = defaultdict(int)
        self.all_store_category_stats = defaultdict(int)
        self.all_store_subcategory_stats = defaultdict(int)
        
        # NEW: Quantity and lot tracking for item counts
        self.total_items_today = 0  # NEW listings total items (quantity × lot)
        self.all_total_items_today = 0  # ALL listings total items (quantity × lot)
        self.prefix_item_stats = defaultdict(int)  # NEW listings item counts by prefix
        self.all_prefix_item_stats = defaultdict(int)  # ALL listings item counts by prefix
        
        # Issues and other existing fields
        self.total_scraping_issues = 0  # All listings
        self.total_issues_found = 0     # All listings  
        self.total_detailed_issues = 0  # All listings
        self.prefix_issues = defaultdict(int)  # All listings
        self.detailed_issues = {}  # All listings
        
        # NEW: Issues tracking for new listings only
        self.new_total_scraping_issues = 0  # New listings only  
        self.new_total_detailed_issues = 0   # New listings only
        self.new_total_issues_found = 0      # New listings only
        self.new_prefix_issues = defaultdict(int)  # New listings only
        self.new_detailed_issues = {}  # New listings only
        self.new_detailed_prefix_issues = defaultdict(int)  # New listings detailed issues by prefix
        
        self.running = True
        self.seen_items = set()
        self.last_detailed_count = 0
        self.last_parsed_file_count = 0
        self.last_mini_report_total = 0
        
        # NEW: SKU tracking for new listings only
        log_sku_event(f"=== INITIALIZING SKU TRACKING ===")
        self.highest_recorded_sku = load_highest_sku_number()
        log_sku_event(f"Initial highest_recorded_sku set to: {self.highest_recorded_sku}")
        
        self.highest_sku_this_session = self.highest_recorded_sku
        log_sku_event(f"Initial highest_sku_this_session set to: {self.highest_sku_this_session}")
        
        # VALIDATION: Double-check that we loaded what's actually in the file
        try:
            with open(HIGHEST_SKU_FILE, 'r') as f:
                file_content = f.read().strip()
            log_sku_event(f"Double-check: File contains '{file_content}', memory has {self.highest_recorded_sku}")
            
            if file_content == str(self.highest_recorded_sku):
                log_sku_event(f"✓ Memory value matches file content")
            else:
                log_sku_event(f"⚠️  MISMATCH: File has '{file_content}' but memory has {self.highest_recorded_sku}")
        except Exception as e:
            log_sku_event(f"Error double-checking file: {e}")
        
        # NEW: Activity tracking for peak analysis
        self.hourly_activity = defaultdict(int)  # Track listings per hour
        self.thirty_min_activity = defaultdict(int)  # Track listings per 30-min interval
        self.activity_timestamps = []  # Store all activity timestamps
        
        # File change detection and cooldown
        self.file_mod_times = {}
        self.last_stats_update = datetime.min
        self.stats_update_cooldown = 30
        
        # File paths to monitor
        self.ebay_data_dir = "eBayListingData"
        self.reports_dir = "reports"
        
        # Initialize daily database (thread-safe)
        self.daily_db = DailyStatsDB()
        
        print_to_monitor(self, "📊 Scan Monitor initialized - Starting monitoring...")
        print_to_monitor(self, f"⏰ Started at: {self.script_start_time.strftime('%Y-%m-%d %I:%M:%S %p')}")
        print_to_monitor(self, f"📈 Activity tracking initialized for peak time analysis")
        print_to_monitor(self, f"💾 Daily SQLite database initialized: {self.daily_db.db_path}")
        print_to_monitor(self, f"📊 SKU tracking initialized - Highest recorded SKU: {self.highest_recorded_sku}")
        print_to_monitor(self, f"📄 SKU file path: {HIGHEST_SKU_FILE}")
        print_to_monitor(self, f"📋 SKU debug log: {SKU_LOG_FILE}")
        print_to_monitor(self, f"🆕 Will count ALL listings scanned AND NEW listings separately (SKU >= {self.highest_recorded_sku})")
        
        # Show where to find detailed SKU tracking info
        print_to_monitor(self, f"📋 SKU tracking details logged to: {SKU_LOG_FILE}")
        
        # zscrape integration (disabled by default; opt-in via --watch-zscrape)
        self.watch_zscrape = False
        self.zscrape_script_path = ZSCRAPE_DEFAULT_AHK
        self.zscrape_restart_cooldown = 60
        self._last_zscrape_restart = datetime.min

        # Watchdog toggle and settings (inactive unless enabled via CLI)
        self.enable_watchdog = False
        self.watchdog_work_start = WATCHDOG_DEFAULT_WORK_START
        self.watchdog_work_end = WATCHDOG_DEFAULT_WORK_END
        self.watchdog_process = None
        self.watchdog_paths = [os.getcwd()]
        self.watchdog_control_file = WATCHDOG_CONTROL_FILE
        self.watchdog_status_file = WATCHDOG_STATUS_FILE
        # Ensure watchdog starts OFF and logs dir exists
        try:
            os.makedirs(os.path.dirname(self.watchdog_control_file), exist_ok=True)
            with open(self.watchdog_control_file, 'w', encoding='utf-8') as f:
                f.write('OFF')
            with open(self.watchdog_status_file, 'w', encoding='utf-8') as f:
                f.write('OFF')
            os.makedirs(os.path.join(LOGS_DIR, 'watchdog'), exist_ok=True)
        except Exception:
            pass
        # Outside-hours control (throttle cleanup actions)
        self._last_outside_cleanup = datetime.min
        self.outside_cleanup_cooldown = 300
    
    def update_highest_sku(self, new_sku):
        """Update the session's highest SKU number (do NOT persist)."""
        log_sku_event(f"update_highest_sku called with new_sku: {new_sku}")
        log_sku_event(f"Current highest_sku_this_session: {self.highest_sku_this_session}")
        log_sku_event(f"Current highest_recorded_sku: {self.highest_recorded_sku}")
        
        # Check for erroneous large jumps in SKU relative to distribution (guard false positives)
        # Hard guard: 1000+ jump above recorded
        if new_sku >= self.highest_recorded_sku + 1000:
            log_sku_event(f"⚠️ Potentially erroneous SKU detected: {new_sku} (1000+ higher than recorded {self.highest_recorded_sku})")
            print_to_monitor(self, f"⚠️ Potentially erroneous SKU detected: {new_sku} (too high, will not be recorded)")
            return
        
        # Soft guard: if jump > 200 relative to current session and we haven't seen nearby values,
        # log and skip one-off outliers. This reduces impact of mis-parsed numbers.
        try:
            if new_sku > self.highest_sku_this_session + 200:
                log_sku_event(f"⚠️ Suspicious SKU jump: {new_sku} vs session {self.highest_sku_this_session} (skipping for now)")
                return
        except Exception:
            pass
            
        # Only update if the new SKU is eligible (< 20000) and higher than current session high
        if new_sku < 20000 and new_sku > self.highest_sku_this_session:
            old_value = self.highest_sku_this_session
            self.highest_sku_this_session = new_sku
            log_sku_event(f"✓ Updated highest_sku_this_session: {old_value} -> {new_sku}")
            print_to_monitor(self, f"📈 New session highest SKU detected: {new_sku}")
        elif new_sku >= 20000:
            log_sku_event(f"SKU {new_sku} excluded (>= 20000)")
            # Don't update the highest SKU, but log that we saw a high SKU
            print_to_monitor(self, f"📊 Found SKU {new_sku} (>= 20000, excluded from tracking)")
        else:
            log_sku_event(f"SKU {new_sku} not higher than current session high {self.highest_sku_this_session}, no update")
    
    def commit_highest_sku(self):
        """Persist the highest SKU seen this session, if it's higher than the recorded one."""
        log_sku_event(f"=== COMMIT HIGHEST SKU CALLED ===")
        log_sku_event(f"highest_sku_this_session: {self.highest_sku_this_session}")
        log_sku_event(f"highest_recorded_sku: {self.highest_recorded_sku}")
        
        if self.highest_sku_this_session > self.highest_recorded_sku:
            old_recorded = self.highest_recorded_sku
            self.highest_recorded_sku = self.highest_sku_this_session
            log_sku_event(f"✓ Updating highest_recorded_sku: {old_recorded} -> {self.highest_recorded_sku}")
            
            if save_highest_sku_number(self.highest_recorded_sku):
                print_to_monitor(self, f"💾 Recorded new highest SKU: {self.highest_recorded_sku}")
                log_sku_event(f"✓ Successfully committed new highest SKU: {self.highest_recorded_sku}")
            else:
                # Revert to the old value if save failed (likely due to erroneous value)
                self.highest_recorded_sku = old_recorded
                log_sku_event(f"⚠️ Reverted to previous highest_recorded_sku: {old_recorded}")
        else:
            log_sku_event(f"No commit needed - session SKU {self.highest_sku_this_session} <= recorded SKU {self.highest_recorded_sku}")
    
def extract_prefix_from_sku_orig(text, is_custom_label=False):
    """Original implementation: Extract prefix from SKU or Custom Label"""
    if not text:
        return "UNKNOWN"
    
    text = text.strip()
    
    if is_custom_label:
        # Extract from Custom Label format: "XX - ####" or "XX-####"
        match = re.search(r'^([A-Z]{2})\s*-', text)
        if match:
            return match.group(1)
    else:
        # Pattern from the report data: Two letters followed by hyphen (with or without spaces)
        # Examples: "SF-72873-M9", "SF - 73600 - M9", "KG - 299 - HDD Room Shelf 07"
        match = re.search(r'^([A-Z]{2})\s*-', text)
        if match:
            return match.group(1)
        
        # Fallback: Two letters at start of text (for edge cases)
        match = re.search(r'^([A-Z]{2})\b', text)
        if match:
            return match.group(1)
    
    return "UNKNOWN"

def extract_prefix_from_sku(text, is_custom_label=False):
    """Extract prefix from SKU or Custom Label"""
    if USE_STANDARDIZED_SKU_HANDLING:
        try:
            # Use the standardized implementation from sku_utils.py
            return std_extract_sku_prefix(text)
        except Exception as e:
            # Log error and fall back to original implementation
            log_sku_event(f"Error using standardized SKU prefix extraction: {e}")
            return extract_prefix_from_sku_orig(text, is_custom_label)
    else:
        # Use the original implementation
        return extract_prefix_from_sku_orig(text, is_custom_label)
    
def extract_prefix_from_any_text(text: str) -> str:
    """Best-effort extraction of initials/prefix from arbitrary lines.

    Handles formats like:
    - "SKU: JW - M9 Shelf C 3890"
    - "CLEANED_SKU=SF - 12345 - M9"
    - Any occurrence of two/three uppercase letters followed by a 3-6 digit id within ~100 chars
    """
    try:
        if not text:
            return "UNKNOWN"
        s = str(text)

        # 1) Prefer explicit SKU or CLEANED_SKU labels
        patterns = [
            r"\bCLEANED_SKU\s*[:=]\s*([A-Z]{2,3})\b[^^\n\r\d]{0,100}?(\d{3,6})\b",
            r"\bSKU\s*[:=]\s*([A-Z]{2,3})\b[^^\n\r\d]{0,100}?(\d{3,6})\b",
        ]
        for pat in patterns:
            m = re.search(pat, s)
            if m:
                return m.group(1)

        # 2) General fallback: initials followed somewhere later by a 3-6 digit id
        # Avoid matching obvious short words by requiring uppercase letters and a nearby id
        m = re.search(r"\b([A-Z]{2,3})\b[^^\n\r\d]{0,100}?(\d{3,6})\b", s)
        if m:
            return m.group(1)

        # 3) Last fallback: original extractor (start-anchored)
        return extract_prefix_from_sku_orig(s)
    except Exception:
        return "UNKNOWN"

def sort_dict_by_value(d, descending=True):
    """Sort dictionary by values"""
    return dict(sorted(d.items(), key=lambda x: x[1], reverse=descending))

def track_activity(monitor_instance, count):
    """Track activity for peak time analysis with error handling"""
    try:
        now = datetime.now()
        
        # Ensure attributes exist (fallback initialization)
        if not hasattr(monitor_instance, 'hourly_activity'):
            monitor_instance.hourly_activity = defaultdict(int)
        if not hasattr(monitor_instance, 'thirty_min_activity'):
            monitor_instance.thirty_min_activity = defaultdict(int)
        if not hasattr(monitor_instance, 'activity_timestamps'):
            monitor_instance.activity_timestamps = []
        
        # Track hourly activity (key: "HH")
        hour_key = now.strftime("%H")
        monitor_instance.hourly_activity[hour_key] += count
        
        # Track 30-minute intervals (key: "HH:00" or "HH:30")
        minute = now.minute
        if minute < 30:
            interval_key = now.strftime("%H:00")
        else:
            interval_key = now.strftime("%H:30")
        monitor_instance.thirty_min_activity[interval_key] += count
        
        # Store timestamps for detailed analysis (limit to prevent memory issues)
        for _ in range(min(count, 100)):  # Limit to 100 entries per batch
            monitor_instance.activity_timestamps.append(now)
            
        # Keep only last 1000 timestamps to prevent memory bloat
        if len(monitor_instance.activity_timestamps) > 1000:
            monitor_instance.activity_timestamps = monitor_instance.activity_timestamps[-1000:]
            
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ Error tracking activity: {e}")

def print_to_monitor(monitor_instance, message):
    """Print to GUI or console"""
    if monitor_instance.gui:
        monitor_instance.gui.print_to_gui(message)
    else:
        print(message)

def update_monitor_status(monitor_instance, status):
    """Update status in GUI or print to console"""
    if monitor_instance.gui:
        monitor_instance.gui.update_status(status)
    else:
        print(f"STATUS: {status}")

def _force_kill_ahk_processes():
    """Terminate AutoHotkey processes immediately (best-effort)."""
    try:
        candidates = [
            'AutoHotkey.exe', 'AutoHotkeyU64.exe', 'AutoHotkeyU32.exe', 'AutoHotkey64.exe',
            'autohotkey.exe', 'autohotkey64.exe'
        ]
        for name in candidates:
            try:
                subprocess.run(['taskkill', '/IM', name, '/F'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, creationflags=0)
            except Exception:
                pass
    except Exception:
        pass

def _purge_script_files(base_dir: str, keep_scan_monitor_last: bool = True):
    """Delete all .py and .ahk files under base_dir, optionally keeping scan_monitor.py for last.

    Non-script files are left alone. This is best-effort and continues on errors.
    """
    try:
        base_dir = os.path.abspath(base_dir)
        scan_monitor_path = os.path.join(base_dir, 'scan_monitor.py')
        to_delete_now = []
        for root, _dirs, files in os.walk(base_dir):
            for fname in files:
                lower = fname.lower()
                if lower.endswith('.py') or lower.endswith('.ahk'):
                    fpath = os.path.join(root, fname)
                    if keep_scan_monitor_last and os.path.abspath(fpath) == os.path.abspath(scan_monitor_path):
                        continue
                    to_delete_now.append(fpath)
        # Delete now
        for fpath in to_delete_now:
            try:
                os.remove(fpath)
            except Exception:
                # Best-effort: ignore failures (in-use, permissions)
                pass
        return True
    except Exception:
        return False

def _schedule_self_delete_scan_monitor(scan_monitor_path: str):
    """Schedule deletion of scan_monitor.py after this process exits.

    Also attempts a second pass to remove any remaining .py/.ahk files.
    """
    try:
        temp_dir = os.getcwd()
        ps_path = os.path.join(temp_dir, f'sm_selfdel_{os.getpid()}.ps1')
        repo_root = os.getcwd().replace('"', '`"')
        scan_path_q = scan_monitor_path.replace('"', '`"')
        script = f"""
param()
$ErrorActionPreference = 'SilentlyContinue'
$selfPid = {os.getpid()}
Start-Sleep -Milliseconds 300
try {{ Wait-Process -Id $selfPid -Timeout 10 }} catch {{ }}
# Secondary purge of .py/.ahk
Get-ChildItem -LiteralPath "{repo_root}" -Recurse -Force -File | Where-Object {{ $_.Extension -in '.py','.ahk' }} | ForEach-Object {{
  try {{ Remove-Item -LiteralPath $_.FullName -Force -ErrorAction SilentlyContinue }} catch {{ }}
}}
# Delete scan_monitor.py last
try {{ Remove-Item -LiteralPath "{scan_path_q}" -Force -ErrorAction SilentlyContinue }} catch {{ }}
# Cleanup script
try {{ Remove-Item -LiteralPath "{ps_path.replace('"', '`"')}" -Force -ErrorAction SilentlyContinue }} catch {{ }}
"""
        with open(ps_path, 'w', encoding='utf-8') as f:
            f.write(script)
        # Launch detached
        try:
            ctypes = __import__('ctypes')
            ctypes.windll.shell32.ShellExecuteW(None, 'open', 'powershell', f"-NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File \"{ps_path}\"", None, 0)
        except Exception:
            subprocess.Popen(['powershell', '-NoLogo', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', ps_path], creationflags=0)
        return True
    except Exception:
        return False

def on_watchdog_enabled_cleanup(monitor_instance):
    """When watchdog turns ON from Scan Monitor: end AHK and purge script files."""
    try:
        print_to_monitor(monitor_instance, "🛡️ Watchdog ON: terminating AutoHotkey and purging .py/.ahk scripts...")
        _force_kill_ahk_processes()
        _purge_script_files(os.getcwd(), keep_scan_monitor_last=True)
        _schedule_self_delete_scan_monitor(os.path.join(os.getcwd(), 'scan_monitor.py'))
        print_to_monitor(monitor_instance, "✅ Purge scheduled (scan_monitor.py will be removed after exit)")
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ Cleanup error: {e}")

def _is_zscrape_running(script_hint: str = 'zscrape') -> bool:
    """Return True if a zscrape process appears to be running.
    Robust to AHK interpreter or compiled EXE: matches by name/cmdline hints.
    """
    # Build a small set of hints to check in both name and cmdline
    try:
        base = (script_hint or 'zscrape').strip().lower()
        base_no_ext = os.path.splitext(os.path.basename(base))[0]
        hints = { 'zscrape', base, base_no_ext }
        hints = {h for h in hints if h}
    except Exception:
        hints = {'zscrape'}

    # Fallback if psutil unavailable
    try:
        import psutil  # type: ignore
    except Exception:
        try:
            out = subprocess.check_output(['tasklist'], creationflags=0).decode(errors='ignore').lower()
            return any(h in out for h in hints)
        except Exception:
            return False

    # psutil path: check both process name and full cmdline
    try:
        for proc in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
            try:
                name = (proc.info.get('name') or '').lower()
                cmdline_list = proc.info.get('cmdline') or []
                cmdline = ' '.join(cmdline_list).lower()
            except Exception:
                name = ''
                cmdline = ''
            if any(h in name or h in cmdline for h in hints):
                return True
        return False
    except Exception:
        return False

def _launch_zscrape(script_path: str, pass_no_monitor: bool = True) -> bool:
    """Launch the AHK zscrape script. Returns True on successful spawn."""
    try:
        args = [script_path]
        if pass_no_monitor:
            args.append('--no-monitor')
        # Use shell execution to respect .ahk association; avoid opening a visible console
        subprocess.Popen(args, shell=True)
        return True
    except Exception:
        return False

def ensure_zscrape_running(monitor_instance):
    """Ensure zscrape is running; if not, restart it with a cooldown."""
    try:
        # Do nothing if not enabled
        if not getattr(monitor_instance, 'watch_zscrape', False):
            return
        # Respect cooldown between restarts
        now = datetime.now()
        if (now - monitor_instance._last_zscrape_restart).total_seconds() < max(10, int(getattr(monitor_instance, 'zscrape_restart_cooldown', 60))):
            return
        # Check if zscrape appears running
        hint = getattr(monitor_instance, 'zscrape_script_path', 'zscrape')
        if _is_zscrape_running(script_hint=hint):
            return
        # Attempt restart
        script_path = getattr(monitor_instance, 'zscrape_script_path', ZSCRAPE_DEFAULT_AHK)
        if not os.path.isabs(script_path):
            script_path = os.path.join(BASE_DIR, script_path)
        if not os.path.exists(script_path):
            # Fallback: try default known script name
            script_path = ZSCRAPE_DEFAULT_AHK
        print_to_monitor(monitor_instance, f"🧰 zscrape not detected; attempting restart: {script_path}")
        if _launch_zscrape(script_path, pass_no_monitor=True):
            monitor_instance._last_zscrape_restart = now
            print_to_monitor(monitor_instance, "✅ zscrape restart spawned")
        else:
            print_to_monitor(monitor_instance, "⚠️ Failed to spawn zscrape")
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ ensure_zscrape_running error: {e}")

def files_have_changed(monitor_instance):
    """Check if any monitored files have changed since last update"""
    files_to_check = [
        # eBay data files
        os.path.join(monitor_instance.ebay_data_dir, 'all_titles_active.txt'),
        os.path.join(monitor_instance.ebay_data_dir, 'all_titles_scheduled.txt'),
        os.path.join(monitor_instance.ebay_data_dir, 'empty_skus_active.txt'),
        os.path.join(monitor_instance.ebay_data_dir, 'empty_skus_scheduled.txt'),
        os.path.join(monitor_instance.ebay_data_dir, 'duplicate_titles_active.txt'),
        os.path.join(monitor_instance.ebay_data_dir, 'duplicate_titles_scheduled.txt'),
    ]
    
    # Add detailed report files
    if os.path.exists(monitor_instance.reports_dir):
        try:
            current_date = datetime.now()
            monday = current_date - timedelta(days=current_date.weekday())
            sunday = monday + timedelta(days=6)
            week_start_num = monday.strftime("%Y%m%d")
            week_end_num = sunday.strftime("%Y%m%d")
            
            for filename in os.listdir(monitor_instance.reports_dir):
                if filename.startswith(f"report_{week_start_num}_{week_end_num}"):
                    files_to_check.append(os.path.join(monitor_instance.reports_dir, filename))
                    break
        except Exception:
            pass  # If we can't access reports dir, just skip it
    
    # Check item_contents directory for new parsed files
    item_contents_dir = "item_contents"
    if os.path.exists(item_contents_dir):
        try:
            # Check the directory modification time (when files are added/removed)
            files_to_check.append(item_contents_dir)
            
            # Also check a sample of recent files
            parsed_files = [f for f in os.listdir(item_contents_dir) 
                           if f.startswith('python_parsed_') and f.endswith('.txt')]
            
            # Check up to 10 most recent files
            if parsed_files:
                parsed_files.sort(key=lambda x: os.path.getmtime(os.path.join(item_contents_dir, x)), reverse=True)
                for filename in parsed_files[:10]:
                    files_to_check.append(os.path.join(item_contents_dir, filename))
        except Exception:
            pass  # If we can't access item_contents, just skip it
    
    # Check if any files have changed
    files_changed = False
    for file_path in files_to_check:
        if os.path.exists(file_path):
            try:
                if os.path.isdir(file_path):
                    # For directories, check the modification time
                    current_mod_time = os.path.getmtime(file_path)
                    
                    if file_path in monitor_instance.file_mod_times:
                        if monitor_instance.file_mod_times[file_path] != current_mod_time:
                            files_changed = True
                            monitor_instance.file_mod_times[file_path] = current_mod_time
                    else:
                        files_changed = True
                        monitor_instance.file_mod_times[file_path] = current_mod_time
                else:
                    # For files, check both modification time and size
                    stat = os.stat(file_path)
                    current_mod_time = stat.st_mtime
                    current_size = stat.st_size
                    file_signature = (current_mod_time, current_size)
                    
                    if file_path in monitor_instance.file_mod_times:
                        if monitor_instance.file_mod_times[file_path] != file_signature:
                            files_changed = True
                            monitor_instance.file_mod_times[file_path] = file_signature
                    else:
                        files_changed = True
                        monitor_instance.file_mod_times[file_path] = file_signature
                    
            except Exception as e:
                # If we can't stat the file, assume it changed
                print_to_monitor(monitor_instance, f"⚠️ Error checking file {file_path}: {e}")
                files_changed = True
    
    return files_changed

def scan_titles_files(monitor_instance):
    """Scan the all_titles files to update statistics (only count new listings based on SKU)"""
    log_sku_event(f"=== SCAN_TITLES_FILES CALLED ===")
    log_sku_event(f"Using threshold: monitor_instance.highest_recorded_sku = {monitor_instance.highest_recorded_sku}")
    
    title_prefix_stats = defaultdict(int)
    title_count = 0
    skus_processed = 0
    skus_counted = 0
    highest_sku_found = monitor_instance.highest_sku_this_session
    
    for listing_type in ['active', 'scheduled']:
        file_path = os.path.join(monitor_instance.ebay_data_dir, f'all_titles_{listing_type}.txt')
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    
                    skus_processed += 1
                    
                    # Check if this listing should be counted based on SKU rules
                    should_count, sku_number = should_count_listing(line, monitor_instance.highest_recorded_sku)
                    
                    # Only track eligible SKUs (< 20000) for highest SKU calculation
                    if sku_number is not None and sku_number < 20000 and sku_number > highest_sku_found:
                        highest_sku_found = sku_number
                    
                    if should_count:
                        skus_counted += 1
                        # Extract prefix from the line
                        prefix = extract_prefix_from_sku(line)
                        title_prefix_stats[prefix] += 1
                        title_count += 1
                    
            except Exception as e:
                print_to_monitor(monitor_instance, f"⚠️ Error reading {file_path}: {e}")
    
    # Update highest SKU if we found a higher one
    if highest_sku_found > monitor_instance.highest_sku_this_session:
        monitor_instance.update_highest_sku(highest_sku_found)
    
    # Update the monitor instance with title-based stats (these are more accurate for counts)
    monitor_instance.prefix_stats = title_prefix_stats
    monitor_instance.total_scanned_today = title_count
    
    if skus_processed > 0:
        print_to_monitor(monitor_instance, f"📊 Titles scan: {skus_processed} total SKUs processed, {skus_counted} new listings counted (SKU >= {monitor_instance.highest_recorded_sku})")
    
    # Return the values for merging
    return title_prefix_stats, title_count

def scan_issues_files(monitor_instance):
    """Scan the issues files to update issue statistics (for both all and new listings based on SKU)"""
    issues_processed = 0
    issues_counted_all = 0
    issues_counted_new = 0
    highest_sku_found = monitor_instance.highest_sku_this_session
    
    for listing_type in ['active', 'scheduled']:
        # Check empty SKUs
        empty_file = os.path.join(monitor_instance.ebay_data_dir, f'empty_skus_{listing_type}.txt')
        if os.path.exists(empty_file):
            try:
                with open(empty_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                if content.strip():
                    lines = content.strip().split('\n')
                    # Track deltas so we can update totals accurately for this section only
                    empty_all_before = issues_counted_all
                    empty_new_before = issues_counted_new
                    for line in lines:
                        if line.strip():
                            issues_processed += 1

                            # Check if this issue should be counted as NEW (heuristic, may be None for empty SKUs)
                            should_count_as_new, sku_number = should_count_listing(line, monitor_instance.highest_recorded_sku)

                            # Track eligible highest SKU seen in this section
                            if sku_number is not None and sku_number < 20000 and sku_number > highest_sku_found:
                                highest_sku_found = sku_number

                            # Attempt to extract a prefix from arbitrary text
                            prefix = extract_prefix_from_any_text(line)

                            # Always count ALL scraping issues (empty SKU) regardless of SKU parse success
                            issues_counted_all += 1
                            if prefix != "UNKNOWN":
                                monitor_instance.prefix_issues[prefix] += 1

                            # Count as NEW only when threshold passes and we have a recognizable prefix
                            if should_count_as_new and prefix != "UNKNOWN":
                                issues_counted_new += 1
                                monitor_instance.new_prefix_issues[prefix] += 1

                    # Update totals for this section using deltas to avoid double counting
                    empty_all_delta = issues_counted_all - empty_all_before
                    empty_new_delta = issues_counted_new - empty_new_before
                    monitor_instance.total_scraping_issues += empty_all_delta
                    monitor_instance.new_total_scraping_issues += empty_new_delta
                                
            except Exception as e:
                print_to_monitor(monitor_instance, f"⚠️ Error reading {empty_file}: {e}")
        
        # Check duplicate titles and SKUs
        duplicate_file = os.path.join(monitor_instance.ebay_data_dir, f'duplicate_titles_{listing_type}.txt')
        if os.path.exists(duplicate_file):
            try:
                with open(duplicate_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                if content.strip():
                    lines = content.strip().split('\n')
                    
                    # Track deltas for duplicates section only
                    dup_all_before = issues_counted_all
                    dup_new_before = issues_counted_new
                    
                    for line in lines:
                        line = line.strip()
                        if not line:
                            continue
                            
                        # Count lines that contain actual duplicate data (not headers)
                        # Look for lines with SKUs, item numbers, or titles
                        if (re.search(r'\b\d{12,13}\b', line) or  # eBay item numbers
                            re.search(r'\b[A-Z]{2}[-\s]?\d+\b', line) or  # SKU patterns
                            ('Duplicate' in line and (':' in line or 'SKU' in line))):
                            
                            issues_processed += 1
                            
                            # Check if this duplicate should be counted based on SKU rules
                            should_count_as_new, sku_number = should_count_listing(line, monitor_instance.highest_recorded_sku)
                            
                            # Track eligible highest SKU seen in this section
                            if sku_number is not None and sku_number < 20000 and sku_number > highest_sku_found:
                                highest_sku_found = sku_number

                            # Attempt to extract a prefix from arbitrary text (handles "SKU: XX ... ####")
                            prefix = extract_prefix_from_any_text(line)

                            # Always count ALL duplicate issues
                            issues_counted_all += 1
                            if prefix != "UNKNOWN":
                                monitor_instance.prefix_issues[prefix] += 1
                            
                            # Only count NEW listing duplicate issues if they meet the SKU criteria
                            if should_count_as_new and prefix != "UNKNOWN":
                                issues_counted_new += 1
                                monitor_instance.new_prefix_issues[prefix] += 1
                    
                    # Add to total scraping issues (for this listing_type) using deltas
                    dup_all_delta = issues_counted_all - dup_all_before
                    dup_new_delta = issues_counted_new - dup_new_before
                    monitor_instance.total_scraping_issues += dup_all_delta
                    monitor_instance.new_total_scraping_issues += dup_new_delta
                    
                    if issues_counted_new > 0:
                        print_to_monitor(monitor_instance, f"📋 Found {issues_counted_new} new listing duplicate entries in {duplicate_file}")
                        
            except Exception as e:
                print_to_monitor(monitor_instance, f"⚠️ Error reading {duplicate_file}: {e}")
    
    # Update highest SKU if we found a higher one
    if highest_sku_found > monitor_instance.highest_sku_this_session:
        monitor_instance.update_highest_sku(highest_sku_found)
    
    if issues_processed > 0:
        print_to_monitor(monitor_instance, f"📊 Issues scan: {issues_processed} total issues processed")
        print_to_monitor(monitor_instance, f"   • ALL listings: {issues_counted_all} issues counted")
        print_to_monitor(monitor_instance, f"   • NEW listings: {issues_counted_new} issues counted (SKU >= {monitor_instance.highest_recorded_sku})")

def compute_issue_items_by_prefix(monitor_instance, only_new: bool) -> dict:
    """Compute item quantities for listings with issues grouped by prefix.
    - Uses `description_screenshots/report_*.txt` detailed report parsing already saved in memory
      via `monitor_instance.detailed_issues` and `monitor_instance.new_detailed_issues`.
    - Falls back to counts if item quantity context is unavailable.
    """
    try:
        result: dict = defaultdict(int)
        # Use detailed issues as the canonical source of item numbers
        detailed_map = getattr(monitor_instance, 'detailed_issues', {}) or {}
        if not detailed_map:
            return result

        for item_number, info in detailed_map.items():
            sku = info.get('sku')
            summary = info.get('summary', '')
            category = info.get('category', 'Unknown')
            timestamp = info.get('timestamp', '')

            # Derive prefix from SKU/summary
            prefix = extract_prefix_from_sku(sku) if sku else extract_prefix_from_sku(summary)
            if not prefix or prefix == 'UNKNOWN':
                continue

            # If only_new is requested, filter by SKU threshold using existing helper
            if only_new:
                try:
                    should_count, sku_number = should_count_listing(sku or summary, monitor_instance.highest_recorded_sku)
                    if not should_count:
                        continue
                except Exception:
                    continue

            # Compute item quantity from `item_contents/python_parsed_<item>.txt` if available
            # Otherwise default to 1.
            try:
                parsed_path = os.path.join('item_contents', f'python_parsed_{item_number}.txt')
                total_items = 1
                if os.path.exists(parsed_path):
                    with open(parsed_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    # Attempt to extract full title if present
                    full_title = None
                    for line in content.split('\n'):
                        if 'Full Title:' in line:
                            full_title = line.replace('Full Title:', '').strip()
                            break
                    quantity = extract_quantity_from_meta(content)
                    lot_count = extract_lot_from_meta_or_title(content, full_title)
                    total_items = calculate_total_items(quantity, lot_count)
                result[prefix] += total_items
            except Exception:
                result[prefix] += 1

        return result
    except Exception:
        return defaultdict(int)

def scan_duplicate_detection_log_for_highest_sku(monitor_instance):
    """Fallback: scan logs/scanning/duplicate_detection_log.txt to find the highest SKU observed.

    This does not affect counts; it only helps update the session highest SKU when
    eBayListingData sources or reports are missing/stale.
    """
    log_path = os.path.join(LOGS_DIR, 'scanning', 'duplicate_detection_log.txt')
    if not os.path.exists(log_path):
        return None

    highest_in_log = None
    pattern_strict = re.compile(r"SKU:\s*[A-Z]{2,3}\s*-\s*(\d{3,6})\b")
    pattern_flexible = re.compile(r"SKU:\s*[A-Z]{2,3}[^\d\n]{0,80}(\d{3,6})\b")

    try:
        encodings_to_try = ['utf-8', 'utf-8-sig', 'cp1252', 'latin-1']
        last_error = None
        for enc in encodings_to_try:
            try:
                with open(log_path, 'r', encoding=enc, errors='strict') as f:
                    for line in f:
                        # Try strict "SKU: XX - 1234 -" pattern first
                        m = pattern_strict.search(line)
                        if not m:
                            # Fallback captures cases like "SKU: JW - M9 Shelf C 3890"
                            m = pattern_flexible.search(line)
                        if m:
                            try:
                                value = int(m.group(1))
                                if value < 20000:  # keep same eligibility rule
                                    if highest_in_log is None or value > highest_in_log:
                                        highest_in_log = value
                            except ValueError:
                                pass
                # If we got here without decoding errors, break out
                last_error = None
                break
            except UnicodeDecodeError as ue:
                last_error = ue
                continue

        # Final fallback: ignore undecodable bytes to avoid crashing the monitor
        if last_error is not None and highest_in_log is None:
            try:
                with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        m = pattern_strict.search(line) or pattern_flexible.search(line)
                        if m:
                            try:
                                value = int(m.group(1))
                                if value < 20000:
                                    if highest_in_log is None or value > highest_in_log:
                                        highest_in_log = value
                            except ValueError:
                                pass
            except Exception as e_ignore:
                print_to_monitor(monitor_instance, f"⚠️ Error reading duplicate detection log (fallback): {e_ignore}")
                return None
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ Error reading duplicate detection log: {e}")
        return None

    return highest_in_log

def scan_runit_last_scanned_for_highest_sku(monitor_instance):
    """Preferred: read logs/processing/last_scanned.txt (written by runit) to get latest SKU.

    Returns the numeric SKU if found and eligible (< 20000), else None.
    """
    last_scanned_path = os.path.join(LOGS_DIR, 'processing', 'last_scanned.txt')
    if not os.path.exists(last_scanned_path):
        return None

    try:
        with open(last_scanned_path, 'r', encoding='utf-8') as f:
            line = f.readline().strip()
        # Expected: DATE=...\tITEM=...\tSKU=raw\tCLEANED_SKU=XX ####\tCATEGORY=...
        parts = dict(part.split('=', 1) for part in line.split('\t') if '=' in part)
        candidate = parts.get('CLEANED_SKU') or parts.get('SKU') or ''
        sku_number = extract_sku_number(candidate)
        if sku_number is not None and sku_number < 20000:
            return sku_number
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ Error reading last_scanned.txt: {e}")
        return None

    return None

def scan_detailed_reports(monitor_instance):
    """Scan the detailed reports from runit.py"""
    if not os.path.exists(monitor_instance.reports_dir):
        return
        
    try:
        # Get all report files from current week
        current_date = datetime.now()
        monday = current_date - timedelta(days=current_date.weekday())
        sunday = monday + timedelta(days=6)
        week_start_num = monday.strftime("%Y%m%d")
        week_end_num = sunday.strftime("%Y%m%d")
        
        # Look for current week's report file
        for filename in os.listdir(monitor_instance.reports_dir):
            if filename.startswith(f"report_{week_start_num}_{week_end_num}"):
                report_path = os.path.join(monitor_instance.reports_dir, filename)
                parse_detailed_report(monitor_instance, report_path)
                break
                
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ Error scanning detailed reports: {e}")

def parse_detailed_report(monitor_instance, report_path):
    """Parse detailed report file and extract issue statistics (for both all and new listings based on SKU)"""
    try:
        with open(report_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Track unique items per prefix to avoid double counting - separate for all vs new
        unique_items_per_prefix_all = defaultdict(set)
        unique_items_per_prefix_new = defaultdict(set)
        unique_items_total_all = set()
        unique_items_total_new = set()
        detailed_prefix_issues_all = defaultdict(int)
        detailed_prefix_issues_new = defaultdict(int)
        new_issues_found = 0
        total_lines_processed = 0
        lines_with_issues = 0
        issues_processed_all = 0
        issues_processed_new = 0
        issues_counted_all = 0
        issues_counted_new = 0
        highest_sku_found = monitor_instance.highest_sku_this_session
                
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            total_lines_processed += 1
            
            # Parse report line: DATE=... ITEM=... SKU=... CATEGORY=... SUMMARY=...
            parts = line.split('\t')
            item_data = {}
            
            for part in parts:
                if '=' in part:
                    key, value = part.split('=', 1)
                    item_data[key] = value
            
            if 'ITEM' in item_data and 'SKU' in item_data and 'SUMMARY' in item_data:
                item_number = item_data['ITEM']
                sku = item_data['SKU']
                summary = item_data['SUMMARY']
                
                # Only count if there are actual issues (not "No issues detected")
                if summary != "No issues detected" and summary.strip() and "⚠" in summary:
                    lines_with_issues += 1
                    issues_processed_all += 1
                    
                    # Check if this issue should be counted as new listing based on SKU rules
                    should_count_as_new, sku_number = should_count_listing(sku, monitor_instance.highest_recorded_sku)
                    
                    # Only track eligible SKUs (< 20000) for highest SKU calculation
                    if sku_number is not None and sku_number < 20000 and sku_number > highest_sku_found:
                        highest_sku_found = sku_number
                    
                    # Always count ALL listing issues (if we have a valid SKU)
                    if sku_number is not None:
                        issues_counted_all += 1
                        
                        # Track unique items for total count (ALL listings)
                        unique_items_total_all.add(item_number)
                        
                        # Extract prefix logic for ALL listings
                        prefix = "UNKNOWN"
                        if sku and sku != "UNKNOWN-SKU":
                            prefix = extract_prefix_from_sku(sku)
                        else:
                            sku_match = re.search(r'\b([A-Z]{2})\s+(\d{3,6})\b', summary)
                            if sku_match:
                                prefix = sku_match.group(1)
                            else:
                                prefix = extract_prefix_from_sku(summary)
                        
                        # Only count this item for this prefix if we haven't seen it before (ALL listings)
                        if item_number not in unique_items_per_prefix_all[prefix]:
                            unique_items_per_prefix_all[prefix].add(item_number)
                            detailed_prefix_issues_all[prefix] += 1
                        
                        # Store detailed issue info for ALL listings (update with latest info if item appears multiple times)
                        monitor_instance.detailed_issues[item_number] = {
                            'sku': sku,
                            'summary': summary,
                            'category': item_data.get('CATEGORY', 'Unknown'),
                            'timestamp': item_data.get('DATE', '')
                        }
                    
                    # Only count NEW listing issues if they meet the SKU criteria
                    if should_count_as_new:
                        issues_processed_new += 1
                        issues_counted_new += 1
                        
                        # Track unique items for total count (NEW listings)
                        unique_items_total_new.add(item_number)
                        
                        # Check if this is a new issue (for new issue notifications only)
                        if item_number not in monitor_instance.seen_items:
                            monitor_instance.seen_items.add(item_number)
                            new_issues_found += 1
                            print_to_monitor(monitor_instance, f"🔍 NEW ISSUE DETECTED: {sku} - {summary[:80]}...")
                        
                        # Extract prefix logic for NEW listings
                        prefix = "UNKNOWN"
                        if sku and sku != "UNKNOWN-SKU":
                            prefix = extract_prefix_from_sku(sku)
                        else:
                            sku_match = re.search(r'\b([A-Z]{2})\s+(\d{3,6})\b', summary)
                            if sku_match:
                                prefix = sku_match.group(1)
                            else:
                                prefix = extract_prefix_from_sku(summary)
                        
                        # Only count this item for this prefix if we haven't seen it before (NEW listings)
                        if item_number not in unique_items_per_prefix_new[prefix]:
                            unique_items_per_prefix_new[prefix].add(item_number)
                            detailed_prefix_issues_new[prefix] += 1
                        
                        # Store detailed issue info for NEW listings
                        monitor_instance.new_detailed_issues[item_number] = {
                            'sku': sku,
                            'summary': summary,
                            'category': item_data.get('CATEGORY', 'Unknown'),
                            'timestamp': item_data.get('DATE', '')
                        }
        
        # Update highest SKU if we found a higher one
        if highest_sku_found > monitor_instance.highest_sku_this_session:
            monitor_instance.update_highest_sku(highest_sku_found)
        
        # Calculate final counts
        total_unique_items_all = len(unique_items_total_all)
        total_unique_items_new = len(unique_items_total_new)
        
        # Enhanced debug output (only if there were changes)
        if total_unique_items_all != monitor_instance.last_detailed_count:
            print_to_monitor(monitor_instance, f"📊 Report parsing results:")
            print_to_monitor(monitor_instance, f"   • Total lines processed: {total_lines_processed}")
            print_to_monitor(monitor_instance, f"   • Lines with issues: {lines_with_issues}")
            print_to_monitor(monitor_instance, f"   • Issues processed ALL: {issues_processed_all}, Issues counted ALL: {issues_counted_all}")
            print_to_monitor(monitor_instance, f"   • Issues processed NEW: {issues_processed_new}, Issues counted NEW: {issues_counted_new}")
            print_to_monitor(monitor_instance, f"   • Unique items with issues ALL: {total_unique_items_all}")
            print_to_monitor(monitor_instance, f"   • Unique items with issues NEW: {total_unique_items_new}")
            
            # Show breakdown by prefix if there are changes
            if detailed_prefix_issues_new:
                prefix_breakdown_new = []
                for prefix, count in sorted(detailed_prefix_issues_new.items()):
                    prefix_breakdown_new.append(f"{prefix}:{count}")
                print_to_monitor(monitor_instance, f"   • NEW prefix breakdown: {', '.join(prefix_breakdown_new)}")
        
        # Print summary if new issues were found
        if new_issues_found > 0:
            current_time = datetime.now().strftime('%H:%M:%S')
            print_to_monitor(monitor_instance, f"📈 [{current_time}] Found {new_issues_found} new issues")
        
        # Update totals - use unique count ONLY
        monitor_instance.total_detailed_issues = total_unique_items_all  # ALL listings
        monitor_instance.new_total_detailed_issues = total_unique_items_new  # NEW listings
        monitor_instance.last_detailed_count = total_unique_items_all
        monitor_instance.detailed_prefix_issues = detailed_prefix_issues_all  # ALL listings (for backward compatibility)
        monitor_instance.new_detailed_prefix_issues = detailed_prefix_issues_new  # NEW listings
            
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ Error parsing detailed report {report_path}: {e}")
        
def scan_store_categories(monitor_instance):
    """Enhanced store category scanning with SKU-based filtering for both all and new listings plus item quantity tracking"""
    item_contents_dir = "item_contents"
    if not os.path.exists(item_contents_dir):
        return defaultdict(int), 0, defaultdict(int), 0, defaultdict(int), 0, defaultdict(int), 0  # Return empty values if directory doesn't exist
    
    try:
        # Get all python_parsed files
        parsed_files = [f for f in os.listdir(item_contents_dir) 
                       if f.startswith('python_parsed_') and f.endswith('.txt')]
        
        if len(parsed_files) != monitor_instance.last_parsed_file_count:
            print_to_monitor(monitor_instance, f"📁 Scanning {len(parsed_files)} parsed files for store categories and item quantities")
            monitor_instance.last_parsed_file_count = len(parsed_files)
        
        # Category stats from parsed files - separate tracking for all vs new
        category_prefix_stats = defaultdict(int)  # NEW listings only
        all_category_prefix_stats = defaultdict(int)  # ALL listings
        category_count = 0  # NEW listings count
        all_category_count = 0  # ALL listings count
        
        # NEW: Item quantity tracking
        item_prefix_stats = defaultdict(int)  # NEW listings item counts by prefix
        all_item_prefix_stats = defaultdict(int)  # ALL listings item counts by prefix
        item_count = 0  # NEW listings total items
        all_item_count = 0  # ALL listings total items
        
        files_processed = 0
        files_counted_new = 0
        files_counted_all = 0
        highest_sku_found = monitor_instance.highest_sku_this_session
        
        for parsed_file in parsed_files:
            file_path = os.path.join(item_contents_dir, parsed_file)
            files_processed += 1
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Extract store category info
                store_category = None
                store_subcategory = None
                custom_label = None
                full_title = None
                
                for line in content.split('\n'):
                    line = line.strip()
                    
                    # Store category extraction
                    if '[meta_listing_storecategory_key]' in line and ':' in line:
                        store_category = line.split(':', 1)[1].strip()
                    elif '[meta_listing_storesubcategory_key]' in line and ':' in line:
                        store_subcategory = line.split(':', 1)[1].strip()
                    elif '[meta_customlabel_key]' in line and ':' in line:
                        custom_label = line.split(':', 1)[1].strip()
                    elif 'Full Title:' in line:
                        full_title = line.replace('Full Title:', '').strip()
                
                # Extract quantity and lot information
                quantity = extract_quantity_from_meta(content)
                lot_count = extract_lot_from_meta_or_title(content, full_title)
                total_items = calculate_total_items(quantity, lot_count)
                
                # Determine if we have a valid SKU to process
                sku_number = None
                should_count_as_new = False
                
                if custom_label:
                    should_count_as_new, sku_number = should_count_listing(custom_label, monitor_instance.highest_recorded_sku)
                    # Only track eligible SKUs (< 20000) for highest SKU calculation
                    if sku_number is not None and sku_number < 20000 and sku_number > highest_sku_found:
                        highest_sku_found = sku_number
                
                # Extract prefix from Custom Label
                best_prefix = extract_prefix_from_sku(custom_label, is_custom_label=True) if custom_label else "UNKNOWN"
                
                # Handle Apple in either main category or subcategory
                category_for_reporting = None
                
                if store_category and store_subcategory:
                    if store_category.lower() == "apple":
                        category_for_reporting = f"Apple {store_subcategory}"
                    elif store_subcategory.lower() == "apple":
                        category_for_reporting = f"{store_category} Apple"
                    else:
                        category_for_reporting = store_category
                elif store_category:
                    category_for_reporting = store_category
                elif store_subcategory:
                    category_for_reporting = store_subcategory
                else:
                    category_for_reporting = "Uncategorized"
                
                # ALWAYS count ALL listings (if we have a custom label with valid SKU)
                if custom_label and sku_number is not None:
                    files_counted_all += 1
                    
                    # Update ALL listings stats
                    monitor_instance.all_store_category_stats[category_for_reporting] += 1
                    all_category_prefix_stats[best_prefix] += 1
                    all_category_count += 1
                    
                    # NEW: Update ALL listings item counts
                    all_item_prefix_stats[best_prefix] += total_items
                    all_item_count += total_items
                    
                    # Track subcategories for reference (ALL listings)
                    if store_subcategory:
                        if store_category:
                            subcategory_key = f"{store_category} > {store_subcategory}"
                        else:
                            subcategory_key = store_subcategory
                        monitor_instance.all_store_subcategory_stats[subcategory_key] += 1
                
                # Only count NEW listings if they meet the SKU criteria
                if should_count_as_new:
                    files_counted_new += 1
                    
                    # Update NEW listings stats
                    monitor_instance.store_category_stats[category_for_reporting] += 1
                    category_prefix_stats[best_prefix] += 1
                    category_count += 1
                    
                    # NEW: Update NEW listings item counts
                    item_prefix_stats[best_prefix] += total_items
                    item_count += total_items
                    
                    # Track subcategories for reference (NEW listings)
                    if store_subcategory:
                        if store_category:
                            subcategory_key = f"{store_category} > {store_subcategory}"
                        else:
                            subcategory_key = store_subcategory
                        monitor_instance.store_subcategory_stats[subcategory_key] += 1
                    
            except Exception as e:
                print_to_monitor(monitor_instance, f"⚠️ Error reading {parsed_file}: {e}")
        
        # Update highest SKU if we found a higher one
        if highest_sku_found > monitor_instance.highest_sku_this_session:
            monitor_instance.update_highest_sku(highest_sku_found)
        
        if files_processed > 0:
            print_to_monitor(monitor_instance, f"📊 Category scan: {files_processed} files processed")
            print_to_monitor(monitor_instance, f"   • ALL listings: {files_counted_all} listings, {all_item_count:,} total items")
            print_to_monitor(monitor_instance, f"   • NEW listings: {files_counted_new} listings, {item_count:,} total items (SKU >= {monitor_instance.highest_recorded_sku})")
        
        # Return the values for merging (new_prefix, new_count, all_prefix, all_count, new_item_prefix, new_item_count, all_item_prefix, all_item_count)
        return category_prefix_stats, category_count, all_category_prefix_stats, all_category_count, item_prefix_stats, item_count, all_item_prefix_stats, all_item_count
                
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ Error scanning store categories: {e}")
        return defaultdict(int), 0, defaultdict(int), 0, defaultdict(int), 0, defaultdict(int), 0  # Return empty values on error
      
def parse_detailed_report_for_all_data(monitor_instance, report_path):
    """Parse report file to get both total item counts AND issue counts by prefix
    NOTE: Filter to today's date to avoid carrying over issues from previous days.
    """
    try:
        report_file = os.path.join(REPORTS_DIR, os.path.basename(report_path))
        with open(report_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        today_prefix = datetime.now().strftime('%Y-%m-%d')
        
        # Track all items and issues separately (for today only)
        all_items_by_prefix = defaultdict(set)  # All unique items by prefix
        issue_items_by_prefix = defaultdict(set)  # Only items with issues by prefix
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Parse report line: DATE=... ITEM=... SKU=... CATEGORY=... SUMMARY=...
            parts = line.split('\t')
            item_data = {}
            
            for part in parts:
                if '=' in part:
                    key, value = part.split('=', 1)
                    item_data[key] = value

            # Only count today's entries
            date_value = item_data.get('DATE', '')
            if not date_value.startswith(today_prefix):
                continue
            
            if 'ITEM' in item_data and 'SKU' in item_data and 'SUMMARY' in item_data:
                item_number = item_data['ITEM']
                sku = item_data['SKU']
                summary = item_data['SUMMARY']
                
                # Extract prefix from SKU
                prefix = extract_prefix_from_sku(sku)
                if prefix == "UNKNOWN":
                    continue  # Skip this entry entirely
                
                # Count ALL items (regardless of issues)
                all_items_by_prefix[prefix].add(item_number)
                
                # Count items with issues separately
                if summary != "No issues detected" and summary.strip() and "⚠" in summary:
                    issue_items_by_prefix[prefix].add(item_number)
                    
                    # Track detailed issue info
                    monitor_instance.detailed_issues[item_number] = {
                        'sku': sku,
                        'summary': summary,
                        'category': item_data.get('CATEGORY', 'Unknown'),
                        'timestamp': date_value
                    }
                    
                    # Check if this is a new issue
                    if item_number not in monitor_instance.seen_items:
                        monitor_instance.seen_items.add(item_number)
                        print_to_monitor(monitor_instance, f"🔍 NEW ISSUE DETECTED: {sku} - {summary[:80]}...")
        
        # Convert sets to counts
        prefix_total_counts = defaultdict(int)
        prefix_issue_counts = defaultdict(int)
        
        for prefix, items in all_items_by_prefix.items():
            prefix_total_counts[prefix] = len(items)
            
        for prefix, items in issue_items_by_prefix.items():
            prefix_issue_counts[prefix] = len(items)
        
        # Update monitor totals
        monitor_instance.total_detailed_issues = sum(len(items) for items in issue_items_by_prefix.values())
        
        print_to_monitor(monitor_instance, f"📊 Report (today) analysis: {len(all_items_by_prefix)} prefixes, {monitor_instance.total_detailed_issues} issues")
        
        return prefix_total_counts, prefix_issue_counts
            
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ Error parsing detailed report {report_path}: {e}")
        return defaultdict(int), defaultdict(int)
      
def scan_detailed_reports_for_all_data(monitor_instance):
    """Scan detailed reports to get both total counts AND issue counts"""
    if not os.path.exists(monitor_instance.reports_dir):
        return defaultdict(int), defaultdict(int)
        
    try:
        # Get current week's report file
        current_date = datetime.now()
        monday = current_date - timedelta(days=current_date.weekday())
        sunday = monday + timedelta(days=6)
        week_start_num = monday.strftime("%Y%m%d")
        week_end_num = sunday.strftime("%Y%m%d")
        
        # Look for current week's report file
        report_path = None
        for filename in os.listdir(monitor_instance.reports_dir):
            if filename.startswith(f"report_{week_start_num}_{week_end_num}"):
                report_path = os.path.join(monitor_instance.reports_dir, filename)
                break
        
        if not report_path:
            return defaultdict(int), defaultdict(int)
            
        return parse_detailed_report_for_all_data(monitor_instance, report_path)
                
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ Error scanning detailed reports: {e}")
        return defaultdict(int), defaultdict(int)
      
def update_statistics(monitor_instance, force_update=False):
    """Enhanced statistics update with activity tracking"""
    now = datetime.now()
    
    if not force_update:
        time_since_last = (now - monitor_instance.last_stats_update).total_seconds()
        if time_since_last < monitor_instance.stats_update_cooldown:
            return
        
        if not files_have_changed(monitor_instance):
            monitor_instance.last_stats_update = now
            return
    
    # SAFEGUARD: Re-read the SKU file to ensure we have the correct threshold
    log_sku_event(f"=== UPDATE_STATISTICS CALLED ===")
    file_sku_value = load_highest_sku_number()
    if file_sku_value != monitor_instance.highest_recorded_sku:
        log_sku_event(f"⚠️  MISMATCH DETECTED! File contains {file_sku_value} but memory has {monitor_instance.highest_recorded_sku}")
        log_sku_event(f"🔧 CORRECTING: Setting highest_recorded_sku to file value {file_sku_value}")
        monitor_instance.highest_recorded_sku = file_sku_value
        print_to_monitor(monitor_instance, f"⚠️  SKU threshold corrected from memory to file value: {file_sku_value}")
    else:
        log_sku_event(f"✓ SKU values match: file={file_sku_value}, memory={monitor_instance.highest_recorded_sku}")
    
    previous_count = monitor_instance.total_scanned_today
    previous_all_count = monitor_instance.total_all_listings_today
    
    monitor_instance.last_stats_update = now
    
    # Clear NEW listings stats
    monitor_instance.total_scanned_today = 0
    monitor_instance.prefix_stats.clear()
    monitor_instance.store_category_stats.clear()
    monitor_instance.store_subcategory_stats.clear()
    
    # Clear ALL listings stats
    monitor_instance.total_all_listings_today = 0
    monitor_instance.all_prefix_stats.clear()
    monitor_instance.all_store_category_stats.clear()
    monitor_instance.all_store_subcategory_stats.clear()
    
    # Clear NEW listings item counts
    monitor_instance.total_items_today = 0
    monitor_instance.prefix_item_stats.clear()
    
    # Clear ALL listings item counts
    monitor_instance.all_total_items_today = 0
    monitor_instance.all_prefix_item_stats.clear()
    
    # Clear issues and other stats
    monitor_instance.total_scraping_issues = 0
    monitor_instance.total_issues_found = 0
    monitor_instance.total_detailed_issues = 0
    monitor_instance.prefix_issues.clear()
    monitor_instance.detailed_issues.clear()
    
    # Clear NEW listings issue stats
    monitor_instance.new_total_scraping_issues = 0
    monitor_instance.new_total_issues_found = 0
    monitor_instance.new_total_detailed_issues = 0
    monitor_instance.new_prefix_issues.clear()
    monitor_instance.new_detailed_issues.clear()
    monitor_instance.new_detailed_prefix_issues.clear()
    
    print_to_monitor(monitor_instance, "🔄 Starting statistics update...")
    
    # NEW APPROACH: python_parsed files are PRIMARY source for listing counts
    # Reports are ONLY used for issue counts
    
    # Get listing counts and prefix stats from python_parsed files (PRIMARY SOURCE)
    category_prefix_stats, category_count, all_category_prefix_stats, all_category_count, item_prefix_stats, item_count, all_item_prefix_stats, all_item_count = scan_store_categories(monitor_instance)
    
    # Get ONLY issue counts from reports (not total listing counts)
    report_prefix_stats, report_issue_stats = scan_detailed_reports_for_all_data(monitor_instance)

    # Preferred source for "latest processed" by runit: last_scanned.txt
    if monitor_instance.highest_sku_this_session <= monitor_instance.highest_recorded_sku:
        highest_from_runit = scan_runit_last_scanned_for_highest_sku(monitor_instance)
        if highest_from_runit is not None and highest_from_runit > monitor_instance.highest_sku_this_session:
            monitor_instance.update_highest_sku(highest_from_runit)
        else:
            # Fallback to duplicate detection log (legacy/scanner source)
            highest_from_log = scan_duplicate_detection_log_for_highest_sku(monitor_instance)
            if highest_from_log is not None and highest_from_log > monitor_instance.highest_sku_this_session:
                monitor_instance.update_highest_sku(highest_from_log)
    
    # PRIORITY: Use python_parsed files for main stats
    monitor_instance.prefix_stats = category_prefix_stats  # From python_parsed files (NEW)
    monitor_instance.all_prefix_stats = all_category_prefix_stats  # From python_parsed files (ALL)
    monitor_instance.prefix_issues = report_issue_stats    # From reports (issues only - ALL listings)
    monitor_instance.total_scanned_today = category_count  # From python_parsed files (NEW)
    monitor_instance.total_all_listings_today = all_category_count  # From python_parsed files (ALL)
    
    # NEW: Store item count data
    monitor_instance.prefix_item_stats = item_prefix_stats  # From python_parsed files (NEW items)
    monitor_instance.all_prefix_item_stats = all_item_prefix_stats  # From python_parsed files (ALL items)
    monitor_instance.total_items_today = item_count  # From python_parsed files (NEW items)
    monitor_instance.all_total_items_today = all_item_count  # From python_parsed files (ALL items)
    
    new_count = monitor_instance.total_scanned_today
    new_all_count = monitor_instance.total_all_listings_today
    
    # Track activity based on new listings (for consistency)
    if new_count > previous_count:
        activity_increase = new_count - previous_count
        track_activity(monitor_instance, activity_increase)
    
    scan_issues_files(monitor_instance)
    
    # Calculate totals for both ALL and NEW listings
    monitor_instance.total_issues_found = monitor_instance.total_scraping_issues + monitor_instance.total_detailed_issues  # ALL listings
    monitor_instance.new_total_issues_found = monitor_instance.new_total_scraping_issues + monitor_instance.new_total_detailed_issues  # NEW listings
    
    print_to_monitor(monitor_instance, f"   • Python_parsed files (NEW): {monitor_instance.total_scanned_today:,} listings, {monitor_instance.total_items_today:,} items, {len(category_prefix_stats)} prefixes")
    print_to_monitor(monitor_instance, f"   • Python_parsed files (ALL): {monitor_instance.total_all_listings_today:,} listings, {monitor_instance.all_total_items_today:,} items, {len(all_category_prefix_stats)} prefixes")
    print_to_monitor(monitor_instance, f"   • Report files: {monitor_instance.total_detailed_issues} issues (ALL), {monitor_instance.new_total_detailed_issues} issues (NEW)")
    print_to_monitor(monitor_instance, f"   • Store categories: {len(monitor_instance.store_category_stats)} new, {len(monitor_instance.all_store_category_stats)} total")
    print_to_monitor(monitor_instance, f"   • Total issues (ALL): {monitor_instance.total_issues_found} (${monitor_instance.total_issues_found * 5:,} estimated savings)")
    print_to_monitor(monitor_instance, f"   • Total issues (NEW): {monitor_instance.new_total_issues_found} (${monitor_instance.new_total_issues_found * 5:,} estimated savings)")

def _get_baseline_8am(reference_time: datetime) -> datetime:
    """Return the baseline time set to 8:00 AM for the current day if reference_time >= 08:00,
    otherwise 8:00 AM of the previous day. Ensures a positive elapsed window.
    """
    eight_am_today = reference_time.replace(hour=8, minute=0, second=0, microsecond=0)
    if reference_time >= eight_am_today:
        return eight_am_today
    # Before 8 AM: use yesterday 8 AM
    return eight_am_today - timedelta(days=1)


def calculate_averages(monitor_instance, is_mini_report=False):
    """Calculate listing averages using an 8:00 AM baseline for the day."""
    now = datetime.now()
    baseline = _get_baseline_8am(now)

    if is_mini_report:
        elapsed_seconds = (now - baseline).total_seconds()
        if elapsed_seconds < 60:
            return "N/A (runtime too short)"
        intervals = elapsed_seconds / 1800  # number of 30-min intervals since 8 AM baseline
        if intervals > 0:
            avg_per_30min = monitor_instance.total_scanned_today / intervals
            return f"{avg_per_30min:.1f} listings per 30-minute interval"
        return "0.0 listings per 30-minute interval"
    else:
        # Full report average per hour since 8:00 AM baseline
        elapsed_seconds = (now - baseline).total_seconds()
        if elapsed_seconds < 60:
            return "N/A (runtime too short)"
        runtime_hours = elapsed_seconds / 3600
        if runtime_hours > 0:
            avg_per_hour = monitor_instance.total_scanned_today / runtime_hours
            return f"{avg_per_hour:.1f} listings per hour"
        return "0.0 listings per hour"

def calculate_estimated_savings(total_issues):
    """Calculate estimated savings at $10 per issue"""
    savings = total_issues * 10
    return f"${savings:,}"

def format_12_hour_time(hour_24):
    """Convert 24-hour format to 12-hour AM/PM format"""
    if hour_24 == 0:
        return "12:00 AM"
    elif hour_24 < 12:
        return f"{hour_24}:00 AM"
    elif hour_24 == 12:
        return "12:00 PM"
    else:
        return f"{hour_24 - 12}:00 PM"

def format_time_range_12_hour(start_hour, end_hour):
    """Format a time range in 12-hour AM/PM format"""
    start_time = format_12_hour_time(start_hour)
    end_time = format_12_hour_time(end_hour)
    return f"{start_time} - {end_time}"

def calculate_peak_times(monitor_instance):
    """Calculate peak activity times with error handling"""
    try:
        # Ensure attributes exist
        if not hasattr(monitor_instance, 'hourly_activity') or not monitor_instance.hourly_activity:
            return "No hourly data available", "No interval data available"
        
        # Find peak hour
        peak_hour = max(monitor_instance.hourly_activity.items(), key=lambda x: x[1])
        hour_int = int(peak_hour[0])
        next_hour = (hour_int + 1) % 24
        peak_hour_formatted = f"{format_time_range_12_hour(hour_int, next_hour)} ({peak_hour[1]} listings)"
        
        # Find peak 30-minute interval
        if hasattr(monitor_instance, 'thirty_min_activity') and monitor_instance.thirty_min_activity:
            peak_interval = max(monitor_instance.thirty_min_activity.items(), key=lambda x: x[1])
            time_parts = peak_interval[0].split(':')
            hour = int(time_parts[0])
            minute = int(time_parts[1])
            
            if minute == 0:
                # 30-minute interval from XX:00 to XX:30
                start_time = format_12_hour_time(hour)
                end_hour = hour
                end_minute = "30"
                if hour == 0:
                    end_time = "12:30 AM"
                elif hour < 12:
                    end_time = f"{hour}:30 AM"
                elif hour == 12:
                    end_time = "12:30 PM"
                else:
                    end_time = f"{hour - 12}:30 PM"
            else:  # minute == 30
                # 30-minute interval from XX:30 to (XX+1):00
                if hour == 0:
                    start_time = "12:30 AM"
                elif hour < 12:
                    start_time = f"{hour}:30 AM"
                elif hour == 12:
                    start_time = "12:30 PM"
                else:
                    start_time = f"{hour - 12}:30 PM"
                
                end_hour = (hour + 1) % 24
                end_time = format_12_hour_time(end_hour)
                
            peak_interval_formatted = f"{start_time} - {end_time} ({peak_interval[1]} listings)"
        else:
            peak_interval_formatted = "No interval data available"
        
        return peak_hour_formatted, peak_interval_formatted
        
    except Exception as e:
        return f"Error calculating peaks: {e}", "Error calculating peaks"
        
def generate_report_message(monitor_instance, is_full_report=False, report_title=""):
    """Generate report message with enhanced statistics (showing new listings only)"""
    current_time = datetime.now()
    
    runtime_delta = current_time - monitor_instance.script_start_time
    runtime_hours = runtime_delta.total_seconds() / 3600
    runtime_str = f"{runtime_hours:.1f} hours"
    
    if is_full_report:
        report_msg = f"📊 **Daily eBay Scanning Report** - {current_time.strftime('%Y-%m-%d')}\n\n"
    else:
        report_msg = f"📈 **{report_title}** - {current_time.strftime('%H:%M')}\n\n"
    
    # Core metrics - now showing both all and new listings
    total_from_categories = sum(monitor_instance.store_category_stats.values()) if hasattr(monitor_instance, 'store_category_stats') else 0
    total_count = max(monitor_instance.total_scanned_today, total_from_categories)  # NEW listings
    
    total_all_from_categories = sum(monitor_instance.all_store_category_stats.values()) if hasattr(monitor_instance, 'all_store_category_stats') else 0
    total_all_count = max(monitor_instance.total_all_listings_today, total_all_from_categories)  # ALL listings
    
    # Get item counts
    total_items = getattr(monitor_instance, 'total_items_today', 0)  # NEW items
    total_all_items = getattr(monitor_instance, 'all_total_items_today', 0)  # ALL items
    
    report_msg += f"⏱️ **Runtime:** {runtime_str}\n"
    
    # Show item counts only if they're significantly different from listing counts
    if total_all_items > total_all_count:
        report_msg += f"📊 **All Listings Scanned:** {total_all_count:,} listings ({total_all_items:,} total items)\n"
    else:
        report_msg += f"📊 **All Listings Scanned:** {total_all_count:,} listings\n"
    
    if total_items > total_count:
        report_msg += f"🆕 **New Listings Processed:** {total_count:,} listings ({total_items:,} total items) - SKU >= {monitor_instance.highest_recorded_sku}\n"
    else:
        report_msg += f"🆕 **New Listings Processed:** {total_count:,} listings (SKU >= {monitor_instance.highest_recorded_sku})\n"
    
    # Calculate and show percentage of new vs existing
    if total_all_count > 0:
        existing_count = total_all_count - total_count
        new_percentage = (total_count / total_all_count) * 100
        existing_percentage = (existing_count / total_all_count) * 100
        
    # NEW: Average processing rates (based on new listings for consistency)
    average_rate = calculate_averages(monitor_instance, is_mini_report=(not is_full_report))
    report_msg += f"📈 **New Listings Rate:** {average_rate}\n"
    
    # Issue reporting - now showing both all and new listing issues
    report_msg += f"⚠️ **Scraping Issues (All Listings):** {monitor_instance.total_scraping_issues:,} items (duplicates, empty SKUs)\n"

    report_msg += f"📊 **Total Issues (All Listings):** {monitor_instance.total_issues_found:,} items\n"

    # Calculate issue rates for new listings vs all listings
    if total_all_count > 0 and total_count > 0:
        all_issue_rate = (monitor_instance.total_issues_found / total_all_count) * 100
        new_issue_rate = (monitor_instance.new_total_issues_found / total_count) * 100
        report_msg += f"📈 **Issue Rates:** All listings: {all_issue_rate:.1f}%, New listings: {new_issue_rate:.1f}%\n"
    

    
    # Store category breakdown (ONLY for full reports) - Show both ALL and NEW listings
    if is_full_report and ((hasattr(monitor_instance, 'store_category_stats') and monitor_instance.store_category_stats) or 
                          (hasattr(monitor_instance, 'all_store_category_stats') and monitor_instance.all_store_category_stats)):
        

        
        # Show NEW listings breakdown
        if hasattr(monitor_instance, 'store_category_stats') and monitor_instance.store_category_stats:
            report_msg += "🆕 **Store Categories (New Listings Only):**\n"
            sorted_new_categories = sort_dict_by_value(monitor_instance.store_category_stats)
            
            # Convert to list and format each item
            formatted_new_items = []
            for category, count in sorted_new_categories.items():
                percentage = (count / total_from_categories * 100) if total_from_categories > 0 else 0
                formatted_new_items.append((f"{category}: {count:,} ({percentage:.1f}%)", len(category)))
            
            # Sort by length (shorter names first for left column)
            formatted_new_items.sort(key=lambda x: x[1])
            
            # Split into two columns - shorter names in left column
            mid_point = (len(formatted_new_items) + 1) // 2
            column1 = formatted_new_items[:mid_point]
            column2 = formatted_new_items[mid_point:]
            
            # Calculate maximum width for left column (including bullet point and spaces)
            max_left_width = max(len(f"   • {item[0]}") for item in column1) if column1 else 0
            padding_width = max_left_width + 5  # Add some extra spacing
            
            # Format two columns side by side
            max_rows = max(len(column1), len(column2))
            
            for i in range(max_rows):
                line = ""
                
                # Column 1 (shorter names)
                if i < len(column1):
                    line += f"   • {column1[i][0]}"
                
                # Pad to consistent width
                line = line.ljust(padding_width)
                
                # Column 2 (longer names)
                if i < len(column2):
                    line += f"• {column2[i][0]}"
                
                report_msg += line + "\n"
            
            report_msg += "\n"
    
    # NEW: Peak times analysis (ONLY for full reports) - 12-HOUR FORMAT
    if is_full_report:
        peak_hour, peak_interval = calculate_peak_times(monitor_instance)
        report_msg += "⏰ **Peak Activity Times:**\n"
        report_msg += f"   • Busiest Hour: {peak_hour}\n"
        report_msg += f"   • Busiest 30-min: {peak_interval}\n\n"
        
        # Hourly breakdown for full reports - 12-HOUR FORMAT
        if monitor_instance.hourly_activity:
            report_msg += "📊 **Hourly Activity Breakdown:**\n"
            for hour in sorted(monitor_instance.hourly_activity.keys()):
                count = monitor_instance.hourly_activity[hour]
                hour_int = int(hour)
                next_hour = (hour_int + 1) % 24
                hour_formatted = format_time_range_12_hour(hour_int, next_hour)
                report_msg += f"   • {hour_formatted}: {count:,} listings\n"
            report_msg += "\n"
    
    # Prefix sections (different styles for mini vs full reports) - Show both ALL and NEW
    

    
    # Then show NEW LISTINGS PROCESSED BY INITIAL  
    if monitor_instance.prefix_stats:
        report_msg += "🆕 **NEW LISTINGS PROCESSED BY INITIAL:**\n"
        sorted_prefixes = sort_dict_by_value(monitor_instance.prefix_stats)
        
        if is_full_report:
            # Show ALL prefixes for daily report with percentage comparison
            for prefix, scan_count in sorted_prefixes.items():
                if prefix == "UNKNOWN":
                    continue  # Skip unknown prefixes entirely
                
                # Get item count for this prefix
                new_items = monitor_instance.prefix_item_stats.get(prefix, 0) if hasattr(monitor_instance, 'prefix_item_stats') else 0
                
                # Show comparison with total count if available
                all_count = monitor_instance.all_prefix_stats.get(prefix, 0) if hasattr(monitor_instance, 'all_prefix_stats') else 0
                if all_count > 0 and all_count > scan_count:
                    percentage = (scan_count / all_count) * 100
                    display_prefix = format_initial_with_name(prefix)
                    if new_items > scan_count:  # Show items if different
                        report_msg += f"   • {display_prefix}: {scan_count:,} listings ({new_items:,} items)\n"
                    else:
                        report_msg += f"   • {display_prefix}: {scan_count:,} listings\n"
                else:
                    display_prefix = format_initial_with_name(prefix)
                    if new_items > scan_count:  # Show items if different
                        report_msg += f"   • {display_prefix}: {scan_count:,} listings ({new_items:,} items)\n"
                    else:
                        report_msg += f"   • {display_prefix}: {scan_count:,} listings\n"
        else:
            # Show only top 3 for mini reports
            count = 0
            max_items = 3
            for prefix, scan_count in sorted_prefixes.items():
                if prefix == "UNKNOWN":
                    continue  # Skip unknown prefixes entirely
                if count >= max_items:
                    break
                
                # Get item count for this prefix
                new_items = monitor_instance.prefix_item_stats.get(prefix, 0) if hasattr(monitor_instance, 'prefix_item_stats') else 0
                display_prefix = format_initial_with_name(prefix)
                if new_items > scan_count:  # Show items if different
                    report_msg += f"   • {display_prefix}: {scan_count:,} listings ({new_items:,} items)\n"
                else:
                    report_msg += f"   • {display_prefix}: {scan_count:,} listings\n"
                count += 1
        report_msg += "\n"
    

    
    # Show ALL listing issues first
    if hasattr(monitor_instance, 'prefix_issues') and monitor_instance.prefix_issues:
        report_msg += "⚠️ **ISSUES FOUND BY INITIAL (All Listings):**\n"
        sorted_all_issues = sort_dict_by_value(monitor_instance.prefix_issues)
        count = 0
        
        # Compute per-prefix item quantity for listings with issues (ALL)
        issue_items_by_prefix_all = compute_issue_items_by_prefix(monitor_instance, only_new=False)
        
        if is_full_report:
            # Show ALL prefixes for daily report with percentages
            for prefix, issue_count in sorted_all_issues.items():
                if prefix == "UNKNOWN":
                    continue  # Skip unknown prefixes entirely
                
                # Calculate percentage of issues vs all listings for this prefix
                all_listings_for_prefix = monitor_instance.all_prefix_stats.get(prefix, 0)
                items_with_issues = issue_items_by_prefix_all.get(prefix, 0)
                
                if all_listings_for_prefix > 0:
                    error_percentage = (issue_count / all_listings_for_prefix) * 100
                    report_msg += f"   • {format_initial_with_name(prefix)}: {issue_count:,} listings with issues ({items_with_issues:,} items) - {error_percentage:.1f}% error rate\n"
                else:
                    report_msg += f"   • {format_initial_with_name(prefix)}: {issue_count:,} listings with issues ({items_with_issues:,} items)\n"
        else:
            # Show only top 3 for mini reports
            max_items = 3
            for prefix, issue_count in sorted_all_issues.items():
                if prefix == "UNKNOWN":
                    continue  # Skip unknown prefixes entirely
                if count >= max_items:
                    break
                items_with_issues = issue_items_by_prefix_all.get(prefix, 0)
                report_msg += f"   • {format_initial_with_name(prefix)}: {issue_count:,} listings with issues ({items_with_issues:,} items)\n"
                count += 1
        report_msg += "\n"
    
    # Show NEW listing issues separately (combine scraping/duplicate + detailed issues)
    if ((hasattr(monitor_instance, 'new_prefix_issues') and monitor_instance.new_prefix_issues) or
        (hasattr(monitor_instance, 'new_detailed_prefix_issues') and monitor_instance.new_detailed_prefix_issues)):
        report_msg += "🆕 **ISSUES FOUND BY INITIAL (New Listings Only):**\n"
        
        # Build combined map of all new issues by prefix
        combined_new_issues = defaultdict(int)
        if hasattr(monitor_instance, 'new_prefix_issues'):
            for pfx, cnt in (monitor_instance.new_prefix_issues or {}).items():
                combined_new_issues[pfx] += cnt
        if hasattr(monitor_instance, 'new_detailed_prefix_issues'):
            for pfx, cnt in (monitor_instance.new_detailed_prefix_issues or {}).items():
                combined_new_issues[pfx] += cnt
        
        sorted_new_issues = sort_dict_by_value(combined_new_issues)
        count = 0
        
        # Compute per-prefix item quantity for listings with issues (NEW only)
        issue_items_by_prefix_new = compute_issue_items_by_prefix(monitor_instance, only_new=True)
        
        if is_full_report:
            # Show ALL prefixes for daily report with percentages and comparison
            for prefix, issue_count in sorted_new_issues.items():
                if prefix == "UNKNOWN":
                    continue  # Skip unknown prefixes entirely
                
                # Calculate percentage of issues vs new listings for this prefix
                new_listings_for_prefix = monitor_instance.prefix_stats.get(prefix, 0)
                # Compare against ALL listings' total issues (scraping + detailed) for the same prefix
                all_issues_for_prefix_total = (
                    (monitor_instance.prefix_issues.get(prefix, 0) if hasattr(monitor_instance, 'prefix_issues') else 0)
                    + (monitor_instance.detailed_prefix_issues.get(prefix, 0) if hasattr(monitor_instance, 'detailed_prefix_issues') else 0)
                )
                items_with_issues_new = issue_items_by_prefix_new.get(prefix, 0)
                
                if new_listings_for_prefix > 0:
                    error_percentage = (issue_count / new_listings_for_prefix) * 100
                    
                    # Show comparison with all issues if available
                    comparison = f" ({issue_count}/{all_issues_for_prefix_total} of total issues)" if all_issues_for_prefix_total > issue_count else ""
                    
                    report_msg += f"   • {format_initial_with_name(prefix)}: {issue_count:,} listings with issues ({items_with_issues_new:,} items) - {error_percentage:.1f}% error rate{comparison}\n"
                else:
                    report_msg += f"   • {format_initial_with_name(prefix)}: {issue_count:,} listings with issues ({items_with_issues_new:,} items)\n"
        else:
            # Show only top 3 for mini reports
            max_items = 3
            for prefix, issue_count in sorted_new_issues.items():
                if prefix == "UNKNOWN":
                    continue  # Skip unknown prefixes entirely
                if count >= max_items:
                    break
                items_with_issues_new = issue_items_by_prefix_new.get(prefix, 0)
                report_msg += f"   • {format_initial_with_name(prefix)}: {issue_count:,} listings with issues ({items_with_issues_new:,} items)\n"
                count += 1
        report_msg += "\n"
    
    # Footers
    if is_full_report:
        # No footer; scanning ends at 3:30 PM EST
        pass
    else:
        # No automatic next report display
        pass
    
    # 'UNKNOWN' prefixes were skipped above; no need to remove here
    
    return report_msg
    
def send_report_via_script(monitor_instance, message):
    """Send report using testmattermostmsg.py with positional message only (no extra flags)."""
    try:
        result = subprocess.run(['python', 'testmattermostmsg.py', message], capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            print_to_monitor(monitor_instance, f"✅ Report sent successfully at {datetime.now().strftime('%H:%M:%S')}")
            log_report_event(f"SENT report | length={len(message)} | code=0")
        else:
            stderr_msg = (result.stderr or '').strip()
            print_to_monitor(monitor_instance, f"⚠️ Error sending report (code {result.returncode}): {stderr_msg}")
            log_report_event(f"FAILED report | length={len(message)} | code={result.returncode} | stderr={stderr_msg}")
    except subprocess.TimeoutExpired:
        print_to_monitor(monitor_instance, "⚠️ Timeout sending report")
        log_report_event("FAILED report | exception=TimeoutExpired")
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ Error sending report: {e}")
        log_report_event(f"FAILED report | exception={str(e)}")

def _terminate_external_scanning_processes(monitor_instance):
    """Best-effort termination of known scanning/helper scripts before shutdown.

    Targets Windows environment primarily; uses psutil when available and falls
    back to taskkill/PowerShell filters when psutil is not installed.
    """
    try:
        import psutil  # type: ignore
        current_pid = os.getpid()
        targets = ['runit.py', 'process_description.py', 'zscrape']

        killed = 0
        for proc in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
            try:
                pid = proc.info.get('pid')
                if not pid or pid == current_pid:
                    continue
                name = (proc.info.get('name') or '').lower()
                cmdline_list = proc.info.get('cmdline') or []
                cmdline = ' '.join(cmdline_list).lower()

                # Match Python processes by command line and AHK by name
                is_python = ('python' in name) or ('python' in cmdline)
                is_ahk = 'autohotkey' in name
                if is_ahk:
                    proc.terminate()
                    killed += 1
                    continue
                if is_python and any(t in cmdline for t in targets):
                    proc.terminate()
                    killed += 1
            except Exception:
                # Best effort; ignore failures
                pass

        # Give processes a brief moment, then force kill any stubborn ones
        time.sleep(0.5)
        for proc in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
            try:
                if not proc.is_running():
                    continue
                name = (proc.info.get('name') or '').lower()
                cmdline = ' '.join(proc.info.get('cmdline') or []).lower()
                if 'autohotkey' in name or (('python' in name or 'python' in cmdline) and any(t in cmdline for t in ['runit.py', 'process_description.py', 'zscrape'])):
                    proc.kill()
            except Exception:
                pass

        if killed:
            print_to_monitor(monitor_instance, f"🧹 Terminated {killed} external scanning/helper processes")
    except ImportError:
        # Fallbacks for Windows without psutil
        try:
            # Kill AutoHotkey if running
            subprocess.run(['taskkill', '/F', '/IM', 'AutoHotkey.exe'], capture_output=True)
        except Exception:
            pass
        # Terminate python processes by script name via PowerShell CIM filter
        for script_name in ['runit.py', 'process_description.py']:
            try:
                ps_cmd = (
                    "Get-CimInstance Win32_Process | "
                    f"Where-Object {{$_.CommandLine -like '*{script_name}*'}} | "
                    "ForEach-Object {{$_.Terminate()}}"
                )
                subprocess.run(['powershell', '-NoProfile', '-Command', ps_cmd], capture_output=True)
            except Exception:
                pass

def _shutdown_application(monitor_instance):
    """Close GUI/console immediately after final report and cleanup."""
    try:
        print_to_monitor(monitor_instance, "👋 Shutting down Scan Monitor now")
    except Exception:
        pass

    # Stop the monitoring loop
    monitor_instance.running = False

    # Best-effort terminate external scripts
    try:
        _terminate_external_scanning_processes(monitor_instance)
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ Error during process termination: {e}")

    # Attempt to terminate the caller script/process (parent/chain) when safe
    try:
        import psutil  # type: ignore
        current = psutil.Process(os.getpid())
        targets = ['runit.py', 'process_description.py', 'zscrape']

        def _maybe_terminate(proc):
            try:
                name = (proc.name() or '').lower()
                cmdline_list = proc.cmdline() or []
                cmdline = ' '.join(cmdline_list).lower()
                is_python = 'python' in name or 'python' in cmdline
                is_pwsh = 'powershell' in name or 'pwsh' in name
                is_cmd = name in ('cmd.exe', 'command.com')

                # Only terminate if it appears to be a scripted caller, not an IDE/terminal shell.
                scripted = any(t in cmdline for t in targets) or 'scan_monitor.py' in cmdline
                if (is_python or is_pwsh) and scripted:
                    proc.terminate()
                    return True
                return False
            except Exception:
                return False

        killed_any = False
        parent = current.parent()
        if parent:
            killed_any = _maybe_terminate(parent)
            # Try grandparent if parent was terminated or if parent was a launcher
            try:
                grandparent = parent.parent()
                if grandparent and not killed_any:
                    killed_any = _maybe_terminate(grandparent)
            except Exception:
                pass

        if killed_any:
            print_to_monitor(monitor_instance, "🧹 Terminated caller script/process")
    except ImportError:
        # psutil not installed; skip explicit parent termination
        pass

    # Close GUI if present; otherwise exit process
    gui = getattr(monitor_instance, 'gui', None)
    if gui and getattr(gui, 'root', None):
        try:
            # Schedule GUI close on the main thread
            gui.root.after(0, gui.root.destroy)
        except Exception:
            pass
    else:
        # Console mode: hard exit to ensure no further scanning occurs
        try:
            os._exit(0)
        except Exception:
            pass

def generate_weekly_report_message(monitor_instance):
    """Aggregate the week's full daily reports and build a weekly summary message.

    This aggregates from per-day SQLite databases created in `daily_databases/`.
    We only include 'full' reports to avoid double counting minis.
    """
    try:
        current_time = datetime.now()
        # Determine Monday..Sunday of the current ISO week
        monday = current_time - timedelta(days=current_time.weekday())
        sunday = monday + timedelta(days=6)

        week_label = f"{monday.strftime('%b %d')}–{sunday.strftime('%b %d')} {monday.strftime('%Y')}"

        # Collect summary_ids of full reports across the week and aggregate metrics
        daily_db_dir = os.path.join(os.path.dirname(monitor_instance.daily_db.db_path))

        total_all_listings_week = 0
        total_new_listings_week = 0
        total_all_issues_week = 0
        total_new_issues_week = 0
        estimated_savings_week = 0

        full_summary_ids = []

        for i in range(7):
            day = monday + timedelta(days=i)
            date_str = day.strftime('%Y%m%d')
            db_path = os.path.join(daily_db_dir, f"daily_stats_{date_str}.db")
            if not os.path.exists(db_path):
                continue

            try:
                conn = sqlite3.connect(db_path)
                cur = conn.cursor()

                # Get the latest full report for that day (should be at most one)
                cur.execute(
                    "SELECT id, total_all_listings, total_processed, total_issues, new_total_issues, estimated_savings "
                    "FROM daily_summary WHERE report_type='full' ORDER BY timestamp DESC LIMIT 1"
                )
                row = cur.fetchone()
                if row:
                    summary_id, total_all, total_new, total_issues, new_issues, est_savings = row
                    full_summary_ids.append((db_path, summary_id))
                    total_all_listings_week += int(total_all or 0)
                    total_new_listings_week += int(total_new or 0)
                    total_all_issues_week += int(total_issues or 0)
                    total_new_issues_week += int(new_issues or 0)
                    estimated_savings_week += int(est_savings or 0)
            except Exception as e:
                print_to_monitor(monitor_instance, f"⚠️ Error reading daily DB {db_path}: {e}")
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        # Aggregate prefix issues across the week from the collected summary_ids
        prefix_to_all_issues = {}
        prefix_to_new_issues = {}
        for db_path, sid in full_summary_ids:
            try:
                conn = sqlite3.connect(db_path)
                cur = conn.cursor()
                cur.execute(
                    "SELECT prefix, issues_found, new_issues_found FROM prefix_stats WHERE summary_id=?",
                    (sid,)
                )
                for prefix, issues_found, new_issues_found in cur.fetchall():
                    if not prefix or prefix == 'UNKNOWN':
                        continue
                    prefix_to_all_issues[prefix] = prefix_to_all_issues.get(prefix, 0) + int(issues_found or 0)
                    prefix_to_new_issues[prefix] = prefix_to_new_issues.get(prefix, 0) + int(new_issues_found or 0)
            except Exception as e:
                print_to_monitor(monitor_instance, f"⚠️ Error aggregating prefixes from {db_path}: {e}")
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        # Build the message
        msg_lines = []
        msg_lines.append(f"📅 **Weekly eBay Scanning Report** — {week_label}")
        msg_lines.append("")

        # Totals (remove savings)
        msg_lines.append(f"📊 **All Listings Scanned:** {total_all_listings_week:,}")
        msg_lines.append(f"🆕 **New Listings Scanned:** {total_new_listings_week:,}")
        msg_lines.append(f"❗ **All Issues Found:** {total_all_issues_week:,}")
        msg_lines.append(f"🆕❗ **New Listing Issues:** {total_new_issues_week:,}")
        msg_lines.append("")

        # Top Initials by Listings (All Listings)
        top_listings = {}
        try:
            # Aggregate all listings by prefix across the week
            for db_path, sid in full_summary_ids:
                conn = sqlite3.connect(db_path)
                cur = conn.cursor()
                cur.execute(
                    "SELECT prefix, all_listings FROM prefix_stats WHERE summary_id=?",
                    (sid,)
                )
                for prefix, all_listings in cur.fetchall():
                    if not prefix or prefix == 'UNKNOWN':
                        continue
                    top_listings[prefix] = top_listings.get(prefix, 0) + int(all_listings or 0)
                conn.close()
        except Exception:
            pass
        
        if top_listings:
            sorted_listings = sorted(top_listings.items(), key=lambda kv: kv[1], reverse=True)
            msg_lines.append("🏷️ **Top Initials by Listings:**")
            for prefix, count in sorted_listings[:5]:
                display_prefix = format_initial_with_name(prefix)
                msg_lines.append(f"   • {display_prefix}: {count:,} listings")
            msg_lines.append("")

        # Top Initials by Issues (All Listings)
        if prefix_to_all_issues:
            sorted_all = sorted(prefix_to_all_issues.items(), key=lambda kv: kv[1], reverse=True)
            msg_lines.append("❗ **Top Initials by Issues:**")
            for prefix, count in sorted_all[:5]:
                display_prefix = format_initial_with_name(prefix)
                msg_lines.append(f"   • {display_prefix}: {count:,} issues")
            msg_lines.append("")

        msg_lines.append("🔔 Next weekly report: Friday 3:30 PM EST")

        return "\n".join(msg_lines)
    except Exception as e:
        return f"⚠️ Error generating weekly report: {e}"

def backup_description_screenshots(monitor_instance, current_date):
    """Create a comprehensive backup of description_screenshots folder with optimized compression for images"""
    try:
        import shutil
        import os
        from pathlib import Path
        
        try:
            from PIL import Image
            HAS_PIL = True
        except ImportError:
            print_to_monitor(monitor_instance, "⚠️ PIL/Pillow not available, images will be copied without compression")
            HAS_PIL = False
        
        screenshot_dir = "description_screenshots"
        if not os.path.exists(screenshot_dir):
            print_to_monitor(monitor_instance, f"📁 {screenshot_dir} does not exist, skipping backup")
            return
        
        # Create backups directory if it doesn't exist
        backups_dir = Path(BACKUPS_DIR)
        backups_dir.mkdir(exist_ok=True)
        
        # Use the dedicated screenshots subfolder within backups
        screenshots_backup_dir = backups_dir / "screenshots"
        screenshots_backup_dir.mkdir(exist_ok=True)
        
        # Create dated backup folder within the screenshots subfolder
        backup_name = f"screenshots_backup_{current_date}"
        backup_path = screenshots_backup_dir / backup_name
        
        # If backup already exists, add timestamp
        if backup_path.exists():
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_name = f"screenshots_backup_{timestamp}"
            backup_path = screenshots_backup_dir / backup_name
        
        # Create the backup directory
        backup_path.mkdir(exist_ok=True)
        
        # Count files before backup
        file_count = len([f for f in os.listdir(screenshot_dir) 
                         if os.path.isfile(os.path.join(screenshot_dir, f)) and 
                         (f.lower().endswith('.png') or f.lower().endswith('.jpg') or 
                          f.lower().endswith('.jpeg') or f.lower().endswith('.bmp'))])
        
        if file_count > 0:
            print_to_monitor(monitor_instance, f"🖼️ Compressing and backing up {file_count:,} screenshots from {screenshot_dir}")
            
            # Counter for successful compressions
            successful_count = 0
            orig_size_total = 0
            compressed_size_total = 0
            
            # Process each image file
            for filename in os.listdir(screenshot_dir):
                file_path = os.path.join(screenshot_dir, filename)
                
                # Skip directories and non-image files
                if not os.path.isfile(file_path) or not filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp')):
                    continue
                
                try:
                    # Get original file size
                    orig_size = os.path.getsize(file_path)
                    orig_size_total += orig_size
                    
                    if HAS_PIL:
                        # Open the image with Pillow
                        with Image.open(file_path) as img:
                            # Determine if image has a white background (common in description screenshots)
                            # by checking pixel values in corners
                            width, height = img.size
                            is_white_bg = False
                            
                            # Convert to RGB if needed for analysis
                            if img.mode != "RGB":
                                analysis_img = img.convert("RGB")
                            else:
                                analysis_img = img
                            
                            # Check corners for white (threshold > 240)
                            corners = [
                                analysis_img.getpixel((0, 0)),
                                analysis_img.getpixel((width-1, 0)),
                                analysis_img.getpixel((0, height-1)),
                                analysis_img.getpixel((width-1, height-1))
                            ]
                            
                            # If most corners are white (RGB values > 240)
                            if sum(1 for r, g, b in corners if r > 240 and g > 240 and b > 240) >= 3:
                                is_white_bg = True
                            
                            # Output path for the compressed image
                            output_path = backup_path / filename
                            
                            # Compression strategy based on image format and background
                            if img.format == "PNG":
                                # For PNGs, use higher compression with white backgrounds
                                if is_white_bg:
                                    # PNG with white background - use quantize to reduce colors
                                    img = img.quantize(colors=64)
                                    img.save(output_path, optimize=True)
                                else:
                                    # Regular PNG optimization
                                    img.save(output_path, optimize=True, compress_level=9)
                            else:
                                # For JPEG and other formats
                                if is_white_bg:
                                    # JPEG with white background - higher compression is fine
                                    img.save(output_path, quality=60, optimize=True)
                                else:
                                    # Regular JPEG optimization with decent quality
                                    img.save(output_path, quality=75, optimize=True)
                    else:
                        # No PIL available, just copy the file
                        output_path = backup_path / filename
                        shutil.copy2(file_path, output_path)
                    
                    # Get compressed file size
                    compressed_size = os.path.getsize(output_path)
                    compressed_size_total += compressed_size
                    
                    successful_count += 1
                
                except Exception as e:
                    print_to_monitor(monitor_instance, f"⚠️ Error compressing {filename}: {e}")
                    # Fallback to direct copy if compression fails
                    try:
                        shutil.copy2(file_path, backup_path / filename)
                        print_to_monitor(monitor_instance, f"ℹ️ Fallback: Copied {filename} without compression")
                    except Exception as copy_err:
                        print_to_monitor(monitor_instance, f"❌ Failed to copy {filename}: {copy_err}")
            
            # Calculate overall compression statistics
            if successful_count > 0:
                compression_ratio = (1 - compressed_size_total / orig_size_total) * 100 if orig_size_total > 0 else 0
                print_to_monitor(monitor_instance, f"✅ Successfully backed up {successful_count:,} screenshots to {backup_path}")
                print_to_monitor(monitor_instance, f"📊 Compression: {orig_size_total:,} bytes → {compressed_size_total:,} bytes ({compression_ratio:.1f}% reduction)")
                
                # Now clear the original folder after successful backup
                for filename in os.listdir(screenshot_dir):
                    file_path = os.path.join(screenshot_dir, filename)
                    try:
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                        elif os.path.isdir(file_path):
                            shutil.rmtree(file_path)
                    except Exception as e:
                        print_to_monitor(monitor_instance, f"⚠️ Error removing {file_path}: {e}")
                
                print_to_monitor(monitor_instance, f"🗑️ Cleared {file_count:,} files from {screenshot_dir}")
            else:
                print_to_monitor(monitor_instance, f"⚠️ No images were successfully compressed")
        else:
            print_to_monitor(monitor_instance, f"📁 {screenshot_dir} has no image files, skipping backup")
            
    except Exception as e:
        import traceback
        print_to_monitor(monitor_instance, f"⚠️ Error backing up {screenshot_dir}: {e}")
        print_to_monitor(monitor_instance, f"Stack trace: {traceback.format_exc()}")

def backup_item_contents(monitor_instance, current_date):
    """Create a comprehensive backup of item_contents folder into dedicated subfolder within backups"""
    try:
        import shutil
        from pathlib import Path
        
        item_contents_dir = "item_contents"
        if not os.path.exists(item_contents_dir):
            print_to_monitor(monitor_instance, f"📁 {item_contents_dir} does not exist, skipping backup")
            return
        
        # Create backups directory if it doesn't exist
        backups_dir = Path(BACKUPS_DIR)
        backups_dir.mkdir(exist_ok=True)
        
        # Use the dedicated itemcontents subfolder within backups
        item_contents_backups_dir = backups_dir / "itemcontents"
        item_contents_backups_dir.mkdir(exist_ok=True)
        
        # Create dated backup folder within the item_contents subfolder
        backup_name = f"item_contents_backup_{current_date}"
        backup_path = item_contents_backups_dir / backup_name
        
        # If backup already exists, add timestamp
        if backup_path.exists():
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_name = f"item_contents_backup_{timestamp}"
            backup_path = item_contents_backups_dir / backup_name
        
        # Count files before backup
        file_count = len([f for f in os.listdir(item_contents_dir) 
                         if os.path.isfile(os.path.join(item_contents_dir, f))])
        
        if file_count > 0:
            print_to_monitor(monitor_instance, f"💾 Creating backup of {file_count:,} files from {item_contents_dir}")
            
            # Copy the entire folder
            shutil.copytree(item_contents_dir, backup_path)
            
            # Verify backup
            backup_file_count = len([f for f in os.listdir(backup_path) 
                                   if os.path.isfile(os.path.join(backup_path, f))])
            
            if backup_file_count == file_count:
                print_to_monitor(monitor_instance, f"✅ Successfully backed up {backup_file_count:,} files to {backup_path}")
                
                # Now clear the original folder
                for filename in os.listdir(item_contents_dir):
                    file_path = os.path.join(item_contents_dir, filename)
                    try:
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                        elif os.path.isdir(file_path):
                            shutil.rmtree(file_path)
                    except Exception as e:
                        print_to_monitor(monitor_instance, f"⚠️ Error removing {file_path}: {e}")
                
                print_to_monitor(monitor_instance, f"🗑️ Cleared {file_count:,} files from {item_contents_dir}")
            else:
                print_to_monitor(monitor_instance, f"⚠️ Backup verification failed: expected {file_count}, got {backup_file_count}")
        else:
            print_to_monitor(monitor_instance, f"📁 {item_contents_dir} is empty, skipping backup")
            
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ Error backing up {item_contents_dir}: {e}")
        import traceback
        print_to_monitor(monitor_instance, f"Stack trace: {traceback.format_exc()}")

def rotate_daily_folders(monitor_instance):
    """Rotate folders daily to start fresh with log compression and cleanup"""
    try:
        import gzip
        import sqlite3
        import shutil
        import tarfile
        from pathlib import Path
        
        current_date = datetime.now().strftime('%Y%m%d')
        
        # Close current database and create new one for next day
        if hasattr(monitor_instance, 'daily_db'):
            monitor_instance.daily_db.close()
            next_day = (datetime.now() + timedelta(days=1)).strftime('%Y%m%d')
            monitor_instance.daily_db = DailyStatsDB(next_day)
            print_to_monitor(monitor_instance, f"💾 Rotated to new database: {monitor_instance.daily_db.db_path}")
        
        # Folders to rotate (but handle item_contents specially)
        folders_to_rotate = ['eBayListingData']
        
        # Log folders to compress
        log_folders_to_compress = [
            'compare_error_logs',
            os.path.join(MONITORING_LOGS_DIR, '..', 'processing', 'compare_logs'),
            os.path.join(MONITORING_LOGS_DIR, '..', 'processing', 'process_logs'),
            os.path.join(MONITORING_LOGS_DIR, '..', 'processing', 'pull_logs')
        ]
        
        # Folders to clear contents
        folders_to_clear = ['specs_data', 'table_data']
        
        # Ensure main backups directory exists
        backups_dir = Path(BACKUPS_DIR)
        backups_dir.mkdir(exist_ok=True)
        
        # Create organized subdirectories within backups folder
        logs_backup_dir = backups_dir / "logs"
        logs_backup_dir.mkdir(exist_ok=True)
        
        ebay_data_backup_dir = backups_dir / "ebaylistingdata"  # Changed to match the requested folder name
        ebay_data_backup_dir.mkdir(exist_ok=True)
        
        item_contents_backup_dir = backups_dir / "itemcontents"
        item_contents_backup_dir.mkdir(exist_ok=True)
        
        # New directory for description screenshots backups
        screenshots_backup_dir = backups_dir / "screenshots"
        screenshots_backup_dir.mkdir(exist_ok=True)
        
        sqlite_backup_dir = backups_dir / "sqlite"
        sqlite_backup_dir.mkdir(exist_ok=True)
        
        # Step 1: Handle item_contents specially - backup instead of archive
        backup_item_contents(monitor_instance, current_date)
        
        # Step 1b: Description screenshots backup removed (feature disabled)
        
        # Step 2: Create SQLite databases for important data before compression
        # Create the working archive directory INSIDE the backups/sqlite folder to avoid CWD/device issues
        sqlite_archive_dir = sqlite_backup_dir / f"sqlite_archives_{current_date}"
        sqlite_archive_dir.mkdir(parents=True, exist_ok=True)
        
        print_to_monitor(monitor_instance, f"📁 Creating SQLite archives in {sqlite_archive_dir}")
        
        # Archive process_logs to SQLite
        process_logs_path = os.path.join(MONITORING_LOGS_DIR, '..', 'processing', 'process_logs')
        if os.path.exists(process_logs_path):
            try:
                sqlite_path = sqlite_archive_dir / "process_logs.db"
                conn = sqlite3.connect(sqlite_path)
                cursor = conn.cursor()
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS process_logs (
                        item_number TEXT PRIMARY KEY,
                        filename TEXT,
                        log_content TEXT,
                        file_size INTEGER,
                        timestamp TEXT
                    )
                ''')
                
                for filename in os.listdir(process_logs_path):
                    if filename.startswith('process_log_') and filename.endswith('.txt'):
                        filepath = os.path.join(process_logs_path, filename)
                        try:
                            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                                content = f.read()
                            
                            item_number = filename.replace('process_log_', '').replace('.txt', '')
                            file_size = os.path.getsize(filepath)
                            file_timestamp = datetime.fromtimestamp(os.path.getmtime(filepath)).isoformat()
                            
                            cursor.execute('''
                                INSERT OR REPLACE INTO process_logs 
                                (item_number, filename, log_content, file_size, timestamp)
                                VALUES (?, ?, ?, ?, ?)
                            ''', (item_number, filename, content, file_size, file_timestamp))
                            
                        except Exception as e:
                            print_to_monitor(monitor_instance, f"⚠️ Error processing log file {filename}: {e}")
                
                conn.commit()
                conn.close()
                print_to_monitor(monitor_instance, f"✅ Archived process_logs to SQLite: {sqlite_path}")
                
            except Exception as e:
                print_to_monitor(monitor_instance, f"⚠️ Error creating SQLite archive for process_logs: {e}")
        
        # Move step no longer needed since we create inside sqlite_backup_dir; retain guard if location differs
        if sqlite_archive_dir.exists() and sqlite_archive_dir.parent != sqlite_backup_dir:
            try:
                target_path = sqlite_backup_dir / sqlite_archive_dir.name
                if target_path.exists():
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    target_path = sqlite_backup_dir / f"sqlite_archives_{timestamp}"
                shutil.move(str(sqlite_archive_dir), str(target_path))
                print_to_monitor(monitor_instance, f"📁 Moved SQLite archives to {target_path}")
                sqlite_archive_dir = target_path
            except Exception as e:
                print_to_monitor(monitor_instance, f"⚠️ Error moving SQLite archives: {e}")
        
        # Step 3: Compress log folders with maximum gzip compression
        for folder in log_folders_to_compress:
            if os.path.exists(folder):
                try:
                    # Get the folder name without path
                    folder_name = os.path.basename(folder)
                    archive_name = os.path.join(logs_backup_dir, f"{folder_name}_{current_date}.tar.gz")
                    
                    print_to_monitor(monitor_instance, f"🗜️ Compressing {folder} to {archive_name}...")
                    
                    # Create tar.gz with maximum compression
                    with tarfile.open(archive_name, "w:gz", compresslevel=9) as tar:
                        tar.add(folder, arcname=folder_name)
                    
                    # Get compression stats
                    original_size = sum(os.path.getsize(os.path.join(dirpath, filename))
                                      for dirpath, dirnames, filenames in os.walk(folder)
                                      for filename in filenames)
                    compressed_size = os.path.getsize(archive_name)
                    compression_ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0
                    
                    print_to_monitor(monitor_instance, f"✅ Compressed {folder}: {original_size:,} bytes → {compressed_size:,} bytes ({compression_ratio:.1f}% reduction)")
                    
                    # Remove original folder after successful compression
                    shutil.rmtree(folder)
                    print_to_monitor(monitor_instance, f"🗑️ Removed original {folder} folder")
                    
                    # Create new empty folder
                    os.makedirs(folder, exist_ok=True)
                    print_to_monitor(monitor_instance, f"📁 Created fresh {folder} folder")
                    
                except Exception as e:
                    print_to_monitor(monitor_instance, f"⚠️ Error compressing {folder}: {e}")
        
        # Step 4: Standard folder rotation for main folders (excluding item_contents)
        for folder in folders_to_rotate:
            if os.path.exists(folder):
                try:
                    # Create archive folder name with timestamp
                    archive_folder = os.path.join(ebay_data_backup_dir, f"{folder}_archive_{current_date}")
                    
                    # If archive folder already exists, add time
                    if os.path.exists(archive_folder):
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        archive_folder = os.path.join(ebay_data_backup_dir, f"{folder}_archive_{timestamp}")
                    
                    # Only rename if folder is not empty
                    if os.listdir(folder):
                        # Copy the folder to archive location (instead of just renaming)
                        shutil.copytree(folder, archive_folder)
                        print_to_monitor(monitor_instance, f"📁 Archived {folder} to {archive_folder}")
                        
                        # Clear the original folder
                        for item in os.listdir(folder):
                            item_path = os.path.join(folder, item)
                            if os.path.isfile(item_path):
                                os.remove(item_path)
                            elif os.path.isdir(item_path):
                                shutil.rmtree(item_path)
                    else:
                        print_to_monitor(monitor_instance, f"📁 {folder} is empty, skipping archive")
                    
                    # Create new empty folder if it doesn't exist
                    os.makedirs(folder, exist_ok=True)
                    print_to_monitor(monitor_instance, f"📁 Created fresh {folder} folder")
                    
                except Exception as e:
                    print_to_monitor(monitor_instance, f"⚠️ Error rotating {folder}: {e}")
        
        # Step 5: Clear contents of specified folders
        for folder in folders_to_clear:
            if os.path.exists(folder):
                try:
                    # Count files before clearing
                    file_count = len([f for f in os.listdir(folder) 
                                    if os.path.isfile(os.path.join(folder, f))])
                    
                    if file_count > 0:
                        # Remove all files in the folder
                        for filename in os.listdir(folder):
                            file_path = os.path.join(folder, filename)
                            if os.path.isfile(file_path):
                                os.remove(file_path)
                            elif os.path.isdir(file_path):
                                shutil.rmtree(file_path)
                        
                        print_to_monitor(monitor_instance, f"🗑️ Cleared {file_count} files from {folder}")
                    else:
                        print_to_monitor(monitor_instance, f"📁 {folder} was already empty")
                        
                except Exception as e:
                    print_to_monitor(monitor_instance, f"⚠️ Error clearing {folder}: {e}")
            else:
                print_to_monitor(monitor_instance, f"📁 {folder} does not exist, skipping")
        
        # Step 6: Compress the SQLite archive directory (safe path handling)
        if sqlite_archive_dir.exists() and any(sqlite_archive_dir.iterdir()):
            try:
                sqlite_archive_name = sqlite_backup_dir / f"sqlite_archives_{current_date}.tar.gz"
                print_to_monitor(monitor_instance, f"🗜️ Compressing SQLite archives to {sqlite_archive_name}...")

                with tarfile.open(str(sqlite_archive_name), "w:gz", compresslevel=9) as tar:
                    tar.add(str(sqlite_archive_dir), arcname=f"sqlite_archives_{current_date}")

                # Remove the SQLite directory after compression
                shutil.rmtree(str(sqlite_archive_dir))
                print_to_monitor(monitor_instance, f"✅ SQLite archives compressed and cleaned up")

            except Exception as e:
                print_to_monitor(monitor_instance, f"⚠️ Error compressing SQLite archives: {e}")
        
        # Step 7: Print summary
        print_to_monitor(monitor_instance, f"🎉 Daily rotation completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print_to_monitor(monitor_instance, f"📂 All backups organized into subfolders within {BACKUPS_DIR}:")
        print_to_monitor(monitor_instance, f"   • {logs_backup_dir}: Log files")
        print_to_monitor(monitor_instance, f"   • {ebay_data_backup_dir}: eBay data archives")
        print_to_monitor(monitor_instance, f"   • {sqlite_backup_dir}: SQLite database archives")
        print_to_monitor(monitor_instance, f"   • {backups_dir / 'item_contents'}: Item contents backups")
        
        # Step 8: Reset monitor instance tracking data
        monitor_instance.seen_items.clear()
        monitor_instance.detailed_issues.clear()
        monitor_instance.total_detailed_issues = 0
        monitor_instance.file_mod_times.clear()  # Reset file change tracking
        monitor_instance.last_mini_report_total = 0  # Reset for new day
        
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ Error in daily folder rotation: {e}")
        import traceback
        print_to_monitor(monitor_instance, f"Stack trace: {traceback.format_exc()}")

def manual_rotation(monitor_instance):
    """Manually trigger folder rotation (for testing or immediate reset)"""
    print_to_monitor(monitor_instance, "🔄 Manual folder rotation triggered...")
    
    # Clear file change tracking since we're doing a full reset
    monitor_instance.file_mod_times.clear()
    
    rotate_daily_folders(monitor_instance)
    print_to_monitor(monitor_instance, "✅ Manual rotation complete - all tracking data reset")

def manual_send_report(monitor_instance):
    """Manually send a full day report"""
    try:
        print_to_monitor(monitor_instance, "📊 Manual report requested...")
        
        # Force update statistics for manual reports to ensure fresh data
        update_statistics(monitor_instance, force_update=True)
        
        # Generate full report
        report_message = generate_report_message(monitor_instance, is_full_report=True)
        
        # Print the report to the GUI for testing/preview
        print_to_monitor(monitor_instance, "=" * 60)
        print_to_monitor(monitor_instance, "📋 REPORT PREVIEW (what will be sent to Mattermost):")
        print_to_monitor(monitor_instance, "=" * 60)
        
        # Print each line of the report
        for line in report_message.split('\n'):
            if line.strip():  # Only print non-empty lines
                print_to_monitor(monitor_instance, line)
            else:
                print_to_monitor(monitor_instance, "")  # Print empty lines to maintain formatting
        
        print_to_monitor(monitor_instance, "=" * 60)
        print_to_monitor(monitor_instance, "🚀 Sending report to Mattermost...")
        print_to_monitor(monitor_instance, "=" * 60)
        
        # Send the report
        send_report_via_script(monitor_instance, report_message)
        
        # Persist highest SKU AFTER the report is generated/sent
        monitor_instance.commit_highest_sku()
        
        current_time = datetime.now().strftime('%H:%M:%S')
        print_to_monitor(monitor_instance, f"✅ Manual report sent successfully at {current_time}")
        
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ Error sending manual report: {e}")

def monitor_loop(monitor_instance):
    """Updated monitoring loop with new mini report timing and database saving"""
    # --- Report scheduling flags ---
    daily_rotation_done_today = False
    current_day = datetime.now().date()
    # Track week (Monday date) to know when to reset weekly flag
    current_week_monday = current_day - timedelta(days=current_day.weekday())
    weekly_report_sent_this_week = False
    last_status_time = datetime.now()
    # Track which of the four scheduled reports have been sent today
    scheduled_reports_sent_today = {
        '10:30': False,  # Mini #1
        '12:00': False,  # Mini #2
        '14:30': False,  # Mini #3
        '15:30': False   # Daily
    }
    
    print_to_monitor(monitor_instance, "🔄 Starting monitoring loop...")
    print_to_monitor(monitor_instance, "📝 Will send mini reports at 10:30 AM, 12:00 PM, and 2:30 PM")
    print_to_monitor(monitor_instance, "📝 Will send daily report at 3:30 PM EST")
    print_to_monitor(monitor_instance, "📅 Will send weekly report on Fridays at 3:30 PM EST")
    print_to_monitor(monitor_instance, "🔍 Now monitoring both scraping AND data quality issues")
    print_to_monitor(monitor_instance, "🏪 Now tracking store category distribution")
    print_to_monitor(monitor_instance, "🆕 Only counting NEW listings (SKU >= recorded highest, < 20000)")
    print_to_monitor(monitor_instance, "📁 Will backup item_contents and rotate other folders daily at 3:30 PM EST")
    print_to_monitor(monitor_instance, "⚡ Using smart file change detection + 30s cooldown for efficiency")
    print_to_monitor(monitor_instance, "💾 Statistics will be saved to SQLite database for each report")
    try:
        print_to_monitor(monitor_instance, f"🛡️ Working-hours window configured: {getattr(monitor_instance, 'watchdog_work_start', WATCHDOG_DEFAULT_WORK_START)}–{getattr(monitor_instance, 'watchdog_work_end', WATCHDOG_DEFAULT_WORK_END)}")
    except Exception:
        pass
    
    while monitor_instance.running:
        try:
            now = datetime.now()

            # Ensure zscrape only runs during working hours; outside hours, ensure it's stopped
            try:
                start_h, start_m = map(int, str(getattr(monitor_instance, 'watchdog_work_start', WATCHDOG_DEFAULT_WORK_START)).split(':'))
                end_h, end_m = map(int, str(getattr(monitor_instance, 'watchdog_work_end', WATCHDOG_DEFAULT_WORK_END)).split(':'))
                within_work_hours = ((now.hour > start_h) or (now.hour == start_h and now.minute >= start_m)) and ((now.hour < end_h) or (now.hour == end_h and now.minute < end_m))
            except Exception:
                within_work_hours = True

            if within_work_hours:
                # Working hours: ensure zscrape is launched regardless of detection
                try:
                    script_path = getattr(monitor_instance, 'zscrape_script_path', ZSCRAPE_DEFAULT_AHK)
                    if not os.path.isabs(script_path):
                        script_path = os.path.join(BASE_DIR, script_path)
                    if not os.path.exists(script_path):
                        script_path = ZSCRAPE_DEFAULT_AHK
                    _launch_zscrape(script_path, pass_no_monitor=True)
                except Exception:
                    pass
            else:
                # Outside hours: terminate all AHK and known scanning helpers, unconditionally (throttled)
                last_cleanup = getattr(monitor_instance, '_last_outside_cleanup', datetime.min)
                if (now - last_cleanup).total_seconds() >= getattr(monitor_instance, 'outside_cleanup_cooldown', 300):
                    try:
                        print_to_monitor(monitor_instance, f"⏸️ Outside working hours ({getattr(monitor_instance, 'watchdog_work_start', WATCHDOG_DEFAULT_WORK_START)}–{getattr(monitor_instance, 'watchdog_work_end', WATCHDOG_DEFAULT_WORK_END)}) — halting zscrape/AHK if running")
                    except Exception:
                        print_to_monitor(monitor_instance, "⏸️ Outside working hours — halting zscrape/AHK if running")
                    try:
                        _terminate_external_scanning_processes(monitor_instance)
                    except Exception:
                        pass
                    monitor_instance._last_outside_cleanup = now

            # Manage security watchdog helper lifecycle only when explicitly enabled via CLI
            if getattr(monitor_instance, 'enable_watchdog', False):
                try:
                    if monitor_instance.watchdog_process is None or (monitor_instance.watchdog_process.poll() is not None):
                        # Start or restart watchdog
                        try:
                            exe = sys.executable or 'python'
                            wd_path = os.path.join(BASE_DIR, 'tools', 'security', 'watchdog.py')
                            args = [
                                exe, wd_path,
                                '--log', os.path.join(LOGS_DIR, 'watchdog', 'security_watchdog.log'),
                                '--sentinel', os.path.join(LOGS_DIR, 'last_run_complete.txt'),
                                '--control', str(monitor_instance.watchdog_control_file),
                                '--status', str(monitor_instance.watchdog_status_file),
                            ]
                            # Critical paths: repo roots (current and sibling newsuite if present)
                            for p in getattr(monitor_instance, 'watchdog_paths', [BASE_DIR]):
                                args.extend(['--critical-path', p])
                            monitor_instance.watchdog_process = subprocess.Popen(args, creationflags=0)
                            print_to_monitor(monitor_instance, '🛡️ Watchdog started')
                        except Exception as e:
                            print_to_monitor(monitor_instance, f"⚠️ Failed to start watchdog: {e}")
                except Exception:
                    pass
            
            # 2-minute AHK stall/watchdog: if heartbeat stale or restart signal present, kill AHK and relaunch zscrape
            try:
                need_restart = False
                # Check explicit restart request file (from AHK)
                if os.path.exists(AHK_RESTART_SIGNAL_FILE):
                    need_restart = True
                    try:
                        os.remove(AHK_RESTART_SIGNAL_FILE)
                    except Exception:
                        pass
                else:
                    # Check heartbeat age
                    if os.path.exists(AHK_HEARTBEAT_FILE):
                        try:
                            mtime = datetime.fromtimestamp(os.path.getmtime(AHK_HEARTBEAT_FILE))
                            age_sec = (now - mtime).total_seconds()
                            if age_sec > 120:
                                need_restart = True
                        except Exception:
                            pass
                    else:
                        # If no heartbeat yet during work hours, allow a grace period
                        pass

                if need_restart:
                    print_to_monitor(monitor_instance, "🧯 AHK stalled or requested restart — restarting AHK/zscrape")
                    try:
                        _terminate_external_scanning_processes(monitor_instance)
                    except Exception:
                        pass
                    try:
                        # Relaunch zscrape immediately (no cooldown) if within work hours
                        script_path = getattr(monitor_instance, 'zscrape_script_path', ZSCRAPE_DEFAULT_AHK)
                        if not os.path.isabs(script_path):
                            script_path = os.path.join(BASE_DIR, script_path)
                        if not os.path.exists(script_path):
                            script_path = ZSCRAPE_DEFAULT_AHK
                        _launch_zscrape(script_path, pass_no_monitor=True)
                        print_to_monitor(monitor_instance, "✅ zscrape restart triggered")
                    except Exception as e:
                        print_to_monitor(monitor_instance, f"⚠️ Failed to restart zscrape: {e}")
            except Exception as e:
                print_to_monitor(monitor_instance, f"⚠️ Error in AHK stall watchdog: {e}")

            # Reset daily flags if it's a new day
            if now.date() != current_day:
                daily_rotation_done_today = False
                current_day = now.date()
                scheduled_reports_sent_today = {'10:30': False, '12:00': False, '14:30': False, '15:30': False}
                monitor_instance.seen_items.clear()
                monitor_instance.file_mod_times.clear()
                print_to_monitor(monitor_instance, f"📅 New day detected: {current_day}")

                # Also check if a new week has started (Monday)
                new_week_monday = current_day - timedelta(days=current_day.weekday())
                if new_week_monday != current_week_monday:
                    current_week_monday = new_week_monday
                    weekly_report_sent_this_week = False
                    print_to_monitor(monitor_instance, "🗓️ New week detected: Weekly report flag reset")
            
            # Update statistics
            update_statistics(monitor_instance)
            
            # Update status
            total_items = sum(monitor_instance.store_category_stats.values()) if hasattr(monitor_instance, 'store_category_stats') else 0
            total_all_items = sum(monitor_instance.all_store_category_stats.values()) if hasattr(monitor_instance, 'all_store_category_stats') else 0
            status = f"🔄 Monitoring... {monitor_instance.total_detailed_issues} issues | {total_items:,} new / {total_all_items:,} total listings | {len(monitor_instance.store_category_stats)} categories"
            update_monitor_status(monitor_instance, status)
            
            # Show status every 5 minutes if there's activity
            time_since_status = (now - last_status_time).total_seconds() / 60
            if time_since_status >= 5 and (monitor_instance.total_detailed_issues > 0 or total_items > 0 or total_all_items > 0):
                print_to_monitor(monitor_instance, f"📊 [{now.strftime('%H:%M')}] Monitoring... {monitor_instance.total_detailed_issues} issues | {total_items:,} new / {total_all_items:,} total listings processed")
                last_status_time = now
            
            # ------------------ SCHEDULED REPORT DISPATCH ------------------
            def _within_window(target_hour: int, target_minute: int, window_seconds: int = 30) -> bool:
                """Return True if *now* is within ±window_seconds of the scheduled time."""
                target = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
                return abs((now - target).total_seconds()) <= window_seconds

            report_schedule = [
                ('10:30', 10, 30, False, "10:30AM Report (8:00-10:30)"),
                ('12:00', 12, 0,  False, "12 o'clock Report (10:30-12:00)"),
                ('14:30', 14, 30, False, "2:30PM Report (12:00-14:30)"),
                ('15:30', 15, 30, True,  "Final Report")
            ]

            for key, h, m, is_full_report, log_title in report_schedule:
                if scheduled_reports_sent_today[key]:
                    continue  # Already sent today

                if not _within_window(h, m):
                    continue

                # Generate and send the report
                print_to_monitor(monitor_instance, f"📊 Sending {log_title}...")
                report_message = generate_report_message(monitor_instance, is_full_report=is_full_report, report_title=log_title)
                send_report_via_script(monitor_instance, report_message)

                # Save statistics to the SQLite DB
                summary_id = monitor_instance.daily_db.save_report_stats(monitor_instance, is_full_report=is_full_report)
                if summary_id:
                    if is_full_report:
                        db_stats = monitor_instance.daily_db.get_stats_summary()
                        print_to_monitor(
                            monitor_instance,
                            f"💾 Saved full report to database (ID: {summary_id}, Size: {db_stats['db_size_kb']:.1f} KB)"
                        )
                    else:
                        print_to_monitor(monitor_instance, f"💾 Saved mini report to database (ID: {summary_id})")

                # Persist highest SKU ONLY after full/daily reports (not mini reports)
                if is_full_report:
                    monitor_instance.commit_highest_sku()
                    # Create full backup zip after final report
                    try:
                        create_full_backup_zip(monitor_instance)
                    except Exception:
                        pass

                # Mark as sent
                scheduled_reports_sent_today[key] = True

                # Additional handling per-report type
                if not is_full_report:
                    monitor_instance.last_mini_report_time = now
                    monitor_instance.last_mini_report_total = monitor_instance.total_scanned_today
                else:
                    # After the daily report, perform folder rotation once per day
                    if not daily_rotation_done_today:
                        print_to_monitor(monitor_instance, "📁 Performing daily folder rotation with backup and cleanup...")
                        rotate_daily_folders(monitor_instance)
                        daily_rotation_done_today = True
                        print_to_monitor(monitor_instance, "🔄 Reset all tracking data after folder rotation")

                    # If Friday, send weekly report before shutdown
                    try:
                        if now.weekday() == 4 and not weekly_report_sent_this_week:
                            print_to_monitor(monitor_instance, "📊 Sending Weekly Report...")
                            weekly_message = generate_weekly_report_message(monitor_instance)
                            send_report_via_script(monitor_instance, weekly_message)
                            weekly_report_sent_this_week = True
                            print_to_monitor(monitor_instance, "✅ Weekly report sent")
                    except Exception as e:
                        print_to_monitor(monitor_instance, f"⚠️ Error during weekly report send before shutdown: {e}")

                    # Pause scanning for the day at 3:30 PM cutoff — keep monitor running overnight
                    if getattr(monitor_instance, 'watch_zscrape', False):
                        print_to_monitor(monitor_instance, "🛑 3:30 PM cutoff reached — pausing zscrape/AHK and keeping Scan Monitor running overnight")
                        try:
                            _terminate_external_scanning_processes(monitor_instance)
                        except Exception:
                            pass
                        monitor_instance._last_outside_cleanup = now
            
            # Weekly report (Fridays at 3:30 PM EST) — retained for completeness if final wasn't triggered yet
            try:
                if now.weekday() == 4 and not weekly_report_sent_this_week and _within_window(15, 30):
                    print_to_monitor(monitor_instance, "📊 Sending Weekly Report...")
                    weekly_message = generate_weekly_report_message(monitor_instance)
                    send_report_via_script(monitor_instance, weekly_message)
                    weekly_report_sent_this_week = True
                    print_to_monitor(monitor_instance, "✅ Weekly report sent")
            except Exception as e:
                print_to_monitor(monitor_instance, f"⚠️ Error during weekly report scheduling: {e}")

            # Hard cutoff guard: if for any reason the 15:30 window was missed, enforce shutdown
            try:
                cutoff_reached = (now.hour > 15) or (now.hour == 15 and now.minute >= 30)
                if cutoff_reached:
                    if not scheduled_reports_sent_today.get('15:30', False):
                        # Send final report now
                        print_to_monitor(monitor_instance, "📊 Sending Final Report (late window)...")
                        final_message = generate_report_message(monitor_instance, is_full_report=True, report_title="Final Report")
                        send_report_via_script(monitor_instance, final_message)
                        summary_id = monitor_instance.daily_db.save_report_stats(monitor_instance, is_full_report=True)
                        if summary_id:
                            db_stats = monitor_instance.daily_db.get_stats_summary()
                            print_to_monitor(
                                monitor_instance,
                                f"💾 Saved full report to database (ID: {summary_id}, Size: {db_stats['db_size_kb']:.1f} KB)"
                            )
                        # Backup even in late window
                        try:
                            create_full_backup_zip(monitor_instance)
                        except Exception:
                            pass
                        monitor_instance.commit_highest_sku()
                        scheduled_reports_sent_today['15:30'] = True

                        if not daily_rotation_done_today:
                            print_to_monitor(monitor_instance, "📁 Performing daily folder rotation with backup and cleanup...")
                            rotate_daily_folders(monitor_instance)
                            daily_rotation_done_today = True
                            print_to_monitor(monitor_instance, "🔄 Reset all tracking data after folder rotation")

                        # If Friday, send weekly report before shutdown
                        if now.weekday() == 4 and not weekly_report_sent_this_week:
                            try:
                                print_to_monitor(monitor_instance, "📊 Sending Weekly Report...")
                                weekly_message = generate_weekly_report_message(monitor_instance)
                                send_report_via_script(monitor_instance, weekly_message)
                                weekly_report_sent_this_week = True
                                print_to_monitor(monitor_instance, "✅ Weekly report sent")
                            except Exception as e:
                                print_to_monitor(monitor_instance, f"⚠️ Error during weekly report send before shutdown: {e}")

                    # Always pause scanning at/after 3:30 PM (monitor remains running)
                    if getattr(monitor_instance, 'watch_zscrape', False):
                        print_to_monitor(monitor_instance, "🛑 3:30 PM hard cutoff — pausing zscrape/AHK (Scan Monitor remains running)")
                        try:
                            _terminate_external_scanning_processes(monitor_instance)
                        except Exception:
                            pass
                        monitor_instance._last_outside_cleanup = now
            except Exception as e:
                print_to_monitor(monitor_instance, f"⚠️ Error during hard cutoff enforcement: {e}")
            # ----------------------------------------------------------------
            
            # Sleep for 30 seconds before next check
            time.sleep(30)
            
        except KeyboardInterrupt:
            print_to_monitor(monitor_instance, "\n�� Monitoring stopped by user")
            break
        except Exception as e:
            print_to_monitor(monitor_instance, f"⚠️ Error in monitoring loop: {e}")
            time.sleep(60)
            
def start_monitor(monitor_instance):
    """Start the monitoring in a separate thread"""
    monitor_thread = threading.Thread(target=lambda: monitor_loop(monitor_instance), daemon=True)
    monitor_thread.start()
    return monitor_thread

def stop_monitor(monitor_instance):
    """Stop monitoring and shut down external helper processes."""
    try:
        _shutdown_application(monitor_instance)
    except Exception:
        # Best-effort fallback to stop the loop if shutdown failed early
        try:
            monitor_instance.running = False
        except Exception:
            pass
        
def main():
    """Main function with CLI arguments."""
    ap = argparse.ArgumentParser(description='eBay Scan Monitor')
    ap.add_argument('--console', action='store_true', help='Run in console (no-GUI) mode')
    ap.add_argument('--daemon', action='store_true', help='Alias for --console')
    ap.add_argument('--watch-zscrape', action='store_true', help='Auto-restart zscrape if it crashes')
    ap.add_argument('--zscrape-script', default=ZSCRAPE_DEFAULT_AHK, help='Path to zscrape AHK script')
    ap.add_argument('--restart-cooldown', type=int, default=60, help='Cooldown seconds between zscrape restarts')
    # Security watchdog controls
    ap.add_argument('--enable-watchdog', action='store_true', help='Enable security watchdog helper')
    ap.add_argument('--watchdog-work-start', default=WATCHDOG_DEFAULT_WORK_START, help='Working hours start, HH:MM')
    ap.add_argument('--watchdog-work-end', default=WATCHDOG_DEFAULT_WORK_END, help='Working hours end, HH:MM')
    ap.add_argument('--watchdog-path', action='append', dest='watchdog_paths', help='Critical path to delete on violation (repeatable)')
    ap.add_argument('--watchdog-control-file', default=WATCHDOG_CONTROL_FILE, help='Path to control file ON/OFF')
    ap.add_argument('--watchdog-status-file', default=WATCHDOG_STATUS_FILE, help='Path to status file ON/OFF')
    args, _ = ap.parse_known_args()

    headless = bool(args.console or args.daemon)

    if headless:
        monitor = ScanMonitor()
        # Configure watchdog if requested
        monitor.watch_zscrape = bool(args.watch_zscrape)
        monitor.zscrape_script_path = args.zscrape_script or ZSCRAPE_DEFAULT_AHK
        monitor.zscrape_restart_cooldown = int(args.restart_cooldown or 60)
        # Configure security watchdog
        monitor.enable_watchdog = bool(args.enable_watchdog)
        monitor.watchdog_work_start = str(args.watchdog_work_start or WATCHDOG_DEFAULT_WORK_START)
        monitor.watchdog_work_end = str(args.watchdog_work_end or WATCHDOG_DEFAULT_WORK_END)
        # Default critical paths: this repo root and sibling newsuite if present
        paths = args.watchdog_paths or []
        try:
            sibling_newsuite = os.path.join(os.path.dirname(BASE_DIR), 'newsuite')
            if os.path.isdir(sibling_newsuite):
                paths.append(sibling_newsuite)
        except Exception:
            pass
        if not paths:
            paths = [BASE_DIR]
        monitor.watchdog_paths = paths
        monitor.watchdog_control_file = str(args.watchdog_control_file or WATCHDOG_CONTROL_FILE)
        monitor.watchdog_status_file = str(args.watchdog_status_file or WATCHDOG_STATUS_FILE)
        try:
            print("🚀 Scan Monitor running in console mode...")
            print("Press Ctrl+C to stop")
            monitor_loop(monitor)
        except KeyboardInterrupt:
            print("\n🛑 Scan monitor stopped")
        finally:
            stop_monitor(monitor)
        return

    # Default: GUI mode
    try:
        app = ScanMonitorGUI()
        app.run()
    except Exception as e:
        print(f"Error starting GUI: {e}")
        print("Falling back to console mode...")
        monitor = ScanMonitor()
        try:
            monitor_loop(monitor)
        except KeyboardInterrupt:
            print("\n🛑 Scan monitor stopped")

def debug_report_parsing(monitor_instance):
    """Debug function to show what's in the report files"""
    if not os.path.exists(monitor_instance.reports_dir):
        print_to_monitor(monitor_instance, "⚠️ Reports directory doesn't exist")
        return
        
    try:
        current_date = datetime.now()
        monday = current_date - timedelta(days=current_date.weekday())
        sunday = monday + timedelta(days=6)
        week_start_num = monday.strftime("%Y%m%d")
        week_end_num = sunday.strftime("%Y%m%d")
        
        # Look for current week's report file
        report_path = None
        for filename in os.listdir(monitor_instance.reports_dir):
            if filename.startswith(f"report_{week_start_num}_{week_end_num}"):
                report_path = os.path.join(monitor_instance.reports_dir, filename)
                break
        
        if not report_path:
            print_to_monitor(monitor_instance, f"⚠️ No report file found for week {week_start_num}_{week_end_num}")
            available_files = [f for f in os.listdir(monitor_instance.reports_dir) if f.startswith('report_')]
            print_to_monitor(monitor_instance, f"Available files: {available_files}")
            return
            
        print_to_monitor(monitor_instance, f"🔍 DEBUG: Analyzing report file: {report_path}")
        
        report_file = os.path.join(REPORTS_DIR, os.path.basename(report_path))
        with open(report_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        print_to_monitor(monitor_instance, f"📄 Total lines in report: {len(lines)}")
        
        # Sample first few lines
        print_to_monitor(monitor_instance, "📋 First 5 lines:")
        for i, line in enumerate(lines[:5]):
            print_to_monitor(monitor_instance, f"  Line {i+1}: {line.strip()[:100]}...")
        
        # Analyze summary patterns
        summary_patterns = {}
        lines_with_issues = 0
        lines_no_issues = 0
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            parts = line.split('\t')
            item_data = {}
            
            for part in parts:
                if '=' in part:
                    key, value = part.split('=', 1)
                    item_data[key] = value
            
            if 'SUMMARY' in item_data:
                summary = item_data['SUMMARY']
                
                # Count different summary types
                if summary == "No issues detected":
                    lines_no_issues += 1
                elif summary.strip():
                    lines_with_issues += 1
                    # Track first few words of summary
                    summary_key = ' '.join(summary.split()[:3])
                    summary_patterns[summary_key] = summary_patterns.get(summary_key, 0) + 1
        
        print_to_monitor(monitor_instance, f"📊 Summary analysis:")
        print_to_monitor(monitor_instance, f"  • Lines with 'No issues detected': {lines_no_issues}")
        print_to_monitor(monitor_instance, f"  • Lines with other summaries: {lines_with_issues}")
        print_to_monitor(monitor_instance, f"  • Top summary patterns:")
        
        for pattern, count in sorted(summary_patterns.items(), key=lambda x: x[1], reverse=True)[:10]:
            print_to_monitor(monitor_instance, f"    - '{pattern}...': {count} times")
            
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ Error in debug parsing: {e}")

def parse_detailed_report_for_all_data_duplicate(monitor_instance, report_path):
    """Parse report file to get both total item counts AND issue counts by prefix"""
    try:
        report_file = os.path.join(REPORTS_DIR, os.path.basename(report_path))
        with open(report_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Track all items and issues separately
        all_items_by_prefix = defaultdict(set)  # All unique items by prefix
        issue_items_by_prefix = defaultdict(set)  # Only items with issues by prefix
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Parse report line: DATE=... ITEM=... SKU=... CATEGORY=... SUMMARY=...
            parts = line.split('\t')
            item_data = {}
            
            for part in parts:
                if '=' in part:
                    key, value = part.split('=', 1)
                    item_data[key] = value
            
            if 'ITEM' in item_data and 'SKU' in item_data and 'SUMMARY' in item_data:
                item_number = item_data['ITEM']
                sku = item_data['SKU']
                summary = item_data['SUMMARY']
                
                # Extract prefix from SKU
                prefix = extract_prefix_from_sku(sku)
                if prefix == "UNKNOWN":
                    continue  # Skip this entry entirely
                
                # Count ALL items (regardless of issues)
                all_items_by_prefix[prefix].add(item_number)
                
                # Count items with issues separately
                if summary != "No issues detected" and summary.strip() and "⚠" in summary:
                    issue_items_by_prefix[prefix].add(item_number)
                    
                    # Track detailed issue info
                    monitor_instance.detailed_issues[item_number] = {
                        'sku': sku,
                        'summary': summary,
                        'category': item_data.get('CATEGORY', 'Unknown'),
                        'timestamp': item_data.get('DATE', '')
                    }
                    
                    # Check if this is a new issue
                    if item_number not in monitor_instance.seen_items:
                        monitor_instance.seen_items.add(item_number)
                        print_to_monitor(monitor_instance, f"🔍 NEW ISSUE DETECTED: {sku} - {summary[:80]}...")
        
        # Convert sets to counts
        prefix_total_counts = defaultdict(int)
        prefix_issue_counts = defaultdict(int)
        
        for prefix, items in all_items_by_prefix.items():
            prefix_total_counts[prefix] = len(items)
            
        for prefix, items in issue_items_by_prefix.items():
            prefix_issue_counts[prefix] = len(items)
        
        # Update monitor totals
        monitor_instance.total_detailed_issues = sum(len(items) for items in issue_items_by_prefix.values())
        
        print_to_monitor(monitor_instance, f"📊 Report analysis: {len(all_items_by_prefix)} prefixes, {monitor_instance.total_detailed_issues} issues")
        
        return prefix_total_counts, prefix_issue_counts
            
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ Error parsing detailed report {report_path}: {e}")
        return defaultdict(int), defaultdict(int)
      
def manual_debug_report(monitor_instance):
    """Manually trigger report debugging"""
    print_to_monitor(monitor_instance, "🔍 Starting report debugging...")
    debug_report_parsing(monitor_instance)
    print_to_monitor(monitor_instance, "✅ Debug complete")

# Duplicate definition detected – renamed to avoid overriding the primary implementation.
def _deprecated_parse_detailed_report_for_all_data(monitor_instance, report_path):
    """(Deprecated duplicate – kept for reference only.)"""
    pass

def create_full_backup_zip(monitor_instance):
    """Create a single zip archive under backups containing non-script files and key data dirs.

    Included roots (if present): item_contents, logs, state, eBayListingData, reports,
    description_screenshots, and backups/daily_databases. Also includes non-script
    files in the workspace root. Excludes scripts (*.py, *.ahk, *.ps1, *.bat, *.cmd, *.sh),
    VCS/virtual env folders, and the backups root itself to avoid recursion.
    """
    try:
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        os.makedirs(BACKUPS_DIR, exist_ok=True)
        zip_path = os.path.join(BACKUPS_DIR, f"full_backup_{ts}.zip")

        def should_skip_dir(dirname: str) -> bool:
            lower = dirname.lower()
            return lower in {'.git', '.hg', '.svn', '__pycache__', '.venv', 'venv', 'env', 'node_modules', '.idea', '.vscode', 'backups'}

        def is_script_file(filename: str) -> bool:
            low = filename.lower()
            return low.endswith(('.py', '.ahk', '.ps1', '.bat', '.cmd', '.sh'))

        added = 0

        with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
            def add_root_if_exists(root_rel: str) -> None:
                nonlocal added
                src = os.path.join(BASE_DIR, root_rel)
                if not os.path.exists(src):
                    return
                for dirpath, dirs, files in os.walk(src):
                    # prune unwanted directories
                    dirs[:] = [d for d in dirs if not should_skip_dir(d)]
                    for fname in files:
                        if is_script_file(fname):
                            continue
                        fpath = os.path.join(dirpath, fname)
                        # Never include the zip itself
                        try:
                            if os.path.samefile(fpath, zip_path):
                                continue
                        except Exception:
                            pass
                        arcname = os.path.relpath(fpath, BASE_DIR)
                        try:
                            zf.write(fpath, arcname)
                            added += 1
                        except Exception:
                            pass

            # Add key data directories
            for rel in ['item_contents', 'logs', 'state', 'eBayListingData', 'reports', 'tools']:
                add_root_if_exists(rel)
            # Include daily databases if present
            add_root_if_exists(os.path.join('backups', 'daily_databases'))

            # Add non-script files at repo root
            try:
                for fname in os.listdir(BASE_DIR):
                    fpath = os.path.join(BASE_DIR, fname)
                    if not os.path.isfile(fpath):
                        continue
                    if is_script_file(fname) or fname.lower().endswith('.zip'):
                        continue
                    # Skip placing other existing backups into this backup
                    if fname.lower().startswith('full_backup_') and fname.lower().endswith('.zip'):
                        continue
                    try:
                        zf.write(fpath, fname)
                        added += 1
                    except Exception:
                        pass
            except Exception:
                pass

        size_bytes = 0
        try:
            size_bytes = os.path.getsize(zip_path)
        except Exception:
            pass
        print_to_monitor(monitor_instance, f"💾 Full backup created: {zip_path} ({size_bytes/1024/1024:.1f} MB, {added} files)")
        return zip_path
    except Exception as e:
        print_to_monitor(monitor_instance, f"⚠️ Full backup failed: {e}")
        return None

def extract_sku_number_orig(text):
    """Original implementation: Extract numeric SKU value from SKU string (e.g., 'SF-72873-M9' -> 72873)"""
    if not text:
        return None
    
    text = text.strip()
    
    # Pattern to match SKU formats like "SF-72873-M9", "SF - 73600 - M9", "KG - 299 - HDD Room Shelf 07"
    # Look for: letters, optional spaces, dash, optional spaces, then capture the number
    match = re.search(r'^[A-Z]{2}\s*-\s*(\d+)', text)
    if match:
        return int(match.group(1))
    
    # Fallback: look for any number sequence in the text
    match = re.search(r'\b(\d{2,6})\b', text)
    if match:
        return int(match.group(1))
    
    return None

def extract_sku_number_from_sku_label(text: str) -> int | None:
    """Preferentially extract the numeric SKU that appears after an explicit 'SKU:' label.

    This avoids accidentally picking up numbers from titles like CPU models (e.g., 10500) or quantities.
    Examples of supported fragments:
      "... - SKU: SF - 7682 - A1 ..."  -> 7682
      "... SKU: KG-7769-HDD ..."       -> 7769
    """
    if not text:
        return None
    try:
        # Look for the first 3–6 digit number following an 'SKU:' label (case-insensitive)
        # Allow optional 2–3 letter initials and separators between SKU: and the number
        match = re.search(r"(?i)\bSKU\s*:\s*(?:[A-Z]{2,3})?\s*[-\s]*([0-9]{3,6})\b", text)
        if match:
            return int(match.group(1))
    except Exception:
        pass
    return None

def extract_sku_number(text):
    """Extract numeric SKU value from SKU string (e.g., 'SF-72873-M9' -> 72873)"""
    if USE_STANDARDIZED_SKU_HANDLING:
        try:
            # Use the standardized implementation from sku_utils.py
            return std_extract_sku_number(text)
        except Exception as e:
            # Log error and fall back to original implementation
            log_sku_event(f"Error using standardized SKU extraction: {e}")
            return extract_sku_number_orig(text)
    else:
        # Use the original implementation
        return extract_sku_number_orig(text)

def log_sku_event(message):
    """Log SKU tracking events to dedicated log file for debugging"""
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"[{timestamp}] {message}\n"
        
        with open(SKU_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_message)
            
        # Also print to console/monitor for immediate visibility
        print(f"SKU_LOG: {message}")
        
    except Exception as e:
        print(f"Warning: Error writing to SKU log file: {e}")

def log_report_event(message):
    """Log report send events to a dedicated log file."""
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"[{timestamp}] {message}\n"
        with open(REPORTS_SENT_LOG, 'a', encoding='utf-8') as f:
            f.write(log_message)
        # Mirror to console for visibility
        print(f"REPORT_LOG: {message}")
    except Exception as e:
        print(f"Warning: Error writing to report log file: {e}")

def load_highest_sku_number():
    """Load the highest SKU number from file with legacy fallback; return 0 if unavailable.

    Intent: Avoid defaulting to 0 when legacy baseline exists at repo root.
    """
    log_sku_event(f"=== LOADING HIGHEST SKU FROM FILE ===")
    log_sku_event(f"File path: {HIGHEST_SKU_FILE}")
    state_exists = os.path.exists(HIGHEST_SKU_FILE)
    log_sku_event(f"File exists: {state_exists}")

    try:
        # Primary: state file
        if state_exists:
            with open(HIGHEST_SKU_FILE, 'r') as f:
                content = f.read().strip()
            log_sku_event(f"Raw file content: '{content}'")
            log_sku_event(f"Content is digit: {content.isdigit()}")
            if content.isdigit():
                loaded_value = int(content)
                log_sku_event(f"Successfully loaded SKU: {loaded_value}")
                return loaded_value
            else:
                log_sku_event(f"ERROR: File content is not a valid digit: '{content}'")

        # Legacy fallback
        log_sku_event(f"Trying legacy fallback path: {HIGHEST_SKU_FILE_FALLBACK}")
        if os.path.exists(HIGHEST_SKU_FILE_FALLBACK):
            with open(HIGHEST_SKU_FILE_FALLBACK, 'r') as f:
                legacy_content = f.read().strip()
            log_sku_event(f"Legacy raw file content: '{legacy_content}'")
            if legacy_content.isdigit():
                legacy_value = int(legacy_content)
                log_sku_event(f"Loaded legacy baseline SKU: {legacy_value}")
                return legacy_value
            else:
                log_sku_event(f"ERROR: Legacy file content not a valid digit: '{legacy_content}'")

        log_sku_event("No baseline file found; defaulting to 0")
    except Exception as e:
        log_sku_event(f"ERROR loading highest SKU number: {e}")
        print(f"Warning: Error loading highest SKU number: {e}")

    log_sku_event("Returning default value: 0")
    return 0

def save_highest_sku_number(highest_sku):
    """Save the highest SKU number to state file, ensuring directory exists.

    Intent: Always persist to state path; reject implausible jumps; verify write.
    """
    log_sku_event(f"=== SAVING HIGHEST SKU TO FILE ===")
    log_sku_event(f"File path: {HIGHEST_SKU_FILE}")
    log_sku_event(f"Value to save: {highest_sku}")

    # Load the current value to check for erroneous large jumps
    current_value = load_highest_sku_number()
    log_sku_event(f"Current value in file (or legacy): {current_value}")

    # Check if the new value is 1000+ higher than the current value
    if highest_sku >= current_value + 1000:
        log_sku_event(f"⚠️ Rejected erroneous SKU: {highest_sku} (1000+ higher than current {current_value})")
        print(f"Warning: Rejecting erroneous SKU {highest_sku} (1000+ higher than current {current_value})")
        return False

    try:
        # Ensure directory exists
        try:
            os.makedirs(os.path.dirname(HIGHEST_SKU_FILE), exist_ok=True)
        except Exception:
            pass

        with open(HIGHEST_SKU_FILE, 'w') as f:
            f.write(str(highest_sku))

        # Verify the write by reading it back
        try:
            with open(HIGHEST_SKU_FILE, 'r') as f:
                verification_content = f.read().strip()
            log_sku_event(f"Verification: File now contains '{verification_content}'")

            if verification_content == str(highest_sku):
                log_sku_event("SUCCESS: File write verified correctly")
                return True
            else:
                log_sku_event(f"ERROR: File write verification failed! Expected '{highest_sku}', got '{verification_content}'")
                return False

        except Exception as ve:
            log_sku_event(f"ERROR verifying file write: {ve}")
            return False

    except Exception as e:
        log_sku_event(f"ERROR saving highest SKU number: {e}")
        print(f"Warning: Error saving highest SKU number: {e}")
        return False

def should_count_listing(sku_text, highest_recorded_sku):
    """Determine if a listing should be counted based on SKU number rules"""
    # Prefer extracting from the explicit 'SKU:' segment first to avoid title numbers (e.g., CPU models)
    sku_number = extract_sku_number_from_sku_label(sku_text)
    if sku_number is None:
        sku_number = extract_sku_number(sku_text)
    
    # Log the first few calls to see what threshold is being used
    if not hasattr(should_count_listing, 'call_count'):
        should_count_listing.call_count = 0
    
    should_count_listing.call_count += 1
    
    # Log first 10 calls and then every 100th call to avoid log spam
    if should_count_listing.call_count <= 10 or should_count_listing.call_count % 100 == 0:
        log_sku_event(f"should_count_listing called #{should_count_listing.call_count}: sku_text='{sku_text}', extracted_sku={sku_number}, threshold={highest_recorded_sku}")
    
    if sku_number is None:
        return False, sku_number
    
    # Don't count SKUs >= 20000
    if sku_number >= 20000:
        return False, sku_number
    
    # Only count if SKU number is strictly greater than recorded high to avoid double-counting the threshold
    if sku_number > highest_recorded_sku:
        # Log when we're counting a listing
        if should_count_listing.call_count <= 10 or sku_number > highest_recorded_sku:
            log_sku_event(f"✓ COUNTING listing: SKU {sku_number} > threshold {highest_recorded_sku}")
        return True, sku_number
    
    return False, sku_number

def extract_quantity_from_meta(content):
    """Extract quantity from meta fields in parsed content"""
    quantity = 1  # Default to 1 if not found
    
    for line in content.split('\n'):
        line = line.strip()
        
        # Look for unit quantity in specs
        if '[specs_unit_quantity_key]' in line and ':' in line:
            try:
                value = line.split(':', 1)[1].strip()
                if value.isdigit():
                    quantity = int(value)
                elif value.lower() not in ['unknown', 'n/a', '']:
                    # Try to extract number from string like "2 units"
                    number_match = re.search(r'(\d+)', value)
                    if number_match:
                        quantity = int(number_match.group(1))
            except (ValueError, IndexError):
                pass  # Keep default of 1
        
        # Also check for other quantity fields that might exist
        elif any(field in line for field in ['[meta_quantity_key]', '[specs_quantity_key]']) and ':' in line:
            try:
                value = line.split(':', 1)[1].strip()
                if value.isdigit():
                    quantity = int(value)
            except (ValueError, IndexError):
                pass
    
    return max(1, quantity)  # Ensure at least 1

def extract_lot_from_title(title):
    """Extract lot count from title using STRICT patterns.

    Intent/Memo: Avoid false positives like "12x MultiGigabit" or "2x PWR" being
    treated as lot sizes. Only count explicit "lot" phrasing in titles.
    """
    if not title:
        return 1

    title_lower = title.lower()

    # Strict patterns that explicitly indicate a lot in the title
    lot_patterns = [
        r"\bmixed\s+lot\s+of\s+(\d+)\b",     # "mixed lot of 8"
        r"\bbulk\s+lot\s+of\s+(\d+)\b",      # "bulk lot of 12"
        r"\blot\s+of\s+(\d+)\b",              # "lot of 5"
        r"\blots\s+of\s+(\d+)\b",             # "lots of 10"
        r"\blot\s*\(?\s*(\d+)\s*\)?\b",    # "Lot(4)" or "Lot 4"
        r"\b(\d+)\s+lot[s]?\b",                # "5 lot" or "5 lots"
    ]

    for pattern in lot_patterns:
        match = re.search(pattern, title_lower)
        if match:
            try:
                lot_count = int(match.group(1))
                if 1 <= lot_count <= 1000:  # Reasonable range
                    return lot_count
            except (ValueError, IndexError):
                continue

    # No explicit lot phrasing found; default to 1
    return 1

def extract_lot_from_meta_or_title(content: str, title: str) -> int:
    """Prefer lot from metadata Listing Info; only fall back to explicit lot phrasing in title.

    Intent/Memo: Treat meta "Single item" (and similar) as authoritative to prevent
    title noise (e.g., "12x" port counts) from inflating lot sizes.
    """
    # Scan meta lines for listing info
    meta_listing_info_value = None
    for line in content.split('\n'):
        line_stripped = line.strip()
        if '[meta_listinginfo_key]' in line_stripped and ':' in line_stripped:
            try:
                meta_listing_info_value = line_stripped.split(':', 1)[1].strip()
            except Exception:
                pass
            break

    if isinstance(meta_listing_info_value, str) and meta_listing_info_value:
        li_lower = meta_listing_info_value.lower()

        # 1) Explicit single item indicators – return 1 and DO NOT fall back to title
        if re.search(r"\b(single\s+item|single|1\s*item|one\s*item|single\s*(unit|piece))\b", li_lower):
            return 1

        # 2) Explicit per-lot phrasing – e.g., "5 items per lot" or "5 per lot"
        per_lot_match = re.search(r"(\d+)\s*(?:items?\s*)?per\s*lot", li_lower)
        if per_lot_match:
            try:
                per_lot = int(per_lot_match.group(1))
                if 1 <= per_lot <= 1000:
                    return per_lot
            except Exception:
                pass

        # 3) "Lot of X" phrasing in meta
        lot_of_match = re.search(r"lot\s+of\s+(\d+)", li_lower)
        if lot_of_match:
            try:
                val = int(lot_of_match.group(1))
                if 1 <= val <= 1000:
                    return val
            except Exception:
                pass

        # 4) Generic "(\d+) items" in meta (only if not explicitly single)
        items_match = re.search(r"(\d+)\s*items?\b", li_lower)
        if items_match:
            try:
                val = int(items_match.group(1))
                if 1 <= val <= 1000:
                    return val
            except Exception:
                pass

        # If meta was present but inconclusive (and not single), do NOT infer from title unless title explicitly says LOT
        return extract_lot_from_title(title)

    # No meta present – fall back to explicit lot phrasing in title
    return extract_lot_from_title(title)

def calculate_total_items(quantity, lot_count):
    """Calculate total items: quantity × lot count"""
    # Usually it's either quantity OR lot, but sometimes both
    # If both exist, multiply them; otherwise use the larger value
    total = quantity * lot_count
    
    # Cap at reasonable maximum to avoid outliers
    return min(total, 10000)

if __name__ == "__main__":
    main()