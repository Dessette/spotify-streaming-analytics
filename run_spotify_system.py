import subprocess
import time
import os
import sys
import signal
import threading
from datetime import datetime
import psutil
import psycopg2

class SpotifySparkSystemRunner:
    def __init__(self):
        self.processes = {}
        self.running = True
        self.producer_completed = False
        self.producer_restart_count = 0
        self.max_producer_restarts = 1
        self.reset_database = True  # Default to reset

        self.pg_host = os.getenv("PG_HOST", "127.0.0.1")
        self.pg_port = int(os.getenv("PG_PORT", "5432"))
        self.pg_db   = os.getenv("PG_DB", "spotify_analytics")  # init.sql’deki isimle aynı olmalı
        self.pg_user = os.getenv("PG_USER", "spotify_user")
        self.pg_psw  = os.getenv("PG_PSW", "spotify_pass")

        self.reset_database = os.getenv("RESET_DB", "true").lower() not in ("0","false","no","n")
        
    def log(self, message, level="INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        colors = {
            "INFO": "\033[0;36m",  # Cyan
            "SUCCESS": "\033[0;32m",  # Green
            "WARNING": "\033[1;33m",  # Yellow
            "ERROR": "\033[0;31m",  # Red
            "SPARK": "\033[0;35m",  # Magenta
            "CLEANUP": "\033[0;33m",  # Yellow
            "RESET": "\033[0m"
        }
        
        color = colors.get(level, colors["RESET"])
        print(f"{color}[{timestamp}] {level}: {message}{colors['RESET']}")
    
    def cleanup_database_tables(self):
        """Public şemadaki TÜM tabloları dinamik TRUNCATE eder (FK için CASCADE)."""
        if not self.reset_database:
            self.log("Database cleanup disabled", "INFO")
            return True

        try:
            self.log("🗑️ Cleaning up analytics database (dynamic)...", "CLEANUP")

            conn = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                database=self.pg_db,
                user=self.pg_user,
                password=self.pg_psw
            )
            conn.autocommit = True
            cur = conn.cursor()

            # Public şemadaki bütün tabloları listele
            cur.execute("""
                SELECT tablename
                FROM pg_tables
                WHERE schemaname='public'
                ORDER BY tablename;
            """)
            tables = [r[0] for r in cur.fetchall()]

            if not tables:
                self.log("Public şemada tablo bulunamadı; temizlenecek bir şey yok.", "WARNING")
                cur.close()
                conn.close()
                return True

            # Hepsini tek komutta TRUNCATE et (CASCADE FK’ler için)
            truncate_sql = "TRUNCATE TABLE " + ", ".join([f'public."{t}"' for t in tables]) + " RESTART IDENTITY CASCADE;"
            cur.execute(truncate_sql)
            self.log(f"✅ Truncated tables: {', '.join(tables)}", "CLEANUP")

            cur.close()
            conn.close()
            self.log("🎯 Database cleanup completed - Fresh start guaranteed!", "SUCCESS")
            return True

        except Exception as e:
            self.log(f"Database cleanup error: {e}", "ERROR")
            self.log("Continuing without cleanup...", "WARNING")
            return False

    
    def check_requirements(self):
        """Check system requirements"""
        self.log("Checking Spark system requirements...", "SPARK")
        
        # Required Python files
        required_files = [
            'kafka_producer.py',
            'spark_streaming_consumer.py', 
            'interactive_dashboard.py',
            'timeless_genre_analytics.py'
        ]
        
        missing_files = []
        for file in required_files:
            if not os.path.exists(file):
                missing_files.append(file)
        
        if missing_files:
            self.log(f"Missing files: {missing_files}", "ERROR")
            return False
        
        # Check data directory
        if not os.path.exists('./data'):
            self.log("Creating ./data directory...", "WARNING")
            os.makedirs('./data')
        
        # Check CSV file
        csv_files = [f for f in os.listdir('./data') if f.endswith('.csv')]
        if not csv_files:
            self.log("No CSV file found in ./data directory!", "WARNING")
            self.log("Download Spotify dataset from https://www.kaggle.com/datasets/iamsumat/spotify-top-2000s-mega-dataset", "INFO")
        else:
            self.log(f"Dataset found: {csv_files[0]}", "SUCCESS")
        
        # Check Docker services
        try:
            result = subprocess.run(['docker', 'ps', '--format', 'table {{.Names}}'], 
                                  capture_output=True, text=True, check=True)
            required_services = ['kafka', 'postgres', 'spark-master']
            missing_services = []
            
            for service in required_services:
                if service not in result.stdout:
                    missing_services.append(service)
            
            if missing_services:
                self.log(f"Missing Docker services: {missing_services}", "WARNING")
                self.log("Run: docker-compose up -d", "INFO")
            else:
                self.log("Docker services active", "SUCCESS")
                
        except subprocess.CalledProcessError:
            self.log("Docker not found or not running", "ERROR")
            return False
        
        # Check PostgreSQL connection
        try:
            conn = psycopg2.connect(
                host="127.0.0.1",
                port=5432,
                database="spotify_analytics",
                user="spotify_user",
                password="spotify_pass"
            )
            conn.close()
            self.log("PostgreSQL connection verified", "SUCCESS")
        except Exception as e:
            self.log(f"PostgreSQL connection failed: {e}", "ERROR")
            self.log("Ensure PostgreSQL container is running", "WARNING")
            return False
        
        # Check PySpark
        try:
            import pyspark
            self.log(f"PySpark version: {pyspark.__version__}", "SPARK")
        except ImportError:
            self.log("PySpark not found! Install with: pip install pyspark", "ERROR")
            return False
        
        return True
    
    def get_database_statistics(self):
        """Public şemadaki tüm tabloların satır sayısını göster."""
        try:
            conn = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                database=self.pg_db,
                user=self.pg_user,
                password=self.pg_psw
            )
            cur = conn.cursor()

            cur.execute("""
                SELECT tablename
                FROM pg_tables
                WHERE schemaname='public'
                ORDER BY tablename;
            """)
            tables = [r[0] for r in cur.fetchall()]

            stats = {}
            for t in tables:
                try:
                    cur.execute(f'SELECT COUNT(*) FROM public."{t}";')
                    stats[t] = cur.fetchone()[0]
                except Exception as e:
                    stats[t] = f"Error: {e}"

            cur.close()
            conn.close()

            self.log("=== DATABASE STATISTICS ===", "INFO")
            for table, count in stats.items():
                self.log(f'{table}: {count} records', "INFO")
            return stats

        except Exception as e:
            self.log(f"Error getting database statistics: {e}", "ERROR")
            return {}

    
    def start_producer(self):
        """Start Kafka Producer"""
        if self.producer_completed and self.producer_restart_count >= self.max_producer_restarts:
            self.log("Producer dataset completed, not restarting", "INFO")
            return True
            
        self.log("Starting Kafka Producer (UNLIMITED mode)...", "INFO")
        try:
            process = subprocess.Popen(
                [sys.executable, 'kafka_producer.py'],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            self.processes['producer'] = process
            
            def read_producer_output():
                for line in iter(process.stdout.readline, ''):
                    if self.running:
                        print(f"[PRODUCER] {line.strip()}")
                        
                        # Detect completion signals
                        if any(keyword in line.lower() for keyword in [
                            'streaming completed', 
                            'producer closed',
                            'total songs sent'
                        ]):
                            self.log("Producer dataset completed", "SUCCESS")
                            self.producer_completed = True
                        
                        # Detect error signals
                        if any(keyword in line.lower() for keyword in [
                            'csv read error',
                            'dataset setup guide',
                            'file not found'
                        ]):
                            self.log("Producer stopped with data error", "ERROR")
                            
                process.stdout.close()
            
            threading.Thread(target=read_producer_output, daemon=True).start()
            self.log("Kafka Producer started (UNLIMITED processing)", "SUCCESS")
            return True
            
        except Exception as e:
            self.log(f"Producer startup error: {e}", "ERROR")
            return False
    
    def start_spark_consumer(self):
        """Start Spark Structured Streaming Consumer (UNLIMITED)"""
        self.log("Starting Spark UNLIMITED Consumer...", "SPARK")
        try:
            # Set Spark environment variables
            env = os.environ.copy()
            env['PYSPARK_PYTHON'] = sys.executable
            env['PYSPARK_DRIVER_PYTHON'] = sys.executable
            
            # Use the improved consumer
            consumer_file = 'spark_streaming_consumer.py'  # Updated consumer
            
            process = subprocess.Popen(
                [sys.executable, consumer_file],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1,
                env=env
            )
            self.processes['spark_consumer'] = process
            
            def read_spark_output():
                for line in iter(process.stdout.readline, ''):
                    if self.running:
                        print(f"[SPARK] {line.strip()}")
                process.stdout.close()
            
            threading.Thread(target=read_spark_output, daemon=True).start()
            self.log("Spark UNLIMITED Consumer started", "SPARK")
            return True
            
        except Exception as e:
            self.log(f"Spark Consumer startup error: {e}", "ERROR")
            return False
    
    def start_dashboard(self):
        """Start Dashboard"""
        self.log("Starting Spark-powered Dashboard...", "INFO")
        try:
            process = subprocess.Popen(
                [sys.executable, 'interactive_dashboard.py'],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            self.processes['dashboard'] = process
            
            def read_dashboard_output():
                for line in iter(process.stdout.readline, ''):
                    if self.running:
                        print(f"[DASHBOARD] {line.strip()}")
                process.stdout.close()
            
            threading.Thread(target=read_dashboard_output, daemon=True).start()
            self.log("Dashboard started: http://localhost:8050", "SUCCESS")
            return True
            
        except Exception as e:
            self.log(f"Dashboard startup error: {e}", "ERROR")
            return False
    
    def monitor_processes(self):
        """Monitor processes with intelligent restart logic"""
        self.log("Process monitoring started...", "INFO")
        
        while self.running:
            try:
                time.sleep(20)
                
                for name, process in list(self.processes.items()):
                    if process.poll() is not None:
                        return_code = process.returncode
                        
                        if name == 'producer':
                            if self.producer_completed or return_code == 0:
                                self.log("Producer completed normally", "SUCCESS")
                                self.producer_completed = True
                                del self.processes['producer']
                                continue
                            elif self.producer_restart_count < self.max_producer_restarts:
                                self.log(f"Producer crashed, restarting... ({self.producer_restart_count + 1}/{self.max_producer_restarts})", "WARNING")
                                self.producer_restart_count += 1
                                self.start_producer()
                                time.sleep(5)
                            else:
                                self.log("Producer reached max restarts", "WARNING")
                                del self.processes['producer']
                        
                        elif name == 'spark_consumer':
                            if return_code == 0:
                                self.log("Spark Consumer closed normally", "INFO")
                            else:
                                self.log(f"Spark Consumer crashed, restarting...", "WARNING")
                                self.start_spark_consumer()
                                time.sleep(10)
                        
                        elif name == 'dashboard':
                            if return_code == 0:
                                self.log("Dashboard closed normally", "INFO")
                            else:
                                self.log(f"Dashboard crashed, restarting...", "WARNING")
                                self.start_dashboard()
                                time.sleep(5)
                        
            except Exception as e:
                if self.running:
                    self.log(f"Monitoring error: {e}", "ERROR")
                    time.sleep(10)
    
    def show_system_status(self):
        """Show comprehensive system status"""
        self.log("=== UNLIMITED SPARK SYSTEM STATUS ===", "SPARK")
        
        # Process statuses
        for name, process in self.processes.items():
            if process.poll() is None:
                if name == 'spark_consumer':
                    self.log(f"{name.upper()}: RUNNING (PID: {process.pid}) ⚡ UNLIMITED", "SUCCESS")
                else:
                    self.log(f"{name.upper()}: RUNNING (PID: {process.pid})", "SUCCESS")
            else:
                self.log(f"{name.upper()}: STOPPED (Return Code: {process.returncode})", "ERROR")
        
        # Producer special status
        if 'producer' not in self.processes:
            if self.producer_completed:
                self.log("PRODUCER: DATASET COMPLETED ✅", "SUCCESS")
            else:
                self.log("PRODUCER: NOT STARTED OR STOPPED", "WARNING")
        
        # Database statistics
        self.get_database_statistics()
        
        # System resources
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            
            self.log(f"CPU Usage: {cpu_percent}%", "INFO")
            self.log(f"RAM Usage: {memory.percent}% ({memory.used // 1024 // 1024} MB / {memory.total // 1024 // 1024} MB)", "INFO")
        except:
            self.log("Resource information unavailable", "WARNING")
        
        # Access URLs
        self.log("=== ACCESS URLS ===", "INFO")
        self.log("🎵 Spark Dashboard: http://localhost:8050", "INFO")
        self.log("⚡ Spark Master UI: http://localhost:8081", "SPARK")
        self.log("🔧 Kafka UI: http://localhost:8080", "INFO")
        self.log("🗄️ pgAdmin: http://localhost:5050", "INFO")
        
        print()
    
    def reset_system_data(self):
        """Reset all system data"""
        self.log("🔄 Resetting system data...", "CLEANUP")
        
        # Stop all processes first
        for name, process in self.processes.items():
            if process.poll() is None:
                self.log(f"Stopping {name}...", "INFO")
                process.terminate()
                time.sleep(3)
                if process.poll() is None:
                    process.kill()
        
        self.processes.clear()
        
        # Clean database
        self.cleanup_database_tables()
        
        # Reset flags
        self.producer_completed = False
        self.producer_restart_count = 0
        
        self.log("✅ System data reset completed", "SUCCESS")
    
    def signal_handler(self, signum, frame):
        """Graceful shutdown"""
        self.log("Shutdown signal received...", "WARNING")
        self.running = False
        
        for name, process in self.processes.items():
            if process.poll() is None:
                self.log(f"Stopping {name}...", "INFO")
                process.terminate()
                
                timeout = 15 if 'spark' in name else 5
                
                try:
                    process.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    self.log(f"Force killing {name}...", "WARNING")
                    process.kill()
        
        self.log("System stopped", "SUCCESS")
        sys.exit(0)
    
    def run(self):
        """Main execution function"""
        # Signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        print("⚡" + "="*80 + "⚡")
        print("    🚀 SPOTIFY UNLIMITED SPARK ANALYTICS SYSTEM 🚀")
        print("⚡" + "="*80 + "⚡")
        print("💪 NO DATA LIMITS - FULL DATASET PROCESSING")
        print("🗑️ DATABASE RESET ON STARTUP")
        print("🎭 ALL GENRES PRESERVED AND ANALYZED")
        print("=" * 82)
        print()
        
        # Ask user about database reset
        try:
            user_choice = input("Reset database on startup? [Y/n]: ").strip().lower()
            if user_choice in ['n', 'no']:
                self.reset_database = False
                self.log("Database reset disabled by user", "INFO")
            else:
                self.reset_database = True
                self.log("Database will be reset for fresh start", "CLEANUP")
        except:
            self.reset_database = True
        
        # Check requirements
        if not self.check_requirements():
            self.log("Requirements not met, exiting...", "ERROR")
            return
        
        # Clean database if requested
        if self.reset_database:
            self.cleanup_database_tables()
        
        # Start services in order
        self.log("Starting UNLIMITED services...", "SPARK")
        time.sleep(2)
        
        # 1. Producer
        if not self.start_producer():
            return
        time.sleep(5)
        
        # 2. Spark Consumer (UNLIMITED)
        if not self.start_spark_consumer():
            return
        time.sleep(10)
        
        # 3. Dashboard
        if not self.start_dashboard():
            return
        time.sleep(5)
        
        # Show status
        self.show_system_status()
        
        # Start monitoring
        monitor_thread = threading.Thread(target=self.monitor_processes, daemon=True)
        monitor_thread.start()
        
        self.log("🎉 UNLIMITED Spark Analytics System operational!", "SUCCESS")
        self.log("⚡ Apache Spark UNLIMITED Streaming active", "SPARK")
        self.log("📊 ALL data → PostgreSQL (no limits)", "INFO")
        self.log("🎭 ALL genres analyzed (no filtering)", "INFO")
        self.log("🎵 UNLIMITED song processing", "INFO")
        self.log("⏹️ Press Ctrl+C to stop", "INFO")
        print("=" * 80)
        
        # Main loop - user input
        try:
            while self.running:
                print("\nCommands:")
                print("  status (s)     - Show system status")
                print("  reset (r)      - Reset all data and restart")
                print("  db-stats (d)   - Show database statistics")
                print("  restart-prod   - Restart producer only")
                print("  help (h)       - Show this help")
                print("  quit (q)       - Stop the system")
                
                user_input = input("\nCommand: ").strip().lower()
                
                if user_input in ['quit', 'q']:
                    break
                elif user_input in ['status', 's']:
                    self.show_system_status()
                elif user_input in ['reset', 'r']:
                    self.reset_system_data()
                    print("Restarting services...")
                    time.sleep(2)
                    self.start_producer()
                    time.sleep(5)
                    self.start_spark_consumer()
                    time.sleep(10)
                    self.start_dashboard()
                elif user_input in ['db-stats', 'd']:
                    self.get_database_statistics()
                elif user_input == 'restart-prod':
                    if 'producer' in self.processes:
                        self.processes['producer'].terminate()
                        time.sleep(3)
                        if self.processes['producer'].poll() is None:
                            self.processes['producer'].kill()
                        del self.processes['producer']
                    
                    self.producer_completed = False
                    self.producer_restart_count = 0
                    self.start_producer()
                elif user_input in ['help', 'h']:
                    print("UNLIMITED Spark Analytics System Commands")
                elif user_input == '':
                    continue
                else:
                    print(f"Unknown command: {user_input}")
                    
        except EOFError:
            pass
        
        # Cleanup
        self.signal_handler(None, None)

if __name__ == "__main__":
    runner = SpotifySparkSystemRunner()
    runner.run()