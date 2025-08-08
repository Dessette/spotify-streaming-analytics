import subprocess
import time
import os
import sys
import signal
import threading
from datetime import datetime
import psutil
import re

class SpotifySystemRunner:
    def __init__(self):
        self.processes = {}
        self.running = True
        self.producer_completed = False
        self.producer_restart_count = 0
        self.max_producer_restarts = 1
        
    def log(self, message, level="INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        colors = {
            "INFO": "\033[0;36m",  # Cyan
            "SUCCESS": "\033[0;32m",  # Green
            "WARNING": "\033[1;33m",  # Yellow
            "ERROR": "\033[0;31m",  # Red
            "RESET": "\033[0m"
        }
        
        color = colors.get(level, colors["RESET"])
        print(f"{color}[{timestamp}] {level}: {message}{colors['RESET']}")
    
    def check_requirements(self):
        """Gerekli dosyaların ve servislerin kontrolü"""
        self.log("Sistem gereksinimleri kontrol ediliyor...")
        
        # Gerekli Python dosyalarını kontrol et
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
            self.log(f"Eksik dosyalar: {missing_files}", "ERROR")
            return False
        
        # Data klasörünü kontrol et
        if not os.path.exists('./data'):
            self.log("./data klasörü oluşturuluyor...", "WARNING")
            os.makedirs('./data')
        
        # CSV dosyasını kontrol et
        csv_files = [f for f in os.listdir('./data') if f.endswith('.csv')]
        if not csv_files:
            self.log("./data klasöründe CSV dosyası bulunamadı!", "WARNING")
            self.log("Spotify dataset'ini https://www.kaggle.com/datasets/iamsumat/spotify-top-2000s-mega-dataset adresinden indirin", "INFO")
        else:
            self.log(f"Dataset bulundu: {csv_files[0]}", "SUCCESS")
        
        # Docker servislerini kontrol et
        try:
            result = subprocess.run(['docker', 'ps', '--format', 'table {{.Names}}'], 
                                  capture_output=True, text=True, check=True)
            if 'spotify-kafka' in result.stdout and 'spotify-postgres' in result.stdout:
                self.log("Docker servisleri aktif", "SUCCESS")
            else:
                self.log("Docker servisleri başlatılmamış. setup_spotify_system.sh çalıştırın", "WARNING")
        except subprocess.CalledProcessError:
            self.log("Docker bulunamadı veya çalışmıyor", "ERROR")
            return False
        
        return True
    
    def start_producer(self):
        """Kafka Producer'ı başlat"""
        if self.producer_completed and self.producer_restart_count >= self.max_producer_restarts:
            self.log("Producer dataset'ini tamamladı, restart edilmiyor", "INFO")
            return True
            
        self.log("Kafka Producer başlatılıyor...")
        try:
            process = subprocess.Popen(
                [sys.executable, 'kafka_producer.py'],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            self.processes['producer'] = process
            
            # Producer output'unu thread'de oku ve completion'ı yakayla
            def read_producer_output():
                for line in iter(process.stdout.readline, ''):
                    if self.running:
                        print(f"[PRODUCER] {line.strip()}")
                        
                        # Dataset completion sinyallerini yakala
                        if any(keyword in line.lower() for keyword in [
                            'streaming completed', 
                            'producer closed',
                            'total songs sent',
                            'streaming tamamlandı'
                        ]):
                            self.log("Producer dataset'ini tamamladı", "SUCCESS")
                            self.producer_completed = True
                        
                        # Error sinyallerini yakala
                        if any(keyword in line.lower() for keyword in [
                            'csv read error',
                            'dataset setup guide',
                            'file not found'
                        ]):
                            self.log("Producer data error ile durdu", "ERROR")
                            
                process.stdout.close()
            
            threading.Thread(target=read_producer_output, daemon=True).start()
            self.log("Kafka Producer başlatıldı", "SUCCESS")
            return True
            
        except Exception as e:
            self.log(f"Producer başlatma hatası: {e}", "ERROR")
            return False
    
    def start_consumer(self):
        """Spark Consumer'ı başlat"""
        self.log("Spark Consumer başlatılıyor...")
        try:
            process = subprocess.Popen(
                [sys.executable, 'spark_streaming_consumer.py'],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            self.processes['consumer'] = process
            
            # Consumer output'unu thread'de oku
            def read_consumer_output():
                for line in iter(process.stdout.readline, ''):
                    if self.running:
                        print(f"[CONSUMER] {line.strip()}")
                process.stdout.close()
            
            threading.Thread(target=read_consumer_output, daemon=True).start()
            self.log("Spark Consumer başlatıldı", "SUCCESS")
            return True
            
        except Exception as e:
            self.log(f"Consumer başlatma hatası: {e}", "ERROR")
            return False
    
    def start_dashboard(self):
        """Dashboard'u başlat"""
        self.log("Dashboard başlatılıyor...")
        try:
            process = subprocess.Popen(
                [sys.executable, 'interactive_dashboard.py'],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            self.processes['dashboard'] = process
            
            # Dashboard output'unu thread'de oku
            def read_dashboard_output():
                for line in iter(process.stdout.readline, ''):
                    if self.running:
                        print(f"[DASHBOARD] {line.strip()}")
                process.stdout.close()
            
            threading.Thread(target=read_dashboard_output, daemon=True).start()
            self.log("Dashboard başlatıldı: http://localhost:8050", "SUCCESS")
            return True
            
        except Exception as e:
            self.log(f"Dashboard başlatma hatası: {e}", "ERROR")
            return False
    
    def monitor_processes(self):
        """Process'leri izle ve akıllıca yeniden başlat"""
        self.log("Akıllı process monitoring başlatıldı...")
        
        while self.running:
            try:
                time.sleep(15)  # Her 15 saniyede kontrol et (10'dan artırıldı)
                
                for name, process in list(self.processes.items()):
                    if process.poll() is not None:  # Process öldü
                        return_code = process.returncode
                        
                        if name == 'producer':
                            if self.producer_completed or return_code == 0:
                                # Normal completion
                                self.log("Producer normal şekilde tamamlandı", "SUCCESS")
                                self.producer_completed = True
                                # Producer'ı process listesinden çıkar
                                del self.processes['producer']
                                continue
                            elif self.producer_restart_count < self.max_producer_restarts:
                                # Hata ile durdu, restart et
                                self.log(f"Producer crashed (code: {return_code}), restart edilyor... ({self.producer_restart_count + 1}/{self.max_producer_restarts})", "WARNING")
                                self.producer_restart_count += 1
                                self.start_producer()
                                time.sleep(5)
                            else:
                                # Max restart'a ulaşıldı
                                self.log("Producer max restart sayısına ulaştı, restart edilmiyor", "WARNING")
                                del self.processes['producer']
                        
                        elif name == 'consumer':
                            if return_code == 0:
                                self.log("Consumer normal şekilde kapandı", "INFO")
                            else:
                                self.log(f"Consumer crashed (code: {return_code}), restart edilyor...", "WARNING")
                                self.start_consumer()
                                time.sleep(5)
                        
                        elif name == 'dashboard':
                            if return_code == 0:
                                self.log("Dashboard normal şekilde kapandı", "INFO")
                            else:
                                self.log(f"Dashboard crashed (code: {return_code}), restart edilyor...", "WARNING")
                                self.start_dashboard()
                                time.sleep(5)
                        
            except Exception as e:
                if self.running:
                    self.log(f"Monitoring hatası: {e}", "ERROR")
                    time.sleep(5)
    
    def show_system_status(self):
        """Sistem durumunu göster"""
        self.log("=== SİSTEM DURUMU ===", "INFO")
        
        # Process durumları
        for name, process in self.processes.items():
            if process.poll() is None:
                self.log(f"{name.upper()}: ÇALIŞIYOR (PID: {process.pid})", "SUCCESS")
            else:
                self.log(f"{name.upper()}: DURDURULDU (Return Code: {process.returncode})", "ERROR")
        
        # Producer özel durumu
        if 'producer' not in self.processes:
            if self.producer_completed:
                self.log("PRODUCER: DATASET TAMAMLANDI", "SUCCESS")
            else:
                self.log("PRODUCER: BAŞLATILMADI VEYA DURDU", "WARNING")
        
        # Resource kullanımı
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            
            self.log(f"CPU Kullanımı: {cpu_percent}%", "INFO")
            self.log(f"RAM Kullanımı: {memory.percent}% ({memory.used // 1024 // 1024} MB / {memory.total // 1024 // 1024} MB)", "INFO")
        except:
            self.log("Resource bilgileri alınamadı", "WARNING")
        
        # Restart bilgileri
        if self.producer_restart_count > 0:
            self.log(f"Producer restart sayısı: {self.producer_restart_count}/{self.max_producer_restarts}", "INFO")
        
        # URL'ler
        self.log("=== ERİŞİM URL'LERİ ===", "INFO")
        self.log("🎵 Dashboard: http://localhost:8050", "INFO")
        self.log("🔧 Kafka UI: http://localhost:8080", "INFO")
        self.log("🗄️ pgAdmin: http://localhost:5050", "INFO")
        
        print()
    
    def restart_producer_manual(self):
        """Manuel producer restart"""
        if 'producer' in self.processes:
            self.log("Mevcut producer kapatılıyor...", "INFO")
            self.processes['producer'].terminate()
            time.sleep(3)
            if self.processes['producer'].poll() is None:
                self.processes['producer'].kill()
            del self.processes['producer']
        
        self.producer_completed = False
        self.producer_restart_count = 0
        self.start_producer()
    
    def signal_handler(self, signum, frame):
        """Graceful shutdown"""
        self.log("Shutdown signal alındı, sistem kapatılıyor...", "WARNING")
        self.running = False
        
        for name, process in self.processes.items():
            if process.poll() is None:
                self.log(f"{name} kapatılıyor...", "INFO")
                process.terminate()
                
                # 5 saniye bekle, sonra force kill
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.log(f"{name} force kill ediliyor...", "WARNING")
                    process.kill()
        
        self.log("Sistem kapatıldı", "SUCCESS")
        sys.exit(0)
    
    def run(self):
        """Ana çalıştırma fonksiyonu"""
        # Signal handler'ları ayarla
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        print("🎵" + "="*80 + "🎵")
        print("    🚀 SPOTIFY TIMELESS ANALYTICS SYSTEM RUNNER 🚀")
        print("🎵" + "="*80 + "🎵")
        print()
        
        # Gereksinimleri kontrol et
        if not self.check_requirements():
            self.log("Gereksinimler karşılanmadı, çıkılıyor...", "ERROR")
            return
        
        # Servisleri sırayla başlat
        self.log("Servisler başlatılıyor...", "INFO")
        time.sleep(2)
        
        # 1. Producer
        if not self.start_producer():
            return
        time.sleep(5)
        
        # 2. Consumer 
        if not self.start_consumer():
            return
        time.sleep(5)
        
        # 3. Dashboard
        if not self.start_dashboard():
            return
        time.sleep(3)
        
        # Durum göster
        self.show_system_status()
        
        # Monitoring başlat
        monitor_thread = threading.Thread(target=self.monitor_processes, daemon=True)
        monitor_thread.start()
        
        self.log("🎉 Tüm servisler başarıyla başlatıldı!", "SUCCESS")
        self.log("📊 Dataset completion otomatik detect edilecek", "INFO")
        self.log("⏹️ Durdurmak için Ctrl+C'ye basın", "INFO")
        print("=" * 80)
        
        # Ana loop - kullanıcı input'u bekle
        try:
            while self.running:
                user_input = input("Komut (status/restart-producer/help/quit): ").strip().lower()
                
                if user_input == 'quit' or user_input == 'q':
                    break
                elif user_input == 'status' or user_input == 's':
                    self.show_system_status()
                elif user_input == 'restart-producer' or user_input == 'rp':
                    self.restart_producer_manual()
                elif user_input == 'help' or user_input == 'h':
                    print("Mevcut komutlar:")
                    print("  status           - Sistem durumunu göster")
                    print("  restart-producer - Producer'ı manuel restart et")
                    print("  help             - Bu yardım mesajını göster")
                    print("  quit             - Sistemi kapat")
                elif user_input == '':
                    continue
                else:
                    print(f"Bilinmeyen komut: {user_input}")
                    
        except EOFError:
            pass
        
        # Temizlik
        self.signal_handler(None, None)

if __name__ == "__main__":
    runner = SpotifySystemRunner()
    runner.run()