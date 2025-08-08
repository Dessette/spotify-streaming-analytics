from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, udf
from pyspark.sql.types import *
from confluent_kafka import Consumer, KafkaError
import json
import logging
import threading
import time
import pandas as pd  # Pandas import eklendi
from collections import deque, defaultdict
from datetime import datetime, timedelta
import traceback

# Logging ayarları
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SpotifyStreamingAnalyzer:
    def __init__(self, kafka_bootstrap_servers='localhost:9092'):
        """
        Real-time Spotify verilerini analiz eden Confluent Kafka + Spark Streaming uygulaması
        """
        self.kafka_servers = kafka_bootstrap_servers
        self.spark = None
        self.running = True
        
        # Analiz sonuçlarını saklamak için
        self.analytics_buffer = {
            'genre_trends': deque(maxlen=200),
            'popularity_trends': deque(maxlen=500),
            'real_time_events': deque(maxlen=100),
            'audio_features': deque(maxlen=300),
            'artist_stats': deque(maxlen=200)
        }
        
        # Performance metrics
        self.metrics = {
            'messages_processed': 0,
            'last_update': datetime.now(),
            'processing_rate': 0.0,
            'start_time': datetime.now()
        }
        
        self.setup_spark()
        self.setup_kafka_consumer()
        
    def setup_spark(self):
        """Spark Session ve yapılandırma"""
        self.spark = SparkSession.builder \
            .appName("SpotifyStreamingAnalytics") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
        
        # Log seviyesini ayarla
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Spark Session oluşturuldu")
    
    def setup_kafka_consumer(self):
        """Confluent Kafka Consumer ayarları"""
        self.consumer_config = {
            'bootstrap.servers': self.kafka_servers,
            'group.id': 'spotify-analytics-group',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000
        }
        
        self.consumer = Consumer(self.consumer_config)
        
        # Subscribe to topics
        topics = ['spotify-historical-stream', 'spotify-realtime-events']
        self.consumer.subscribe(topics)
        
        logger.info(f"✅ Kafka Consumer oluşturuldu: {topics}")
    
    def process_song_data(self, message_data):
        """Şarkı verisini analiz et"""
        try:
            song_data = message_data.get('song_data', {})
            event_type = message_data.get('event_type', 'unknown')
            timestamp = datetime.now()
            
            # Genre trend analizi
            genre_data = {
                'timestamp': timestamp,
                'genre': song_data.get('top_genre', 'unknown'),
                'popularity': song_data.get('popularity', 0),
                'event_type': event_type
            }
            self.analytics_buffer['genre_trends'].append(genre_data)
            
            # Audio features analizi
            audio_features = {
                'timestamp': timestamp,
                'energy': song_data.get('energy', 0),
                'danceability': song_data.get('danceability', 0),
                'valence': song_data.get('valence', 0),
                'acousticness': song_data.get('acousticness', 0),
                'genre': song_data.get('top_genre', 'unknown'),
                'popularity': song_data.get('popularity', 0)
            }
            self.analytics_buffer['audio_features'].append(audio_features)
            
            # Artist stats
            artist_data = {
                'timestamp': timestamp,
                'artist': song_data.get('artist', 'unknown'),
                'popularity': song_data.get('popularity', 0),
                'genre': song_data.get('top_genre', 'unknown'),
                'year': song_data.get('year', 2000)
            }
            self.analytics_buffer['artist_stats'].append(artist_data)
            
            # Popularity timeline
            popularity_data = {
                'timestamp': timestamp,
                'popularity': song_data.get('popularity', 0),
                'title': song_data.get('title', 'unknown'),
                'artist': song_data.get('artist', 'unknown'),
                'genre': song_data.get('top_genre', 'unknown'),
                'year': song_data.get('year', 2000),
                'event_type': event_type
            }
            self.analytics_buffer['popularity_trends'].append(popularity_data)
            
        except Exception as e:
            logger.error(f"Song data processing error: {e}")
    
    def process_real_time_event(self, message_data):
        """Real-time event'leri işle"""
        try:
            event_data = {
                'timestamp': datetime.now(),
                'event_type': message_data.get('event_type', 'unknown'),
                'song_data': message_data.get('song_data', {}),
                'metadata': message_data.get('metadata', {})
            }
            
            self.analytics_buffer['real_time_events'].append(event_data)
            
            # Real-time event log
            song_title = event_data['song_data'].get('title', 'Unknown')
            artist = event_data['song_data'].get('artist', 'Unknown') 
            reason = event_data['metadata'].get('reason', 'unknown')
            boost = event_data['metadata'].get('boost_amount', 0)
            
            logger.info(f"🔥 Real-time Event: '{song_title}' by {artist} - {reason} (+{boost} popularity)")
            
        except Exception as e:
            logger.error(f"Real-time event processing error: {e}")
    
    def generate_windowed_analytics(self):
        """Windowed analytics üret"""
        try:
            current_time = datetime.now()
            
            # Son 5 dakika için genre analizi
            recent_genres = [
                g for g in self.analytics_buffer['genre_trends'] 
                if (current_time - g['timestamp']).total_seconds() < 300
            ]
            
            if recent_genres:
                genre_stats = defaultdict(lambda: {'count': 0, 'total_popularity': 0})
                
                for genre_data in recent_genres:
                    genre = genre_data['genre']
                    # Güvenli popularity değeri al
                    popularity = genre_data.get('popularity', 0)
                    if isinstance(popularity, (int, float)) and not pd.isna(popularity):
                        genre_stats[genre]['count'] += 1
                        genre_stats[genre]['total_popularity'] += popularity
                
                # Top 5 genre'yi hesapla
                top_genres = []
                for genre, stats in genre_stats.items():
                    if stats['count'] > 0:  # Division by zero kontrolü
                        avg_popularity = stats['total_popularity'] / stats['count']
                        top_genres.append({
                            'genre': genre,
                            'count': stats['count'],
                            'avg_popularity': round(avg_popularity, 2),
                            'total_popularity': stats['total_popularity']
                        })
                
                top_genres = sorted(top_genres, key=lambda x: x['avg_popularity'], reverse=True)[:5]
                
                if top_genres:
                    logger.info("📊 Son 5 Dakika - Top Genres:")
                    for i, genre_stat in enumerate(top_genres, 1):
                        logger.info(f"   {i}. {genre_stat['genre']}: {genre_stat['avg_popularity']} avg popularity ({genre_stat['count']} songs)")
            
            # Son 2 dakika için audio features ortalaması
            recent_audio = [
                a for a in self.analytics_buffer['audio_features']
                if (current_time - a['timestamp']).total_seconds() < 120
            ]
            
            if recent_audio:
                # Güvenli ortalama hesaplama
                valid_audio = []
                for a in recent_audio:
                    if all(isinstance(a.get(key, 0), (int, float)) and not pd.isna(a.get(key, 0)) 
                           for key in ['energy', 'danceability', 'valence']):
                        valid_audio.append(a)
                
                if valid_audio:
                    avg_energy = sum(a['energy'] for a in valid_audio) / len(valid_audio)
                    avg_dance = sum(a['danceability'] for a in valid_audio) / len(valid_audio)
                    avg_valence = sum(a['valence'] for a in valid_audio) / len(valid_audio)
                    
                    logger.info(f"🎛️ Son 2 Dakika - Audio Features: Energy: {avg_energy:.1f}, Dance: {avg_dance:.1f}, Valence: {avg_valence:.1f}")
            
            # Real-time events summary
            recent_events = [
                e for e in self.analytics_buffer['real_time_events']
                if (current_time - e['timestamp']).total_seconds() < 180
            ]
            
            if recent_events:
                event_reasons = defaultdict(int)
                for event in recent_events:
                    reason = event.get('metadata', {}).get('reason', 'unknown')
                    event_reasons[reason] += 1
                
                logger.info(f"🚨 Son 3 Dakika - Real-time Events: {len(recent_events)} total")
                for reason, count in event_reasons.items():
                    logger.info(f"   • {reason}: {count}")
                    
        except Exception as e:
            logger.error(f"Analytics generation error: {e}")
            traceback.print_exc()
            # Hata durumunda basit bir mesaj ver
            logger.info("📊 Analytics temporary unavailable, continuing...")

    
    def update_performance_metrics(self):
        """Performance metriklerini güncelle"""
        self.metrics['messages_processed'] += 1
        
        if self.metrics['messages_processed'] % 100 == 0:
            now = datetime.now()
            time_diff = (now - self.metrics['last_update']).total_seconds()
            
            if time_diff > 0:
                self.metrics['processing_rate'] = 100 / time_diff
            
            self.metrics['last_update'] = now
            
            # Performance log
            uptime = (now - self.metrics['start_time']).total_seconds()
            logger.info(f"📈 Performance: {self.metrics['messages_processed']} msgs processed, "
                       f"{self.metrics['processing_rate']:.1f} msgs/sec, "
                       f"uptime: {uptime:.0f}s")
    
    def kafka_consumer_loop(self):
        """Kafka mesajlarını sürekli oku"""
        logger.info("🎵 Kafka consumer loop başladı...")
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        continue
                
                try:
                    # JSON parse et
                    message_data = json.loads(msg.value().decode('utf-8'))
                    topic = msg.topic()
                    
                    # Topic'e göre işle
                    if topic == 'spotify-historical-stream':
                        self.process_song_data(message_data)
                    elif topic == 'spotify-realtime-events':
                        self.process_real_time_event(message_data)
                    
                    # Metrics güncelle
                    self.update_performance_metrics()
                    
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}")
                except Exception as e:
                    logger.error(f"Message processing error: {e}")
                    
        except KeyboardInterrupt:
            logger.info("🛑 Consumer loop interrupted")
        finally:
            self.consumer.close()
    
    def analytics_loop(self):
        """Periyodik analytics raporu üret"""
        logger.info("📊 Analytics loop başladı...")
        
        while self.running:
            try:
                time.sleep(30)  # Her 30 saniyede bir analiz
                if not self.running:
                    break
                    
                self.generate_windowed_analytics()
                
            except Exception as e:
                logger.error(f"Analytics loop error: {e}")
                time.sleep(5)
    
    def start_streaming_analytics(self):
        """Ana streaming analiz fonksiyonu"""
        logger.info("🎵 Spotify Streaming Analytics başlıyor...")
        
        # Consumer thread'i başlat
        consumer_thread = threading.Thread(target=self.kafka_consumer_loop, daemon=True)
        consumer_thread.start()
        
        # Analytics thread'i başlat  
        analytics_thread = threading.Thread(target=self.analytics_loop, daemon=True)
        analytics_thread.start()
        
        logger.info("✅ Tüm streaming analizleri aktif!")
        
        return [consumer_thread, analytics_thread]
    
    def get_current_analytics(self):
        """Dashboard için mevcut analytics verilerini döndür"""
        return {
            'genre_trends': list(self.analytics_buffer['genre_trends']),
            'popularity_trends': list(self.analytics_buffer['popularity_trends']),
            'real_time_events': list(self.analytics_buffer['real_time_events']),
            'audio_features': list(self.analytics_buffer['audio_features']),
            'artist_stats': list(self.analytics_buffer['artist_stats']),
            'metrics': self.metrics.copy()
        }
    
    def stop_all_streams(self):
        """Tüm stream'leri durdur"""
        logger.info("🛑 Tüm streaming analizleri durduruluyor...")
        
        self.running = False
        
        if hasattr(self, 'consumer'):
            self.consumer.close()
        
        if self.spark:
            self.spark.stop()
        
        logger.info("✅ Tüm kaynaklar temizlendi")

# Kullanım
if __name__ == "__main__":
    print("🎵" + "="*60 + "🎵")
    print("    ⚡ SPOTIFY REAL-TIME ANALYTICS CONSUMER ⚡    ")
    print("🎵" + "="*60 + "🎵")
    print()
    
    analyzer = SpotifyStreamingAnalyzer()
    
    try:
        # Streaming analizlerini başlat
        threads = analyzer.start_streaming_analytics()
        
        print("✅ Spotify Real-time Analytics Consumer aktif!")
        print("📊 Kafka'dan gelen verileri real-time analiz ediyor...")
        print("🔥 Real-time event'leri takip ediyor...")
        print("📈 Her 30 saniyede windowed analytics üretiyor...")
        print()
        print("💡 Analytics sonuçları console'da görünecek")
        print("⏹️  Durdurmak için Ctrl+C'ye basın")
        print("-" * 60)
        
        # Ana thread'i canlı tut
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n⏹️  Consumer durduruluyor...")
    finally:
        analyzer.stop_all_streams()
        print("👋 Spotify Analytics Consumer kapandı!")