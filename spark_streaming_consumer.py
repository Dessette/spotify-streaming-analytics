from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, udf
from pyspark.sql.types import *
from confluent_kafka import Consumer, KafkaError, Producer
import json
import logging
import threading
import time
import pandas as pd
from collections import deque, defaultdict
from datetime import datetime, timedelta
import traceback

from timeless_genre_analytics import TimelessGenreAnalyzer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SpotifyStreamingAnalyzer:
    def __init__(self, kafka_bootstrap_servers='localhost:9092'):
        self.kafka_servers = kafka_bootstrap_servers
        self.spark = None
        self.running = True
        
        # Küçültülmüş buffer'lar
        self.analytics_buffer = {
            'genre_trends': deque(maxlen=100),
            'popularity_trends': deque(maxlen=200),
            'real_time_events': deque(maxlen=50),
            'audio_features': deque(maxlen=150),
            'artist_stats': deque(maxlen=100)
        }
        
        # Timeout sistemi için active event tracking
        self.active_real_time_events = {}  # event_id -> event_data
        self.timeout_stats = {
            'total_events': 0,
            'active_events': 0,
            'expired_events': 0,
            'rollback_processed': 0
        }
        
        self.timeless_analyzer = TimelessGenreAnalyzer(analysis_periods=[5, 10])
        
        self.metrics = {
            'messages_processed': 0,
            'last_update': datetime.now(),
            'processing_rate': 0.0,
            'start_time': datetime.now(),
            'timeless_analyses': 0,
            'timeless_last_update': None
        }
        
        self.producer_config = {
            'bootstrap.servers': self.kafka_servers,
            'client.id': 'spotify-analytics-producer',
            'compression.type': 'gzip'
        }
        self.producer = Producer(self.producer_config)
        
        self.setup_spark()
        self.setup_kafka_consumer()
        
    def setup_spark(self):
        self.spark = SparkSession.builder \
            .appName("SpotifyTimelessAnalytics") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark Session created")
    
    def setup_kafka_consumer(self):
        self.consumer_config = {
            'bootstrap.servers': self.kafka_servers,
            'group.id': 'spotify-analytics-group',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000
        }
        
        self.consumer = Consumer(self.consumer_config)
        topics = ['spotify-historical-stream', 'spotify-realtime-events']
        self.consumer.subscribe(topics)
        
        logger.info(f"Kafka Consumer created: {topics}")
    
    def process_song_data_enhanced(self, message_data):
        try:
            song_data = message_data.get('song_data', {})
            event_type = message_data.get('event_type', 'unknown')
            timestamp = datetime.now()
            
            self.process_standard_analytics(song_data, event_type, timestamp)
            self.timeless_analyzer.add_song_data(song_data)
            
            # Her 20 şarkıda bir timeless analizi (50'den azaltıldı)
            if self.metrics['messages_processed'] % 20 == 0:
                self.run_timeless_analysis()
                
        except Exception as e:
            logger.error(f"Song data processing error: {e}")
    
    def process_standard_analytics(self, song_data, event_type, timestamp):
        genre_data = {
            'timestamp': timestamp,
            'genre': song_data.get('top_genre', 'unknown'),
            'popularity': song_data.get('popularity', 0),
            'event_type': event_type
        }
        self.analytics_buffer['genre_trends'].append(genre_data)
        
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
        
        artist_data = {
            'timestamp': timestamp,
            'artist': song_data.get('artist', 'unknown'),
            'popularity': song_data.get('popularity', 0),
            'genre': song_data.get('top_genre', 'unknown'),
            'year': song_data.get('year', 2000)
        }
        self.analytics_buffer['artist_stats'].append(artist_data)
        
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
    
    def run_timeless_analysis(self):
        try:
            results = self.timeless_analyzer.analyze_genre_timelessness()
            
            if results:
                top_timeless = self.timeless_analyzer.get_top_timeless_genres(10)
                summary = self.timeless_analyzer.get_analytics_summary()
                trending_changes = self.timeless_analyzer.get_trending_timeless_changes()
                
                analytics_result = {
                    'analysis_type': 'timeless_genre_analysis',
                    'timestamp': datetime.now().isoformat(),
                    'top_timeless_genres': [
                        {
                            'rank': i + 1,
                            'genre': genre,
                            'timeless_score': round(metric.timeless_score, 2),
                            'consistency_score': round(metric.consistency_score, 2),
                            'longevity_score': round(metric.longevity_score, 2),
                            'decade_presence': metric.decade_presence,
                            'peak_stability': round(metric.peak_stability, 2)
                        }
                        for i, (genre, metric) in enumerate(top_timeless)
                    ],
                    'summary_statistics': summary,
                    'trending_changes': trending_changes,
                    'analysis_metadata': {
                        'total_genres_analyzed': len(results),
                        'total_data_points': len(self.timeless_analyzer.genre_data_buffer),
                        'analysis_periods_used': self.timeless_analyzer.analysis_periods
                    }
                }
                
                self.send_analytics_result(analytics_result)
                
                self.metrics['timeless_analyses'] += 1
                self.metrics['timeless_last_update'] = datetime.now()
                
                # Sadece top 3 log
                for i, (genre, metric) in enumerate(top_timeless[:3], 1):
                    logger.info(f"Top {i}: {genre}: {metric.timeless_score:.1f}/100")
                
                if trending_changes:
                    for change in trending_changes[:2]:
                        logger.info(f"{change['genre']}: {change['change_text']}")
                
        except Exception as e:
            logger.error(f"Timeless analysis error: {e}")
    
    def send_analytics_result(self, result):
        try:
            self.producer.produce(
                topic='spotify-analytics-results',
                key='timeless_analysis',
                value=json.dumps(result, ensure_ascii=False).encode('utf-8')
            )
            self.producer.poll(0)
            
        except Exception as e:
            logger.error(f"Analytics result send error: {e}")
    
    def process_real_time_event_enhanced(self, message_data):
        try:
            event_type = message_data.get('event_type', 'unknown')
            
            if event_type == 'timeout_rollback':
                self.handle_timeout_rollback(message_data)
            else:
                self.handle_regular_real_time_event(message_data)
                
        except Exception as e:
            logger.error(f"Real-time event processing error: {e}")
    
    def handle_regular_real_time_event(self, message_data):
        """Normal real-time event'leri işle"""
        try:
            event_data = {
                'timestamp': datetime.now(),
                'event_type': message_data.get('event_type', 'unknown'),
                'song_data': message_data.get('song_data', {}),
                'metadata': message_data.get('metadata', {}),
                'status': 'active'
            }
            
            # Event ID varsa active event'lere ekle
            event_id = event_data['metadata'].get('event_id')
            if event_id and event_data['metadata'].get('has_timeout'):
                self.active_real_time_events[event_id] = {
                    'event_data': event_data,
                    'song_data': event_data['song_data'],
                    'created_time': datetime.now(),
                    'status': 'active'
                }
                self.timeout_stats['total_events'] += 1
                self.timeout_stats['active_events'] = len(self.active_real_time_events)
            
            self.analytics_buffer['real_time_events'].append(event_data)
            self.analyze_real_time_impact(event_data)
            
            song_title = event_data['song_data'].get('title', 'Unknown')
            artist = event_data['song_data'].get('artist', 'Unknown') 
            reason = event_data['metadata'].get('reason', 'unknown')
            boost = event_data['metadata'].get('boost_amount', 0)
            event_short_id = event_id[:8] if event_id else 'no-id'
            
            logger.info(f"Real-time Event [{event_short_id}]: '{song_title}' by {artist} - {reason} (+{boost})")
            
        except Exception as e:
            logger.error(f"Regular real-time event error: {e}")
    
    def handle_timeout_rollback(self, message_data):
        """Timeout rollback event'lerini işle"""
        try:
            original_event_id = message_data.get('original_event_id')
            rollback_song_data = message_data.get('song_data', {})
            rollback_metadata = message_data.get('metadata', {})
            
            if original_event_id and original_event_id in self.active_real_time_events:
                # Original event'i kapat
                original_event = self.active_real_time_events.pop(original_event_id)
                original_event['status'] = 'expired'
                
                # Rollback event'i oluştur
                rollback_event = {
                    'timestamp': datetime.now(),
                    'event_type': 'timeout_rollback',
                    'song_data': rollback_song_data,
                    'metadata': rollback_metadata,
                    'original_event_id': original_event_id,
                    'status': 'rollback'
                }
                
                self.analytics_buffer['real_time_events'].append(rollback_event)
                
                # Timeless analyzer'dan boosted veriyi çıkar, orijinal veriyi ekle
                self.rollback_timeless_data(original_event['song_data'], rollback_song_data)
                
                # Stats güncelle
                self.timeout_stats['expired_events'] += 1
                self.timeout_stats['rollback_processed'] += 1
                self.timeout_stats['active_events'] = len(self.active_real_time_events)
                
                song_title = rollback_song_data.get('title', 'Unknown')
                artist = rollback_song_data.get('artist', 'Unknown')
                original_boost = rollback_metadata.get('original_boost', 0)
                event_short_id = original_event_id[:8]
                
                logger.info(f"ROLLBACK [{event_short_id}]: '{song_title}' by {artist} - boost {original_boost} expired")
                
        except Exception as e:
            logger.error(f"Timeout rollback error: {e}")
    
    def rollback_timeless_data(self, boosted_song_data, original_song_data):
        """Timeless analyzer'dan boosted veriyi çıkar, orijinal veriyi ekle"""
        try:
            # Bu işlem için timeless analyzer'ın buffer'ından boosted veriyi bul ve çıkar
            buffer = self.timeless_analyzer.genre_data_buffer
            
            # Boosted şarkıyı bul ve çıkar (title + artist match)
            boosted_title = boosted_song_data.get('title')
            boosted_artist = boosted_song_data.get('artist')
            
            # Buffer'dan boosted versiyonu çıkar (son eklenen olması muhtemel)
            for i in range(len(buffer) - 1, -1, -1):
                song = buffer[i]
                if (song.get('title') == boosted_title and 
                    song.get('artist') == boosted_artist and
                    song.get('event_type') in ['popularity_surge', 'historical']):
                    # Boosted version'u kaldır
                    del buffer[i]
                    break
            
            # Orijinal şarkıyı ekle
            original_processed = {
                'timestamp': datetime.now(),
                'title': original_song_data.get('title', 'Unknown'),
                'artist': original_song_data.get('artist', 'Unknown'),
                'genre': original_song_data.get('top_genre', 'unknown'),
                'year': int(original_song_data.get('year', 2000)),
                'popularity': float(original_song_data.get('popularity', 0)),
                'energy': float(original_song_data.get('energy', 0)),
                'danceability': float(original_song_data.get('danceability', 0)),
                'valence': float(original_song_data.get('valence', 0)),
                'event_type': 'rollback_original'
            }
            
            buffer.append(original_processed)
            
            logger.info(f"Timeless data rollback: {boosted_title} popularity reverted")
            
        except Exception as e:
            logger.error(f"Timeless rollback error: {e}")
    
    def analyze_real_time_impact(self, event_data):
        try:
            song_genre = event_data['song_data'].get('top_genre', 'unknown')
            boost_amount = event_data['metadata'].get('boost_amount', 0)
            
            if song_genre in self.timeless_analyzer.timeless_metrics:
                current_metric = self.timeless_analyzer.timeless_metrics[song_genre]
                
                impact_prediction = {
                    'genre': song_genre,
                    'current_timeless_score': current_metric.timeless_score,
                    'boost_amount': boost_amount,
                    'predicted_impact': 'positive' if boost_amount > 20 else 'neutral',
                    'consistency_risk': 'high' if boost_amount > 40 else 'low',
                    'timestamp': datetime.now().isoformat()
                }
                
                logger.info(f"Impact: {song_genre} - {impact_prediction['predicted_impact']}")
                
        except Exception as e:
            logger.error(f"Real-time impact analysis error: {e}")
    
    def generate_enhanced_analytics(self):
        try:
            self.generate_standard_windowed_analytics()
            self.generate_timeless_insights()
            
        except Exception as e:
            logger.error(f"Analytics generation error: {e}")
    
    def generate_standard_windowed_analytics(self):
        try:
            current_time = datetime.now()
            recent_genres = [
                g for g in self.analytics_buffer['genre_trends'] 
                if (current_time - g['timestamp']).total_seconds() < 300
            ]
            
            if recent_genres:
                genre_stats = defaultdict(lambda: {'count': 0, 'total_popularity': 0})
                
                for genre_data in recent_genres:
                    genre = genre_data['genre']
                    popularity = genre_data.get('popularity', 0)
                    if isinstance(popularity, (int, float)) and not pd.isna(popularity):
                        genre_stats[genre]['count'] += 1
                        genre_stats[genre]['total_popularity'] += popularity
                
                top_genres = []
                for genre, stats in genre_stats.items():
                    if stats['count'] > 0:
                        avg_popularity = stats['total_popularity'] / stats['count']
                        top_genres.append({
                            'genre': genre,
                            'count': stats['count'],
                            'avg_popularity': round(avg_popularity, 2)
                        })
                
                top_genres = sorted(top_genres, key=lambda x: x['avg_popularity'], reverse=True)[:5]
                
                if top_genres:
                    logger.info("Recent Genre Trends:")
                    for i, genre_stat in enumerate(top_genres, 1):
                        logger.info(f"  {i}. {genre_stat['genre']}: {genre_stat['avg_popularity']}")
                        
        except Exception as e:
            logger.error(f"Standard analytics error: {e}")
    
    def generate_timeless_insights(self):
        try:
            summary = self.timeless_analyzer.get_analytics_summary()
            
            if summary and summary.get('top_timeless_genres'):
                champion = summary['top_timeless_genres'][0]
                logger.info(f"Timeless Champion: {champion['genre']} ({champion['score']}/100)")
                
                stats = summary.get('statistics', {})
                logger.info(f"Analytics: {stats.get('total_genres_analyzed', 0)} genres, "
                          f"Avg: {stats.get('avg_timeless_score', 0):.1f}, "
                          f"Max: {stats.get('max_timeless_score', 0):.1f}")
                
                # Timeout sistem durumu
                logger.info(f"Timeout System: {self.timeout_stats['active_events']} active, "
                          f"{self.timeout_stats['expired_events']} expired, "
                          f"{self.timeout_stats['rollback_processed']} rollbacks")
                        
        except Exception as e:
            logger.error(f"Timeless insights error: {e}")
    
    def update_performance_metrics(self):
        self.metrics['messages_processed'] += 1
        
        if self.metrics['messages_processed'] % 100 == 0:
            now = datetime.now()
            time_diff = (now - self.metrics['last_update']).total_seconds()
            
            if time_diff > 0:
                self.metrics['processing_rate'] = 100 / time_diff
            
            self.metrics['last_update'] = now
            
            uptime = (now - self.metrics['start_time']).total_seconds()
            timeless_info = ""
            if self.metrics['timeless_last_update']:
                last_timeless = (now - self.metrics['timeless_last_update']).total_seconds()
                timeless_info = f", last timeless: {last_timeless:.0f}s ago"
            
            logger.info(f"Performance: {self.metrics['messages_processed']} msgs, "
                       f"{self.metrics['processing_rate']:.1f} msgs/sec, "
                       f"uptime: {uptime:.0f}s, "
                       f"timeless analyses: {self.metrics['timeless_analyses']}"
                       f"{timeless_info}, "
                       f"active events: {self.timeout_stats['active_events']}, "
                       f"expired: {self.timeout_stats['expired_events']}")
    
    def kafka_consumer_loop_enhanced(self):
        logger.info("Kafka consumer loop started")
        
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
                    message_data = json.loads(msg.value().decode('utf-8'))
                    topic = msg.topic()
                    
                    if topic == 'spotify-historical-stream':
                        self.process_song_data_enhanced(message_data)
                    elif topic == 'spotify-realtime-events':
                        self.process_real_time_event_enhanced(message_data)
                    
                    self.update_performance_metrics()
                    
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}")
                except Exception as e:
                    logger.error(f"Message processing error: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Consumer loop interrupted")
        finally:
            self.consumer.close()
    
    def enhanced_analytics_loop(self):
        logger.info("Analytics loop started")
        
        while self.running:
            try:
                time.sleep(30)  # 45s -> 30s
                if not self.running:
                    break
                    
                self.generate_enhanced_analytics()
                
                # Her 3 dakikada bir timeless analizi (5 dakikadan azaltıldı)
                if self.metrics['messages_processed'] % 150 == 0:
                    self.run_timeless_analysis()
                
            except Exception as e:
                logger.error(f"Analytics loop error: {e}")
                time.sleep(10)
    
    def start_enhanced_streaming_analytics(self):
        logger.info("Spotify Streaming Analytics starting")
        
        consumer_thread = threading.Thread(target=self.kafka_consumer_loop_enhanced, daemon=True)
        consumer_thread.start()
        
        analytics_thread = threading.Thread(target=self.enhanced_analytics_loop, daemon=True)
        analytics_thread.start()
        
        logger.info("Streaming analytics active")
        logger.info("Timeless genre analysis integrated")
        
        return [consumer_thread, analytics_thread]
    
    def get_enhanced_analytics(self):
        standard_analytics = {
            'genre_trends': list(self.analytics_buffer['genre_trends']),
            'popularity_trends': list(self.analytics_buffer['popularity_trends']),
            'real_time_events': list(self.analytics_buffer['real_time_events']),
            'audio_features': list(self.analytics_buffer['audio_features']),
            'artist_stats': list(self.analytics_buffer['artist_stats']),
            'metrics': self.metrics.copy()
        }
        
        timeless_analytics = {
            'timeless_summary': self.timeless_analyzer.get_analytics_summary(),
            'top_timeless_genres': [
                {
                    'genre': genre,
                    'timeless_score': metric.timeless_score,
                    'consistency_score': metric.consistency_score,
                    'longevity_score': metric.longevity_score,
                    'decade_presence': metric.decade_presence
                }
                for genre, metric in self.timeless_analyzer.get_top_timeless_genres(10)
            ],
            'trending_changes': self.timeless_analyzer.get_trending_timeless_changes()
        }
        
        return {
            'standard_analytics': standard_analytics,
            'timeless_analytics': timeless_analytics,
            'combined_metrics': {
                'total_messages': self.metrics['messages_processed'],
                'timeless_analyses': self.metrics['timeless_analyses'],
                'genres_analyzed': len(self.timeless_analyzer.timeless_metrics),
                'data_points': len(self.timeless_analyzer.genre_data_buffer)
            },
            'timeout_system': {
                'total_events': self.timeout_stats['total_events'],
                'active_events': self.timeout_stats['active_events'],
                'expired_events': self.timeout_stats['expired_events'],
                'rollback_processed': self.timeout_stats['rollback_processed'],
                'active_event_details': [
                    {
                        'event_id': eid[:8],
                        'title': data['song_data'].get('title', 'Unknown'),
                        'artist': data['song_data'].get('artist', 'Unknown'),
                        'created_time': data['created_time'].isoformat(),
                        'status': data['status']
                    }
                    for eid, data in self.active_real_time_events.items()
                ]
            }
        }
    
    def stop_enhanced_streams(self):
        logger.info("Stopping streaming analytics")
        
        self.running = False
        
        if hasattr(self, 'consumer'):
            self.consumer.close()
            
        if hasattr(self, 'producer'):
            self.producer.flush()
        
        if self.spark:
            self.spark.stop()
        
        logger.info("Resources cleaned")

if __name__ == "__main__":
    print("SPOTIFY REAL-TIME + TIMELESS ANALYTICS")
    print("=" * 60)
    
    analyzer = SpotifyStreamingAnalyzer()
    
    try:
        threads = analyzer.start_enhanced_streaming_analytics()
        
        print("Spotify Analytics Consumer active!")
        print("Standard real-time analytics active")
        print("Timeless genre analysis integrated")
        print("Real-time event impact analysis active")
        print("TIMEOUT SYSTEM: 30s auto-rollback for real-time events")
        print("Analytics every 30 seconds")
        print("Timeless rankings and trend changes")
        print("Analytics results in console + Kafka")
        print("Timeless Champion tracking active")
        print("Press Ctrl+C to stop")
        print("-" * 60)
        
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping Consumer...")
    finally:
        analyzer.stop_enhanced_streams()
        print("Spotify Analytics Consumer closed!")