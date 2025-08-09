import pandas as pd
import json
import time
from confluent_kafka import Producer
from datetime import datetime, timedelta
import os
import random
import logging
import threading
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SpotifyDataStreamer:
    def __init__(self, kafka_bootstrap_servers='localhost:9092'):
        self.producer_config = {
            'bootstrap.servers': kafka_bootstrap_servers,
            'client.id': 'spotify-producer',
            'compression.type': 'gzip',
            'batch.size': 16384,
            'linger.ms': 10,
            'retries': 5,
            'acks': 'all'
        }
        
        self.producer = Producer(self.producer_config)
        
        self.topics = {
            'historical_stream': 'spotify-historical-stream',
            'real_time_events': 'spotify-realtime-events',
            'analytics_results': 'spotify-analytics-results'
        }
        
        # Timeout sistemi için
        self.active_events = {}  # event_id -> event_data
        self.timeout_duration = 30  # 30 saniye
        self.running = True
        
        self.data = None
        self.load_data()
        
        # Timeout checker thread başlat
        self.timeout_thread = threading.Thread(target=self.timeout_checker, daemon=True)
        self.timeout_thread.start()
        
    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        
    def load_data(self):
        data_dir = './data'
        
        logger.info(f"Loading dataset from: {data_dir}")
        
        csv_files = []
        for file in os.listdir(data_dir):
            if file.endswith('.csv'):
                csv_files.append(os.path.join(data_dir, file))
        
        csv_path = csv_files[0]
        logger.info(f"CSV file found: {csv_path}")
        
        try:
            self.data = pd.read_csv(csv_path)
            logger.info(f"Dataset loaded: {len(self.data)} records, {len(self.data.columns)} columns")
            
            self.prepare_data()
            
        except Exception as e:
            raise Exception(f"CSV read error: {e}\n"
                          f"File may be corrupted, try downloading again")
    
    def prepare_data(self):
        try:
            column_mapping = {
                'Index': 'index',
                'Title': 'title',
                'Artist': 'artist',
                'Top Genre': 'top_genre',
                'Year': 'year',
                'Beats Per Minute (BPM)': 'bpm',
                'Energy': 'energy',
                'Danceability': 'danceability',
                'Loudness (dB)': 'loudness',
                'Liveness': 'liveness',
                'Valence': 'valence',
                'Length (Duration)': 'length',
                'Acousticness': 'acousticness',
                'Speechiness': 'speechiness',
                'Popularity': 'popularity'
            }
            
            self.data = self.data.rename(columns=column_mapping)
            logger.info(f"Column names standardized")
            
            self.data = self.data.dropna()
            
            numeric_columns = ['year', 'bpm', 'energy', 'danceability', 'loudness', 
                             'liveness', 'valence', 'length', 'acousticness', 'speechiness', 'popularity']
            
            for col in numeric_columns:
                if col in self.data.columns:
                    self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
            
            string_columns = ['title', 'artist', 'top_genre']
            for col in string_columns:
                if col in self.data.columns:
                    self.data[col] = self.data[col].astype(str).str.strip()
            
            if 'year' in self.data.columns:
                self.data = self.data.sort_values('year').reset_index(drop=True)
            
            logger.info(f"Data prepared: {len(self.data)} records, {len(self.data.columns)} columns")
            logger.info(f"Year range: {self.data['year'].min()} - {self.data['year'].max()}")
            logger.info(f"Unique artists: {self.data['artist'].nunique()}")
            logger.info(f"Unique genres: {self.data['top_genre'].nunique()}")
            
        except Exception as e:
            logger.error(f"Data preparation error: {e}")
            raise
    
    def create_song_event(self, row, event_type='historical'):
        def safe_int(value, default=0):
            try:
                if pd.isna(value) or value == '' or value is None:
                    return default
                return int(float(value))
            except (ValueError, TypeError):
                return default
        
        def safe_str(value, default='Unknown'):
            try:
                if pd.isna(value) or value == '' or value is None:
                    return default
                return str(value).strip()
            except:
                return default
        
        return {
            'event_type': event_type,
            'timestamp': datetime.now().isoformat(),
            'song_data': {
                'title': safe_str(row['title']),
                'artist': safe_str(row['artist']),
                'top_genre': safe_str(row['top_genre']),
                'year': safe_int(row['year'], 2000),
                'bpm': safe_int(row['bpm'], 120),
                'energy': safe_int(row['energy'], 50),
                'danceability': safe_int(row['danceability'], 50),
                'loudness': safe_int(row['loudness'], -10),
                'liveness': safe_int(row['liveness'], 10),
                'valence': safe_int(row['valence'], 50),
                'length': safe_int(row['length'], 180000),
                'acousticness': safe_int(row['acousticness'], 20),
                'speechiness': safe_int(row['speechiness'], 5),
                'popularity': safe_int(row['popularity'], 50)
            },
            'metadata': {
                'processing_time': datetime.now().isoformat(),
                'source': 'spotify_data'
            }
        }
    
    def send_message(self, topic, key, value):
        try:
            self.producer.produce(
                topic=topic,
                key=str(key) if key else None,
                value=json.dumps(value, ensure_ascii=False).encode('utf-8'),
                callback=self.delivery_report
            )
        except Exception as e:
            logger.error(f"Message send error: {e}")
    
    def timeout_checker(self):
        """30 saniye sonra real-time event'leri timeout et ve rollback gönder"""
        logger.info("Timeout checker started")
        
        while self.running:
            try:
                current_time = datetime.now()
                expired_events = []
                
                # Expired event'leri bul
                for event_id, event_data in self.active_events.items():
                    if current_time >= event_data['expire_time']:
                        expired_events.append(event_id)
                
                # Expired event'leri işle
                for event_id in expired_events:
                    event_data = self.active_events.pop(event_id)
                    self.send_timeout_rollback(event_data)
                
                time.sleep(1)  # Her saniye kontrol et
                
            except Exception as e:
                logger.error(f"Timeout checker error: {e}")
                time.sleep(5)
    
    def send_timeout_rollback(self, original_event):
        """Timeout olan event için rollback gönder"""
        try:
            rollback_event = {
                'event_type': 'timeout_rollback',
                'timestamp': datetime.now().isoformat(),
                'original_event_id': original_event['event_id'],
                'song_data': original_event['original_song_data'],  # Orijinal değerler
                'metadata': {
                    'processing_time': datetime.now().isoformat(),
                    'source': 'timeout_system',
                    'original_boost': original_event['boost_amount'],
                    'rollback_reason': 'timeout_expired',
                    'duration_seconds': self.timeout_duration
                }
            }
            
            self.send_message(
                topic=self.topics['real_time_events'],
                key=f"timeout_{original_event['event_id']}",
                value=rollback_event
            )
            
            song_title = original_event['original_song_data']['title']
            artist = original_event['original_song_data']['artist']
            boost = original_event['boost_amount']
            
            logger.info(f"TIMEOUT ROLLBACK: '{song_title}' by {artist} - boost {boost} expired")
            
        except Exception as e:
            logger.error(f"Rollback send error: {e}")
    
    def simulate_real_time_events(self):
        random_song = self.data.sample(1).iloc[0]
        
        popularity_boost = random.randint(15, 40)
        boosted_song = random_song.copy()
        boosted_song['popularity'] = min(100, boosted_song['popularity'] + popularity_boost)
        
        # Unique event ID
        event_id = str(uuid.uuid4())
        expire_time = datetime.now() + timedelta(seconds=self.timeout_duration)
        
        # Boosted event oluştur
        event = self.create_song_event(boosted_song, 'popularity_surge')
        event['metadata']['event_id'] = event_id
        event['metadata']['boost_amount'] = popularity_boost
        event['metadata']['reason'] = random.choice([
            'viral_tiktok', 'playlist_feature', 'artist_collab', 'radio_play', 'meme_trend', 'influencer_post'
        ])
        event['metadata']['original_popularity'] = int(random_song['popularity'])
        event['metadata']['new_popularity'] = int(boosted_song['popularity'])
        event['metadata']['expire_time'] = expire_time.isoformat()
        event['metadata']['timeout_duration'] = self.timeout_duration
        event['metadata']['has_timeout'] = True
        
        # Active event'leri track et
        self.active_events[event_id] = {
            'event_id': event_id,
            'expire_time': expire_time,
            'boost_amount': popularity_boost,
            'original_song_data': self.create_song_event(random_song, 'original')['song_data'],  # Orijinal değerler
            'boosted_song_data': event['song_data'],  # Boosted değerler
            'created_time': datetime.now()
        }
        
        logger.info(f"Real-time event created: {event_id} - expires in {self.timeout_duration}s")
        
        return event
    
    def stream_historical_data(self, seconds_per_year=2, batch_size=25):
        """Tüm yılları baştan sona Kafka'ya gönderir (tüm dataset)."""
        yearly_data = self.data.groupby('year')
        total_songs = len(self.data)
        processed = 0
    
        for year, year_df in yearly_data:
            start_time = time.time()
            year_df = year_df.reset_index(drop=True)
    
            for i in range(0, len(year_df), batch_size):
                batch = year_df.iloc[i:i+batch_size]
                for _, song in batch.iterrows():
                    try:
                        event = self.create_song_event(song, 'historical')
                        self.send_message(
                            topic=self.topics['historical_stream'],
                            key=f"{song['artist']}_{song['title']}",
                            value=event
                        )
                        processed += 1
                    except Exception as e:
                        logger.error(f"Song send error: {e}")
    
                self.producer.poll(0)
    
            # Gerçek zamanlı event (10% olasılık)
            if random.random() < 0.10:
                try:
                    rt_event = self.simulate_real_time_events()
                    self.send_message(
                        topic=self.topics['real_time_events'],
                        key=rt_event['metadata']['event_id'],
                        value=rt_event
                    )
                except Exception as e:
                    logger.error(f"Real-time event error: {e}")
    
            # Yıllar arası bekleme
            elapsed = time.time() - start_time
            if elapsed < seconds_per_year:
                time.sleep(seconds_per_year - elapsed)
    
        self.producer.flush()
        logger.info(f"Streaming completed: {processed}/{total_songs} songs sent.")


    
    def close(self):
        logger.info("Stopping producer and timeout system...")
        self.running = False
        
        # Kalan active event'ler için rollback gönder
        for event_id, event_data in list(self.active_events.items()):
            self.send_timeout_rollback(event_data)
        
        self.producer.flush()
        logger.info("Producer closed with all rollbacks sent")

if __name__ == "__main__":
    if not os.path.exists('./data'):
        print("'./data' directory not found!")
        print("\nDATASET SETUP GUIDE:")
        print("1. mkdir data")
        print("2. Go to: https://www.kaggle.com/datasets/iamsumat/spotify-top-2000s-mega-dataset")
        print("3. Click 'Download'")
        print("4. Extract ZIP to './data/' directory")
        print("5. Run producer again")
        exit(1)
    
    try:
        print("SPOTIFY REAL-TIME ANALYTICS PRODUCER")
        print("=" * 50)
        
        logger.info("Spotify Data Streamer starting...")
        streamer = SpotifyDataStreamer()
        
        print(f"Dataset loaded and prepared!")
        print(f"Total songs: {len(streamer.data)}")
        print(f"Year range: {streamer.data['year'].min()} - {streamer.data['year'].max()}")
        print(f"Artists: {streamer.data['artist'].nunique()}")
        print(f"Genres: {streamer.data['top_genre'].nunique()}")
        print()
        
        print("Historical Data Replay starting...")
        print("2000s music data streaming with time compression...")
        print("Time compression: 2000x (1 year = ~3 seconds)")
        print("Real-time events randomly injected...")
        print("TIMEOUT SYSTEM: Real-time events expire after 30 seconds")
        print("Popularity boosts automatically rollback on timeout")
        print()
        print("Press Ctrl+C to stop")
        print("-" * 50)
        
        # Faster streaming - 3 seconds per year (down from 5)
        streamer.stream_historical_data(seconds_per_year=3, batch_size=15)
        
    except Exception as e:
        logger.error(f"Main error: {e}")
        print(f"\n{e}")
        
    except KeyboardInterrupt:
        print("\nStreaming stopped.")
    finally:
        try:
            streamer.close()
        except:
            pass
        print("Producer closed.")