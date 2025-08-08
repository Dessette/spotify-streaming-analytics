import pandas as pd
import json
import time
from confluent_kafka import Producer
from datetime import datetime, timedelta
import os
import random
import logging

# Logging ayarları
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SpotifyDataStreamer:
    def __init__(self, kafka_bootstrap_servers='localhost:9092'):
        """
        Spotify verilerini Kafka'ya stream eden producer (Confluent Kafka)
        """
        # Confluent Kafka Producer config
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
        
        # Kafka topics
        self.topics = {
            'historical_stream': 'spotify-historical-stream',
            'real_time_events': 'spotify-realtime-events',
            'analytics_results': 'spotify-analytics-results'
        }
        
        self.data = None
        self.load_data()
        
    def delivery_report(self, err, msg):
        """Kafka delivery callback"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
        
    def load_data(self):
        """Spotify dataset'ini ./data klasöründen yükle"""
        data_dir = './data'
        
        logger.info(f"📁 Dataset aranıyor: {data_dir}")
        
        if not os.path.exists(data_dir):
            raise FileNotFoundError(
                f"❌ '{data_dir}' klasörü bulunamadı!\n"
                f"🛠️ Çözüm:\n"
                f"   1. mkdir data\n"
                f"   2. Kaggle'dan dataset'i indir: https://www.kaggle.com/datasets/iamsumat/spotify-top-2000s-mega-dataset\n"
                f"   3. ZIP dosyasını '{data_dir}' klasörüne çıkart"
            )
        
        # CSV dosyalarını bul
        csv_files = []
        for file in os.listdir(data_dir):
            if file.endswith('.csv'):
                csv_files.append(os.path.join(data_dir, file))
        
        if not csv_files:
            raise FileNotFoundError(
                f"❌ '{data_dir}' klasöründe CSV dosyası bulunamadı!\n"
                f"🛠️ Çözüm:\n"
                f"   1. https://www.kaggle.com/datasets/iamsumat/spotify-top-2000s-mega-dataset\n"
                f"   2. 'Download' butonuna tıkla\n"
                f"   3. ZIP dosyasını '{data_dir}' klasörüne çıkart\n"
                f"   4. CSV dosyasının doğru yerde olduğunu kontrol et"
            )
        
        # İlk CSV dosyasını yükle
        csv_path = csv_files[0]
        logger.info(f"📄 CSV dosyası bulundu: {csv_path}")
        
        try:
            self.data = pd.read_csv(csv_path)
            logger.info(f"✅ Dataset başarıyla yüklendi: {len(self.data)} kayıt, {len(self.data.columns)} kolon")
            
            # Veriyi temizle ve hazırla
            self.prepare_data()
            
        except Exception as e:
            raise Exception(f"❌ CSV dosyası okunamadı: {e}\n"
                          f"🛠️ Dosya bozuk olabilir, tekrar indirmeyi dene")
    

    
    def prepare_data(self):
        """Veriyi temizle ve hazırla"""
        try:
            # Kaggle dataset'inin gerçek column isimlerini standart isimlere çevir
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
            
            # Column isimlerini yeniden adlandır
            self.data = self.data.rename(columns=column_mapping)
            logger.info(f"📋 Column isimleri standartlaştırıldı")
            
            # Eksik verileri temizle
            self.data = self.data.dropna()
            
            # Veri tiplerini kontrol et ve düzelt
            numeric_columns = ['year', 'bpm', 'energy', 'danceability', 'loudness', 
                             'liveness', 'valence', 'length', 'acousticness', 'speechiness', 'popularity']
            
            for col in numeric_columns:
                if col in self.data.columns:
                    self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
            
            # String kolonları temizle
            string_columns = ['title', 'artist', 'top_genre']
            for col in string_columns:
                if col in self.data.columns:
                    self.data[col] = self.data[col].astype(str).str.strip()
            
            # Veriyi yıla göre sırala (historical replay için)
            if 'year' in self.data.columns:
                self.data = self.data.sort_values('year').reset_index(drop=True)
            
            logger.info(f"📊 Data hazırlandı: {len(self.data)} kayıt, {len(self.data.columns)} kolon")
            logger.info(f"🎵 Yıl aralığı: {self.data['year'].min()} - {self.data['year'].max()}")
            logger.info(f"🎤 Benzersiz sanatçı sayısı: {self.data['artist'].nunique()}")
            logger.info(f"🎸 Benzersiz genre sayısı: {self.data['top_genre'].nunique()}")
            
        except Exception as e:
            logger.error(f"Veri hazırlama hatası: {e}")
            raise
    
    def create_song_event(self, row, event_type='historical'):
        """Şarkı verisini Kafka event'ine dönüştür"""
        # NaN değerleri güvenli şekilde integer'a çevir
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
        """Confluent Kafka ile mesaj gönder"""
        try:
            self.producer.produce(
                topic=topic,
                key=str(key) if key else None,
                value=json.dumps(value, ensure_ascii=False).encode('utf-8'),
                callback=self.delivery_report
            )
        except Exception as e:
            logger.error(f"Mesaj gönderme hatası: {e}")
    
    def simulate_real_time_events(self):
        """Rastgele real-time events üret"""
        random_song = self.data.sample(1).iloc[0]
        
        popularity_boost = random.randint(15, 40)
        boosted_song = random_song.copy()
        boosted_song['popularity'] = min(100, boosted_song['popularity'] + popularity_boost)
        
        event = self.create_song_event(boosted_song, 'popularity_surge')
        event['metadata']['boost_amount'] = popularity_boost
        event['metadata']['reason'] = random.choice([
            'viral_tiktok', 'playlist_feature', 'artist_collab', 'radio_play', 'meme_trend', 'influencer_post'
        ])
        event['metadata']['original_popularity'] = int(random_song['popularity'])
        event['metadata']['new_popularity'] = int(boosted_song['popularity'])
        
        return event
    
    def stream_historical_data(self, seconds_per_year=10, batch_size=10):
        """
        Spotify verisini yıllara göre zamanlı olarak Kafka'ya gönderir.
        Örneğin: seconds_per_year=10 → Her yılın şarkılarını 10 saniyede gönder.
        """
        logger.info("🎵 Historical data streaming başlıyor...")

        total_songs = len(self.data)
        processed = 0

        yearly_data = self.data.groupby('year')

        for year, year_df in yearly_data:
            logger.info(f"📅 {year} yılı: {len(year_df)} şarkı hazırlanıyor...")

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

                        # Durum güncelleme
                        if processed % 200 == 0:
                            progress = (processed / total_songs) * 100
                            logger.info(f"⚡ İlerleme: {processed}/{total_songs} ({progress:.1f}%) - Son: {song['artist']} - {song['title']}")

                    except Exception as e:
                        logger.error(f"❌ Şarkı gönderme hatası: {e}")

                self.producer.poll(0)  # buffer boşalt

            # Gerçek zamanlı event ekle (%5 olasılık)
            if random.random() < 0.05:
                try:
                    rt_event = self.simulate_real_time_events()
                    self.send_message(
                        topic=self.topics['real_time_events'],
                        key=None,
                        value=rt_event
                    )
                    song_title = rt_event['song_data']['title']
                    artist = rt_event['song_data']['artist']
                    reason = rt_event['metadata']['reason']
                    old_pop = rt_event['metadata']['original_popularity']
                    new_pop = rt_event['metadata']['new_popularity']
                    logger.info(f"🔥 Real-time event: '{song_title}' by {artist} - {reason} ({old_pop}→{new_pop})")
                except Exception as e:
                    logger.error(f"❌ Real-time event hatası: {e}")

            # Yıl başına süreyi dengele
            elapsed = time.time() - start_time
            remaining = max(0, seconds_per_year - elapsed)
            if remaining > 0:
                logger.info(f"⏸️ {year} yılı tamamlandı, {remaining:.2f} saniye bekleniyor...")
                time.sleep(remaining)

        self.producer.flush()
        logger.info(f"✅ Streaming tamamlandı! Toplam {processed} şarkı gönderildi.")
        logger.info(f"📈 Kafka topics'e başarıyla stream edildi:")
        logger.info(f"   • {self.topics['historical_stream']}: {processed} historical events")
        logger.info(f"   • {self.topics['real_time_events']}: ~{processed//20} real-time events")

    
    def close(self):
        """Producer'ı kapat"""
        self.producer.flush()

# Kullanım örneği
if __name__ == "__main__":
    # Önce data klasörünü kontrol et
    if not os.path.exists('./data'):
        print("❌ './data' klasörü bulunamadı!")
        print("\n🛠️ DATASET KURULUM REHBERİ:")
        print("1. mkdir data")
        print("2. https://www.kaggle.com/datasets/iamsumat/spotify-top-2000s-mega-dataset adresine git")
        print("3. 'Download' butonuna tıkla")
        print("4. ZIP dosyasını './data/' klasörüne çıkart")
        print("5. Producer'ı tekrar çalıştır")
        exit(1)
    
    try:
        print("🎵" + "="*60 + "🎵")
        print("    🎧 SPOTIFY REAL-TIME ANALYTICS PRODUCER 🎧    ")
        print("🎵" + "="*60 + "🎵")
        print()
        
        # Streamer oluştur
        logger.info("🚀 Spotify Data Streamer başlatılıyor...")
        streamer = SpotifyDataStreamer()
        
        print(f"✅ Dataset yüklendi ve hazırlandı!")
        print(f"📊 Toplam şarkı: {len(streamer.data)}")
        print(f"🎯 Yıl aralığı: {streamer.data['year'].min()} - {streamer.data['year'].max()}")
        print(f"🎤 Sanatçı sayısı: {streamer.data['artist'].nunique()}")
        print(f"🎸 Genre sayısı: {streamer.data['top_genre'].nunique()}")
        print()
        
        # Historical Data Replay'i başlat
        print("⚡ Historical Data Replay başlatılıyor...")
        print("📈 2000'li yılların müzik verisi zamanlı olarak stream ediliyor...")
        print("🔄 Time compression: 1000x (1 yıl = ~1 saniye)")
        print("💡 Real-time event'ler rastgele enjekte ediliyor...")
        print()
        print("⏹️  Durdurmak için Ctrl+C'ye basın")
        print("-" * 60)
        
        # Streaming'i başlat
        streamer.stream_historical_data(seconds_per_year=5, batch_size=20)
        
    except Exception as e:
        logger.error(f"Ana hata: {e}")
        print(f"\n{e}")
        
    except KeyboardInterrupt:
        print("\n⏹️  Streaming durduruldu.")
    finally:
        try:
            streamer.close()
        except:
            pass
        print("👋 Producer kapatıldı.")