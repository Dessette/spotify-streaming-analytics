import dash
from dash import dcc, html, Input, Output, callback
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import numpy as np
from confluent_kafka import Consumer, KafkaError
import json
import threading
import time
from collections import deque, defaultdict
from datetime import datetime, timedelta
import logging

# Logging ayarları
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SpotifyDashboard:
    def __init__(self, kafka_servers='localhost:9092'):
        """
        Real-time Spotify Analytics Dashboard (Confluent Kafka)
        """
        self.kafka_servers = kafka_servers
        self.app = dash.Dash(__name__, external_stylesheets=['https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap'])
        
        # Data buffers - Real-time veri saklamak için
        self.max_buffer_size = 10000
        self.data_buffers = {
            'songs': deque(maxlen=self.max_buffer_size),
            'genre_trends': deque(maxlen=200),
            'real_time_events': deque(maxlen=100),
            'audio_features': deque(maxlen=300),
            'popularity_timeline': deque(maxlen=500),
            'artist_stats': deque(maxlen=200)
        }
        
        # Performance metrics
        self.performance_metrics = {
            'messages_processed': 0,
            'last_update': datetime.now(),
            'processing_rate': 0.0,
            'start_time': datetime.now(),
            'genres_count': 0,
            'artists_count': 0
        }
        
        self.running = True
        self.setup_kafka_consumer()
        self.setup_layout()
        self.setup_callbacks()
        
    def setup_kafka_consumer(self):
        """Confluent Kafka consumer'ını kurma ve background thread başlatma"""
        try:
            self.consumer_config = {
                'bootstrap.servers': self.kafka_servers,
                'group.id': 'spotify-dashboard-group',
                'auto.offset.reset': 'latest',
                'enable.auto.commit': True,
                'session.timeout.ms': 30000
            }
            
            self.consumer = Consumer(self.consumer_config)
            topics = ['spotify-historical-stream', 'spotify-realtime-events']
            self.consumer.subscribe(topics)
            
            # Kafka bağlantısını test et
            logger.info("🔄 Kafka bağlantısı test ediliyor...")
            test_msg = self.consumer.poll(timeout=2.0)
            
            # Background thread'i başlat
            self.consumer_thread = threading.Thread(
                target=self.consume_kafka_messages, 
                daemon=True
            )
            self.consumer_thread.start()
            logger.info("✅ Kafka consumer başlatıldı")
            
        except Exception as e:
            logger.error(f"❌ Kafka consumer hatası: {e}")
    
    def consume_kafka_messages(self):
        """Kafka mesajlarını sürekli oku ve buffer'lara ekle"""
        logger.info("🎵 Kafka mesaj tüketimi başladı...")
        
        consecutive_errors = 0
        max_errors = 10
        
        while self.running:
            try:
                msg = self.consumer.poll(timeout=2.0)
                
                if msg is None:
                    # Kafka'dan mesaj gelmiyor, mock data'ya geç
                    if consecutive_errors == 0:  # İlk kez uyar
                        logger.warning("⚠️ Kafka'dan mesaj gelmiyor")
                    consecutive_errors += 1
                    if consecutive_errors > max_errors:
                        consecutive_errors = 0  # Reset counter
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        continue
                
                try:
                    # JSON parse et
                    data = json.loads(msg.value().decode('utf-8'))
                    topic = msg.topic()
                    
                    # Mesajı uygun buffer'a ekle
                    if topic == 'spotify-historical-stream':
                        self.process_song_data(data)
                        logger.debug(f"📥 Song data alındı: {data.get('song_data', {}).get('title', 'Unknown')}")
                    elif topic == 'spotify-realtime-events':
                        self.process_real_time_event(data)
                        logger.info(f"🔥 Real-time event alındı: {data.get('event_type', 'unknown')}")
                    
                    # Performance metriklerini güncelle
                    self.performance_metrics['messages_processed'] += 1
                    consecutive_errors = 0  # Reset error counter
                    
                    # Her 50 mesajda bir rate hesapla
                    if self.performance_metrics['messages_processed'] % 50 == 0:
                        now = datetime.now()
                        time_diff = (now - self.performance_metrics['last_update']).total_seconds()
                        if time_diff > 0:
                            self.performance_metrics['processing_rate'] = 50 / time_diff
                        self.performance_metrics['last_update'] = now
                        logger.info(f"📊 Dashboard: {self.performance_metrics['messages_processed']} mesaj işlendi")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}")
                    consecutive_errors += 1
                except Exception as e:
                    logger.error(f"Message processing error: {e}")
                    consecutive_errors += 1
                    
            except Exception as e:
                logger.error(f"Kafka tüketim hatası: {e}")
                consecutive_errors += 1
                time.sleep(5)  # Hata durumunda bekle
    
    def process_song_data(self, data):
        """Şarkı verisini işle ve buffer'a ekle"""
        if 'song_data' in data:
            song = data['song_data']
            song['timestamp'] = datetime.now()
            song['event_type'] = data.get('event_type', 'historical')
            
            self.data_buffers['songs'].append(song)
            
            # Genre trend'i güncelle
            self.update_genre_trends(song)
            
            # Audio features'ı güncelle
            self.update_audio_features(song)
            
            # Popularity timeline'ı güncelle
            self.update_popularity_timeline(song)
            
            # Artist stats'ı güncelle
            self.update_artist_stats(song)
    
    def process_real_time_event(self, data):
        """Real-time event'leri işle"""
        event = {
            'timestamp': datetime.now(),
            'song': data.get('song_data', {}),
            'metadata': data.get('metadata', {}),
            'event_type': data.get('event_type', 'unknown')
        }
        self.data_buffers['real_time_events'].append(event)
    
    def update_genre_trends(self, song):
        """Genre trend verilerini güncelle"""
        genre_data = {
            'genre': song['top_genre'],
            'popularity': song['popularity'],
            'timestamp': song['timestamp'],
            'count': 1
        }
        self.data_buffers['genre_trends'].append(genre_data)
    
    def update_audio_features(self, song):
        """Audio features verilerini güncelle"""
        features = {
            'timestamp': song['timestamp'],
            'energy': song['energy'],
            'danceability': song['danceability'],
            'valence': song['valence'],
            'acousticness': song['acousticness'],
            'genre': song['top_genre'],
            'popularity': song['popularity']
        }
        self.data_buffers['audio_features'].append(features)
    
    def update_popularity_timeline(self, song):
        """Popularity timeline'ını güncelle"""
        timeline_data = {
            'timestamp': song['timestamp'],
            'popularity': song['popularity'],
            'genre': song['top_genre'],
            'artist': song['artist'],
            'title': song['title'],
            'year': song['year']
        }
        self.data_buffers['popularity_timeline'].append(timeline_data)
    
    def update_artist_stats(self, song):
        """Artist istatistiklerini güncelle"""
        artist_data = {
            'timestamp': song['timestamp'],
            'artist': song['artist'],
            'popularity': song['popularity'],
            'genre': song['top_genre'],
            'year': song['year']
        }
        self.data_buffers['artist_stats'].append(artist_data)
        
        # Performance metrics güncelle
        unique_genres = set(g['genre'] for g in self.data_buffers['genre_trends'])
        unique_artists = set(a['artist'] for a in self.data_buffers['artist_stats'])
        
        self.performance_metrics['genres_count'] = len(unique_genres)
        self.performance_metrics['artists_count'] = len(unique_artists)
    
    def setup_layout(self):
        """Dashboard layout'unu kur"""
        self.app.layout = html.Div([
            # Header
            html.Div([
                html.H1("🎵 Spotify Real-time Analytics Dashboard", className="header-title"),
                html.Div(id="live-indicator", children="🔴 LIVE", className="live-indicator"),
                html.Div(id="performance-metrics", className="performance-metrics")
            ], className="header"),
            
            # Ana container
            html.Div([
                # Üst satır - Ana metrikler
                html.Div([
                    # Real-time popularity gauge
                    html.Div([
                        dcc.Graph(id="popularity-gauge")
                    ], className="metric-card"),
                    
                    # Song count
                    html.Div([
                        dcc.Graph(id="song-count-metric")
                    ], className="metric-card"),
                    
                    # Top genre
                    html.Div([
                        dcc.Graph(id="top-genre-metric")
                    ], className="metric-card"),
                    
                    # Artists count
                    html.Div([
                        dcc.Graph(id="artists-count-metric")
                    ], className="metric-card")
                ], className="metrics-row"),
                
                # İkinci satır - Trend grafikleri
                html.Div([
                    # Genre popularity trends
                    html.Div([
                        dcc.Graph(id="genre-trends-chart")
                    ], className="chart-card"),
                    
                    # Popularity timeline
                    html.Div([
                        dcc.Graph(id="popularity-timeline")
                    ], className="chart-card")
                ], className="charts-row"),
                
                # Üçüncü satır - Audio features ve events
                html.Div([
                    # Audio features radar
                    html.Div([
                        dcc.Graph(id="audio-features-radar")
                    ], className="chart-card"),
                    
                    # Real-time events
                    html.Div([
                        dcc.Graph(id="real-time-events")
                    ], className="chart-card")
                ], className="charts-row"),
                
                # Alt satır - Detaylı analizler
                html.Div([
                    # Year distribution heatmap
                    html.Div([
                        dcc.Graph(id="year-genre-heatmap")
                    ], className="chart-card-full")
                ], className="charts-row")
            ], className="main-container"),
            
            # Update interval
            dcc.Interval(
                id='interval-component',
                interval=2000,  # 2 saniyede bir güncelle
                n_intervals=0
            )
        ])
    
    def setup_callbacks(self):
        """Dashboard callback'lerini kur"""
        
        @self.app.callback(
            [Output('popularity-gauge', 'figure'),
             Output('song-count-metric', 'figure'),
             Output('top-genre-metric', 'figure'),
             Output('artists-count-metric', 'figure'),
             Output('genre-trends-chart', 'figure'),
             Output('popularity-timeline', 'figure'),
             Output('audio-features-radar', 'figure'),
             Output('real-time-events', 'figure'),
             Output('year-genre-heatmap', 'figure'),
             Output('performance-metrics', 'children')],
            [Input('interval-component', 'n_intervals')]
        )
        def update_dashboard(n):
            return (
                self.create_popularity_gauge(),
                self.create_song_count_metric(),
                self.create_top_genre_metric(),
                self.create_artists_count_metric(),
                self.create_genre_trends_chart(),
                self.create_popularity_timeline(),
                self.create_audio_features_radar(),
                self.create_real_time_events_chart(),
                self.create_year_genre_heatmap(),
                self.create_performance_metrics()
            )
    
    def create_popularity_gauge(self):
        """Popularity gauge grafiği"""
        if not self.data_buffers['songs']:
            current_popularity = 0
        else:
            recent_songs = list(self.data_buffers['songs'])[-20:]
            current_popularity = np.mean([s['popularity'] for s in recent_songs])
        
        fig = go.Figure(go.Indicator(
            mode = "gauge+number+delta",
            value = current_popularity,
            domain = {'x': [0, 1], 'y': [0, 1]},
            title = {'text': "Ortalama Popülerlik (Son 20 Şarkı)", 'font': {'size': 14}},
            delta = {'reference': 50},
            gauge = {
                'axis': {'range': [None, 100]},
                'bar': {'color': "#1DB954"},
                'steps': [
                    {'range': [0, 25], 'color': "#ffebee"},
                    {'range': [25, 50], 'color': "#fff3e0"},
                    {'range': [50, 75], 'color': "#e8f5e8"},
                    {'range': [75, 100], 'color': "#c8e6c9"}],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 90}}))
        
        fig.update_layout(height=200, margin=dict(l=20, r=20, t=40, b=20), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        return fig
    
    def create_song_count_metric(self):
        """Şarkı sayısı metriği"""
        total_songs = len(self.data_buffers['songs'])
        
        fig = go.Figure()
        fig.add_trace(go.Indicator(
            mode = "number",
            value = total_songs,
            title = {"text": "Toplam Şarkı", "font": {"size": 14}},
            number = {"font": {"size": 40, "color": "#1DB954"}}
        ))
        
        fig.update_layout(height=200, margin=dict(l=20, r=20, t=40, b=20), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        return fig
    
    def create_top_genre_metric(self):
        """En popüler genre metriği"""
        if not self.data_buffers['genre_trends']:
            top_genre = "N/A"
        else:
            recent_genres = list(self.data_buffers['genre_trends'])[-50:]
            genre_counts = defaultdict(int)
            for genre_data in recent_genres:
                genre_counts[genre_data['genre']] += 1
            
            if genre_counts:
                top_genre = max(genre_counts, key=genre_counts.get)
            else:
                top_genre = "N/A"
        
        # Plotly Indicator için sadece number mode kullan
        fig = go.Figure()
        fig.add_annotation(
            text=f"<b>En Popüler Genre</b><br><span style='font-size:24px; color:#1DB954'>{top_genre}</span>",
            x=0.5, y=0.5,
            xref="paper", yref="paper",
            showarrow=False,
            font=dict(size=14, color="white"),
            align="center"
        )
        
        fig.update_layout(
            height=200, 
            margin=dict(l=20, r=20, t=40, b=20), 
            paper_bgcolor='rgba(0,0,0,0)', 
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(visible=False),
            yaxis=dict(visible=False)
        )
        return fig
    
    def create_artists_count_metric(self):
        """Sanatçı sayısı metriği"""
        artists_count = self.performance_metrics.get('artists_count', 0)
        
        fig = go.Figure()
        fig.add_trace(go.Indicator(
            mode = "number",
            value = artists_count,
            title = {"text": "Benzersiz Sanatçı", "font": {"size": 14}},
            number = {"font": {"size": 40, "color": "#1DB954"}}
        ))
        
        fig.update_layout(height=200, margin=dict(l=20, r=20, t=40, b=20), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        return fig
    
    def create_genre_trends_chart(self):
        """Genre trend grafiği"""
        if not self.data_buffers['genre_trends']:
            return go.Figure().add_annotation(text="Veri bekleniyor...", showarrow=False, font=dict(color="white"))
        
        # Son 5 dakika için genre analizi
        recent_time = datetime.now() - timedelta(minutes=5)
        recent_genres = [g for g in self.data_buffers['genre_trends'] if g['timestamp'] > recent_time]
        
        if not recent_genres:
            return go.Figure().add_annotation(text="Son 5 dakikada veri yok", showarrow=False, font=dict(color="white"))
        
        # Genre'lara göre grupla ve ortalama popülerlik hesapla
        genre_stats = defaultdict(lambda: {'count': 0, 'total_popularity': 0})
        for genre_data in recent_genres:
            genre = genre_data['genre']
            genre_stats[genre]['count'] += 1
            genre_stats[genre]['total_popularity'] += genre_data['popularity']
        
        genres = []
        avg_popularities = []
        counts = []
        
        for genre, stats in genre_stats.items():
            avg_popularity = stats['total_popularity'] / stats['count']
            genres.append(genre)
            avg_popularities.append(avg_popularity)
            counts.append(stats['count'])
        
        # Top 8 genre'yi göster
        sorted_data = sorted(zip(genres, avg_popularities, counts), key=lambda x: x[1], reverse=True)[:8]
        genres, avg_popularities, counts = zip(*sorted_data) if sorted_data else ([], [], [])
        
        fig = px.bar(
            x=genres,
            y=avg_popularities,
            title="Genre Popülerlik Ortalaması (Son 5 Dakika)",
            color=avg_popularities,
            color_continuous_scale="viridis"
        )
        
        fig.update_layout(
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
            showlegend=False,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color="white"),
            title_font_color="white"
        )
        fig.update_xaxes(tickangle=45)
        return fig
    
    def create_popularity_timeline(self):
        """Popularity timeline grafiği"""
        if not self.data_buffers['popularity_timeline']:
            return go.Figure().add_annotation(text="Veri bekleniyor...", showarrow=False, font=dict(color="white"))
        
        # Son 10 dakika
        recent_time = datetime.now() - timedelta(minutes=10)
        recent_timeline = [t for t in self.data_buffers['popularity_timeline'] if t['timestamp'] > recent_time]
        
        if not recent_timeline:
            return go.Figure().add_annotation(text="Son 10 dakikada veri yok", showarrow=False, font=dict(color="white"))
        
        df = pd.DataFrame(recent_timeline)
        
        fig = px.scatter(
            df, 
            x='timestamp', 
            y='popularity',
            color='genre',
            title="Popülerlik Zaman Çizelgesi (Son 10 Dakika)",
            hover_data=['artist', 'title', 'year']
        )
        
        fig.update_layout(
            height=300, 
            margin=dict(l=20, r=20, t=40, b=20),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color="white"),
            title_font_color="white"
        )
        return fig
    
    def create_audio_features_radar(self):
        """Audio features radar grafiği"""
        if not self.data_buffers['audio_features']:
            return go.Figure().add_annotation(text="Veri bekleniyor...", showarrow=False, font=dict(color="white"))
        
        recent_features = list(self.data_buffers['audio_features'])[-30:]  # Son 30 şarkı
        avg_features = {
            'Energy': np.mean([f['energy'] for f in recent_features]),
            'Danceability': np.mean([f['danceability'] for f in recent_features]),
            'Valence': np.mean([f['valence'] for f in recent_features]),
            'Acousticness': np.mean([f['acousticness'] for f in recent_features])
        }
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatterpolar(
            r=list(avg_features.values()),
            theta=list(avg_features.keys()),
            fill='toself',
            name='Ortalama Audio Features',
            line_color='#1DB954'
        ))
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, 100],
                    color="white"
                ),
                angularaxis=dict(color="white")
            ),
            showlegend=True,
            title="Audio Features Profili (Son 30 Şarkı)",
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color="white"),
            title_font_color="white"
        )
        
        return fig
    
    def create_real_time_events_chart(self):
        """Real-time events grafiği"""
        if not self.data_buffers['real_time_events']:
            return go.Figure().add_annotation(text="Real-time event bekleniyor...", showarrow=False, font=dict(color="white"))
        
        events = list(self.data_buffers['real_time_events'])[-15:]
        
        if not events:
            return go.Figure().add_annotation(text="Henüz real-time event yok", showarrow=False, font=dict(color="white"))
        
        # Event'leri timeline olarak göster
        event_data = []
        for event in events:
            song_title = event['song'].get('title', 'Unknown')
            artist = event['song'].get('artist', 'Unknown')
            reason = event['metadata'].get('reason', 'unknown')
            boost = event['metadata'].get('boost_amount', 0)
            
            event_data.append({
                'timestamp': event['timestamp'],
                'event': f"{song_title} - {artist}",
                'reason': reason,
                'boost': boost,
                'y_pos': len(event_data)
            })
        
        df = pd.DataFrame(event_data)
        
        fig = px.scatter(
            df,
            x='timestamp',
            y='y_pos',
            color='reason',
            size='boost',
            hover_data=['event', 'boost'],
            title="Real-time Events Timeline"
        )
        
        fig.update_layout(
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
            showlegend=True,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color="white"),
            title_font_color="white",
            yaxis_title="Event Index"
        )
        
        return fig
    
    def create_year_genre_heatmap(self):
        """Yıl-Genre heatmap"""
        if len(self.data_buffers['popularity_timeline']) < 20:
            return go.Figure().add_annotation(text="Yeterli veri yok", showarrow=False, font=dict(color="white"))
        
        # Son veriyi al
        recent_data = list(self.data_buffers['popularity_timeline'])[-200:]
        df = pd.DataFrame(recent_data)
        
        # Yıl ve genre'ye göre ortalama popülerlik hesapla
        year_genre_stats = df.groupby(['year', 'genre'])['popularity'].mean().reset_index()
        
        if year_genre_stats.empty:
            return go.Figure().add_annotation(text="Heatmap için veri yok", showarrow=False, font=dict(color="white"))
        
        # Pivot table oluştur
        pivot_df = year_genre_stats.pivot(index='genre', columns='year', values='popularity')
        pivot_df = pivot_df.fillna(0)
        
        fig = px.imshow(
            pivot_df,
            title="Yıl-Genre Popülerlik Haritası",
            color_continuous_scale="viridis",
            aspect="auto"
        )
        
        fig.update_layout(
            height=400,
            margin=dict(l=20, r=20, t=40, b=20),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color="white"),
            title_font_color="white"
        )
        
        return fig
    
    def create_performance_metrics(self):
        """Performance metrics HTML"""
        metrics = self.performance_metrics
        uptime = (datetime.now() - metrics['start_time']).total_seconds()
        
        return html.Div([
            html.Span(f"İşlenen: {metrics['messages_processed']}", className="metric"),
            html.Span(f"Hız: {metrics['processing_rate']:.1f} msg/sec", className="metric"),
            html.Span(f"Uptime: {uptime:.0f}s", className="metric"),
            html.Span(f"Genres: {metrics['genres_count']}", className="metric"),
            html.Span(f"Artists: {metrics['artists_count']}", className="metric")
        ])
    
    def run(self, host='127.0.0.1', port=8050, debug=False):
        """Dashboard'u başlat"""
        logger.info(f"🚀 Spotify Dashboard başlatılıyor: http://{host}:{port}")
        
        # Custom CSS ekle
        self.app.index_string = '''
        <!DOCTYPE html>
        <html>
            <head>
                {%metas%}
                <title>Spotify Analytics Dashboard</title>
                {%favicon%}
                {%css%}
                <style>
                    body { 
                        margin: 0; 
                        font-family: "Inter", "Helvetica Neue", Helvetica, Arial, sans-serif; 
                        background: linear-gradient(135deg, #0d1117 0%, #161b22 100%);
                        color: white; 
                        min-height: 100vh;
                    }
                    .header { 
                        background: linear-gradient(90deg, #1DB954, #1ed760); 
                        padding: 20px; 
                        display: flex; 
                        justify-content: space-between; 
                        align-items: center;
                        box-shadow: 0 4px 20px rgba(29, 185, 84, 0.3);
                    }
                    .header-title { 
                        margin: 0; 
                        color: white; 
                        font-weight: 700;
                        font-size: 28px;
                    }
                    .live-indicator { 
                        color: #ff4444; 
                        font-weight: bold; 
                        font-size: 18px;
                        animation: pulse 2s infinite;
                    }
                    @keyframes pulse {
                        0% { opacity: 1; }
                        50% { opacity: 0.5; }
                        100% { opacity: 1; }
                    }
                    .performance-metrics { 
                        display: flex; 
                        gap: 20px; 
                        flex-wrap: wrap;
                    }
                    .metric { 
                        background: rgba(255,255,255,0.2); 
                        padding: 8px 16px; 
                        border-radius: 20px;
                        font-size: 14px;
                        font-weight: 500;
                        backdrop-filter: blur(10px);
                    }
                    .main-container { 
                        padding: 30px; 
                        max-width: 1400px; 
                        margin: 0 auto;
                    }
                    .metrics-row, .charts-row { 
                        display: flex; 
                        gap: 20px; 
                        margin-bottom: 30px; 
                        flex-wrap: wrap;
                    }
                    .metric-card { 
                        flex: 1; 
                        background: rgba(255,255,255,0.05); 
                        border-radius: 16px; 
                        padding: 20px;
                        backdrop-filter: blur(10px);
                        border: 1px solid rgba(255,255,255,0.1);
                        min-width: 200px;
                    }
                    .chart-card { 
                        flex: 1; 
                        background: rgba(255,255,255,0.05); 
                        border-radius: 16px; 
                        padding: 20px;
                        backdrop-filter: blur(10px);
                        border: 1px solid rgba(255,255,255,0.1);
                        min-width: 400px;
                    }
                    .chart-card-full { 
                        flex: 1; 
                        background: rgba(255,255,255,0.05); 
                        border-radius: 16px; 
                        padding: 20px;
                        backdrop-filter: blur(10px);
                        border: 1px solid rgba(255,255,255,0.1);
                    }
                    
                    /* Mobile Responsive */
                    @media (max-width: 768px) {
                        .metrics-row, .charts-row {
                            flex-direction: column;
                        }
                        .metric-card, .chart-card {
                            min-width: unset;
                        }
                        .header {
                            flex-direction: column;
                            gap: 10px;
                        }
                        .performance-metrics {
                            justify-content: center;
                        }
                    }
                </style>
            </head>
            <body>
                {%app_entry%}
                <footer>
                    {%config%}
                    {%scripts%}
                    {%renderer%}
                </footer>
            </body>
        </html>
        '''
        
        self.app.run_server(host=host, port=port, debug=debug)
    
    def stop(self):
        """Dashboard'u durdur"""
        self.running = False
        if hasattr(self, 'consumer'):
            self.consumer.close()

# Kullanım
if __name__ == "__main__":
    print("🎵" + "="*60 + "🎵")
    print("    📊 SPOTIFY REAL-TIME ANALYTICS DASHBOARD 📊    ")
    print("🎵" + "="*60 + "🎵")
    print()
    
    dashboard = SpotifyDashboard()
    
    try:
        print("✅ Dashboard başlatılıyor...")
        print("🔗 URL: http://localhost:8050")
        print("📊 Real-time grafikler yükleniyor...")
        print("⏹️  Durdurmak için Ctrl+C'ye basın")
        print("-" * 60)
        
        dashboard.run(debug=False)
    except KeyboardInterrupt:
        logger.info("🛑 Dashboard kapatılıyor...")
    finally:
        dashboard.stop()
        logger.info("👋 Dashboard kapatıldı!")