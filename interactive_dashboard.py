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

from timeless_genre_analytics import TimelessGenreAnalyzer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SpotifyDashboard:
    def __init__(self, kafka_servers='localhost:9092'):
        self.kafka_servers = kafka_servers
        self.app = dash.Dash(__name__)
        
        # Küçültülmüş data buffers
        self.max_buffer_size = 2000
        self.data_buffers = {
            'songs': deque(maxlen=self.max_buffer_size),
            'genre_trends': deque(maxlen=100),
            'real_time_events': deque(maxlen=50),
            'audio_features': deque(maxlen=150),
            'popularity_timeline': deque(maxlen=250),
            'artist_stats': deque(maxlen=100)
        }
        
        self.timeless_analyzer = TimelessGenreAnalyzer(analysis_periods=[5, 10])
        
        self.performance_metrics = {
            'messages_processed': 0,
            'last_update': datetime.now(),
            'processing_rate': 0.0,
            'start_time': datetime.now(),
            'genres_count': 0,
            'artists_count': 0,
            'timeless_analysis_count': 0,
            'timeout_events': 0,
            'active_events': 0,
            'rollback_events': 0
        }
        
        self.running = True
        self.setup_kafka_consumer()
        self.setup_layout()
        self.setup_callbacks()
        
    def setup_kafka_consumer(self):
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
            
            self.consumer_thread = threading.Thread(
                target=self.consume_kafka_messages, 
                daemon=True
            )
            self.consumer_thread.start()
            logger.info("Kafka consumer started")
            
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}")
    
    def consume_kafka_messages(self):
        logger.info("Kafka message consumption started")
        
        consecutive_errors = 0
        max_errors = 10
        timeless_update_counter = 0
        
        while self.running:
            try:
                msg = self.consumer.poll(timeout=2.0)
                
                if msg is None:
                    consecutive_errors += 1
                    if consecutive_errors > max_errors:
                        consecutive_errors = 0
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        continue
                
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    topic = msg.topic()
                    
                    if topic == 'spotify-historical-stream':
                        self.process_song_data(data)
                        
                        if 'song_data' in data:
                            self.timeless_analyzer.add_song_data(data['song_data'])
                            timeless_update_counter += 1
                            
                            # Her 50 şarkıda bir timeless analizi (100'den azaltıldı)
                            if timeless_update_counter % 50 == 0:
                                self.update_timeless_analysis()
                                timeless_update_counter = 0
                        
                    elif topic == 'spotify-realtime-events':
                        self.process_real_time_event(data)
                    
                    self.performance_metrics['messages_processed'] += 1
                    consecutive_errors = 0
                    
                    if self.performance_metrics['messages_processed'] % 50 == 0:
                        now = datetime.now()
                        time_diff = (now - self.performance_metrics['last_update']).total_seconds()
                        if time_diff > 0:
                            self.performance_metrics['processing_rate'] = 50 / time_diff
                        self.performance_metrics['last_update'] = now
                        
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}")
                except Exception as e:
                    logger.error(f"Message processing error: {e}")
                    
            except Exception as e:
                logger.error(f"Kafka consumption error: {e}")
                time.sleep(5)
    
    def update_timeless_analysis(self):
        try:
            self.timeless_analyzer.analyze_genre_timelessness()
            self.performance_metrics['timeless_analysis_count'] += 1
            logger.info("Timeless genre analysis updated")
        except Exception as e:
            logger.error(f"Timeless analysis error: {e}")
    
    def process_song_data(self, data):
        if 'song_data' in data:
            song = data['song_data']
            song['timestamp'] = datetime.now()
            song['event_type'] = data.get('event_type', 'historical')
            
            self.data_buffers['songs'].append(song)
            self.update_genre_trends(song)
            self.update_audio_features(song)
            self.update_popularity_timeline(song)
            self.update_artist_stats(song)
    
    def process_real_time_event(self, data):
        event_type = data.get('event_type', 'unknown')
        
        event = {
            'timestamp': datetime.now(),
            'song': data.get('song_data', {}),
            'metadata': data.get('metadata', {}),
            'event_type': event_type,
            'status': 'active' if event_type == 'popularity_surge' else 'rollback' if event_type == 'timeout_rollback' else 'unknown'
        }
        
        # Timeout metrics güncelle
        if event_type == 'popularity_surge':
            self.performance_metrics['timeout_events'] += 1
            if data.get('metadata', {}).get('has_timeout'):
                self.performance_metrics['active_events'] += 1
        elif event_type == 'timeout_rollback':
            self.performance_metrics['rollback_events'] += 1
            if self.performance_metrics['active_events'] > 0:
                self.performance_metrics['active_events'] -= 1
        
        self.data_buffers['real_time_events'].append(event)
    
    def update_genre_trends(self, song):
        genre_data = {
            'genre': song['top_genre'],
            'popularity': song['popularity'],
            'timestamp': song['timestamp'],
            'count': 1
        }
        self.data_buffers['genre_trends'].append(genre_data)
    
    def update_audio_features(self, song):
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
        artist_data = {
            'timestamp': song['timestamp'],
            'artist': song['artist'],
            'popularity': song['popularity'],
            'genre': song['top_genre'],
            'year': song['year']
        }
        self.data_buffers['artist_stats'].append(artist_data)
        
        unique_genres = set(g['genre'] for g in self.data_buffers['genre_trends'])
        unique_artists = set(a['artist'] for a in self.data_buffers['artist_stats'])
        
        self.performance_metrics['genres_count'] = len(unique_genres)
        self.performance_metrics['artists_count'] = len(unique_artists)
    
    def setup_layout(self):
        self.app.layout = html.Div([
            # Header
            html.Div([
                html.H1("Spotify Real-time Analytics Dashboard", className="header-title"),
                html.Div([
                    html.Div(id="live-indicator", children="LIVE", className="live-indicator"),
                ], className="status-indicators"),
                html.Div(id="performance-metrics", className="performance-metrics")
            ], className="header"),
            
            html.Div([
                # Ana metrikler
                html.Div([
                    html.Div([dcc.Graph(id="popularity-gauge")], className="metric-card"),
                    html.Div([dcc.Graph(id="song-count-metric")], className="metric-card"),
                    html.Div([dcc.Graph(id="top-genre-metric")], className="metric-card"),
                    html.Div([dcc.Graph(id="timeless-champion-metric")], className="metric-card")
                ], className="metrics-row"),

                # Trend grafikleri
                html.Div([
                    html.Div([dcc.Graph(id="genre-trends-chart")], className="chart-card"),
                    html.Div([dcc.Graph(id="popularity-timeline")], className="chart-card")
                ], className="charts-row"),

                # Timeless Analytics
                html.Div([
                    html.Div([dcc.Graph(id="timeless-rankings-chart")], className="chart-card"),
                    html.Div([dcc.Graph(id="timeless-score-distribution")], className="chart-card")
                ], className="charts-row"),
                
                # Timeless Timeline
                html.Div([
                    html.Div([dcc.Graph(id="timeless-genre-timeline")], className="chart-card-full")
                ], className="charts-row"),

                html.Div([
                    html.Div([dcc.Graph(id="active-events-gauge")], className="metric-card")
                ], className="metrics-row"),
                
                # Audio features ve events
                html.Div([
                    html.Div([dcc.Graph(id="audio-features-radar")], className="chart-card"),
                    html.Div([dcc.Graph(id="real-time-events")], className="chart-card")
                ], className="charts-row"),
                
                # Timeless Metrics Overview
                html.Div([
                    html.Div([dcc.Graph(id="timeless-metrics-overview")], className="chart-card-full")
                ], className="charts-row")
            ], className="main-container"),
            
            # Update intervals - azaltıldı
            dcc.Interval(
                id='interval-component',
                interval=2000,  # 3000ms -> 2000ms
                n_intervals=0
            ),
            
            dcc.Interval(
                id='timeless-interval',
                interval=15000,  # 30000ms -> 15000ms
                n_intervals=0
            )
        ])
    
    def setup_callbacks(self):
        @self.app.callback(
            [Output('popularity-gauge', 'figure'),
             Output('song-count-metric', 'figure'),
             Output('top-genre-metric', 'figure'),
             Output('timeless-champion-metric', 'figure'),
             Output('active-events-gauge', 'figure'),
             Output('timeless-rankings-chart', 'figure'),
             Output('timeless-score-distribution', 'figure'),
             Output('genre-trends-chart', 'figure'),
             Output('popularity-timeline', 'figure'),
             Output('timeless-genre-timeline', 'figure'),
             Output('audio-features-radar', 'figure'),
             Output('real-time-events', 'figure'),
             Output('timeless-metrics-overview', 'figure'),
             Output('performance-metrics', 'children')],
            [Input('interval-component', 'n_intervals'),
             Input('timeless-interval', 'n_intervals')]
        )
        def update_dashboard(n, timeless_n):
            return (
                self.create_popularity_gauge(),
                self.create_song_count_metric(),
                self.create_top_genre_metric(),
                self.create_timeless_champion_metric(),
                self.create_active_events_gauge(),
                self.create_timeless_rankings_chart(),
                self.create_timeless_score_distribution(),
                self.create_genre_trends_chart(),
                self.create_popularity_timeline(),
                self.create_timeless_genre_timeline(),
                self.create_audio_features_radar(),
                self.create_real_time_events_chart(),
                self.create_timeless_metrics_overview(),
                self.create_performance_metrics()
            )
    
    def create_timeless_champion_metric(self):
        try:
            top_timeless = self.timeless_analyzer.get_top_timeless_genres(1)
            
            if not top_timeless:
                champion_text = "Analyzing..."
                score_text = "..."
            else:
                genre, metric = top_timeless[0]
                champion_text = genre.title()
                score_text = f"{metric.timeless_score:.1f}/100"
            
            fig = go.Figure()
            fig.add_annotation(
                text=f"<b>Timeless Champion</b><br><span style='font-size:20px; color:#FFD700'>{champion_text}</span><br><span style='font-size:16px; color:#1DB954'>{score_text}</span>",
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
            
        except Exception as e:
            logger.error(f"Timeless champion metric error: {e}")
            return go.Figure().add_annotation(text="Error", showarrow=False, font=dict(color="white"))
    
    def create_timeless_rankings_chart(self):
        try:
            top_timeless = self.timeless_analyzer.get_top_timeless_genres(8)
            
            if not top_timeless:
                return go.Figure().add_annotation(text="Timeless analysis pending...", showarrow=False, font=dict(color="white"))
            
            genres = [genre for genre, metric in top_timeless]
            scores = [metric.timeless_score for genre, metric in top_timeless]
            decades = [metric.decade_presence for genre, metric in top_timeless]
            
            hover_text = [
                f"Genre: {genre}<br>"
                f"Timeless Score: {score:.1f}/100<br>"
                f"Decade Presence: {decade} periods<br>"
                f"Consistency: {top_timeless[i][1].consistency_score:.1f}/100"
                for i, (genre, score, decade) in enumerate(zip(genres, scores, decades))
            ]
            
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                x=genres,
                y=scores,
                text=[f"{score:.1f}" for score in scores],
                textposition='outside',
                hovertext=hover_text,
                hoverinfo='text',
                marker=dict(
                    color=scores,
                    colorscale='Viridis',
                    colorbar=dict(title="Timeless Score")
                )
            ))
            
            fig.update_layout(
                title="Top Timeless Genres Rankings",
                xaxis_title="Genres",
                yaxis_title="Timeless Score",
                height=350,
                margin=dict(l=20, r=20, t=40, b=20),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color="white"),
                title_font_color="white"
            )
            
            fig.update_xaxes(tickangle=45)
            return fig
            
        except Exception as e:
            logger.error(f"Timeless rankings chart error: {e}")
            return go.Figure().add_annotation(text="Chart error", showarrow=False, font=dict(color="white"))
    
    def create_timeless_score_distribution(self):
        try:
            analytics_summary = self.timeless_analyzer.get_analytics_summary()
            
            if not analytics_summary.get('top_timeless_genres'):
                return go.Figure().add_annotation(text="Score distribution pending...", showarrow=False, font=dict(color="white"))
            
            all_metrics = list(self.timeless_analyzer.timeless_metrics.values())
            
            if len(all_metrics) < 3:
                return go.Figure().add_annotation(text="Insufficient data", showarrow=False, font=dict(color="white"))
            
            timeless_scores = [m.timeless_score for m in all_metrics]
            
            fig = go.Figure()
            
            fig.add_trace(go.Histogram(
                x=timeless_scores,
                name="Timeless Scores",
                opacity=0.7,
                nbinsx=10,
                marker_color='#1DB954'
            ))
            
            fig.update_layout(
                title="Timeless Score Distribution",
                xaxis_title="Timeless Score",
                yaxis_title="Genre Count",
                height=350,
                margin=dict(l=20, r=20, t=40, b=20),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color="white"),
                title_font_color="white",
                showlegend=False
            )
            
            avg_score = np.mean(timeless_scores)
            fig.add_vline(
                x=avg_score, 
                line_dash="dash", 
                line_color="red",
                annotation_text=f"Avg: {avg_score:.1f}",
                annotation_position="top"
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Score distribution error: {e}")
            return go.Figure().add_annotation(text="Distribution error", showarrow=False, font=dict(color="white"))
    
    def create_timeless_genre_timeline(self):
        try:
            top_timeless = self.timeless_analyzer.get_top_timeless_genres(1)
            
            if not top_timeless:
                return go.Figure().add_annotation(text="Timeline pending...", showarrow=False, font=dict(color="white"))
            
            genre, metric = top_timeless[0]
            timeline_data = self.timeless_analyzer.get_genre_timeline(genre)
            
            if not timeline_data.get('timeline'):
                return go.Figure().add_annotation(text="Timeline data missing", showarrow=False, font=dict(color="white"))
            
            timeline = timeline_data['timeline']
            
            periods = [t['period'] for t in timeline]
            avg_popularities = [t['avg_popularity'] for t in timeline]
            song_counts = [t['song_count'] for t in timeline]
            
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=periods,
                y=avg_popularities,
                mode='lines+markers',
                name='Avg Popularity',
                line=dict(color='#1DB954', width=3),
                marker=dict(size=8),
                hovertemplate='<b>%{x}</b><br>Popularity: %{y:.1f}<br><extra></extra>'
            ))
            
            fig.add_trace(go.Bar(
                x=periods,
                y=song_counts,
                name='Song Count',
                opacity=0.6,
                marker_color='lightblue',
                yaxis='y2',
                hovertemplate='<b>%{x}</b><br>Songs: %{y}<br><extra></extra>'
            ))
            
            fig.update_layout(
                title=f"{genre.title()} - Timeless Journey (Score: {metric.timeless_score:.1f}/100)",
                xaxis_title="Time Periods",
                yaxis_title="Average Popularity",
                height=400,
                margin=dict(l=20, r=20, t=40, b=20),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color="white"),
                title_font_color="white",
                yaxis2=dict(
                    title="Song Count",
                    overlaying='y',
                    side='right',
                    color='lightblue'
                )
            )
            
            fig.update_xaxes(tickangle=45)
            return fig
            
        except Exception as e:
            logger.error(f"Timeless timeline error: {e}")
            return go.Figure().add_annotation(text="Timeline error", showarrow=False, font=dict(color="white"))
    
    def create_timeless_metrics_overview(self):
        try:
            analytics_summary = self.timeless_analyzer.get_analytics_summary()
            
            if not analytics_summary:
                return go.Figure().add_annotation(text="Metrics overview pending...", showarrow=False, font=dict(color="white"))
            
            top_genres = analytics_summary.get('top_timeless_genres', [])[:6]
            
            if not top_genres:
                return go.Figure().add_annotation(text="Insufficient genres analyzed", showarrow=False, font=dict(color="white"))
            
            genres = [g['genre'].title() for g in top_genres]
            scores = [f"{g['score']:.1f}" for g in top_genres]
            decades = [f"{g['decades']}" for g in top_genres]
            consistency = [f"{g['consistency']:.1f}" for g in top_genres]
            
            score_values = [g['score'] for g in top_genres]
            max_score = max(score_values) if score_values else 100
            
            colors = []
            for score in score_values:
                intensity = score / max_score
                colors.append(f'rgba(29, 185, 84, {intensity * 0.7 + 0.3})')
            
            fig = go.Figure(data=[go.Table(
                header=dict(
                    values=['Genre', 'Timeless Score', 'Decades', 'Consistency'],
                    fill_color='rgba(29, 185, 84, 0.8)',
                    align='center',
                    font=dict(color='white', size=14),
                    height=40
                ),
                cells=dict(
                    values=[genres, scores, decades, consistency],
                    fill_color=[colors, colors, colors, colors],
                    align='center',
                    font=dict(color='white', size=12),
                    height=35
                )
            )])
            
            stats = analytics_summary.get('statistics', {})
            subtitle = f"{stats.get('total_genres_analyzed', 0)} genres analyzed | Avg Score: {stats.get('avg_timeless_score', 0):.1f} | Max: {stats.get('max_timeless_score', 0):.1f}"
            
            fig.update_layout(
                title=f"Timeless Metrics Overview<br><sub>{subtitle}</sub>",
                height=300,
                margin=dict(l=20, r=20, t=60, b=20),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color="white"),
                title_font_color="white"
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Metrics overview error: {e}")
    
    def create_active_events_gauge(self):
        """Aktif event sayısı"""
        active_events = self.performance_metrics['active_events']
        
        fig = go.Figure()
        fig.add_trace(go.Indicator(
            mode = "number",
            value = active_events,
            title = {"text": "Active Events", "font": {"size": 14}},
            number = {"font": {"size": 40, "color": "#FFA500" if active_events > 0 else "#1DB954"}}
        ))
        
        fig.update_layout(height=200, margin=dict(l=20, r=20, t=40, b=20), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        return fig
    
    def create_popularity_gauge(self):
        if not self.data_buffers['songs']:
            current_popularity = 0
        else:
            recent_songs = list(self.data_buffers['songs'])[-20:]
            current_popularity = np.mean([s['popularity'] for s in recent_songs])
        
        fig = go.Figure(go.Indicator(
            mode = "gauge+number+delta",
            value = current_popularity,
            domain = {'x': [0, 1], 'y': [0, 1]},
            title = {'text': "Average Popularity (Last 20 Songs)", 'font': {'size': 14}},
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
        total_songs = len(self.data_buffers['songs'])
        
        fig = go.Figure()
        fig.add_trace(go.Indicator(
            mode = "number",
            value = total_songs,
            title = {"text": "Total Songs", "font": {"size": 14}},
            number = {"font": {"size": 40, "color": "#1DB954"}}
        ))
        
        fig.update_layout(height=200, margin=dict(l=20, r=20, t=40, b=20), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        return fig
    
    def create_top_genre_metric(self):
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
        
        fig = go.Figure()
        fig.add_annotation(
            text=f"<b>Top Genre</b><br><span style='font-size:24px; color:#1DB954'>{top_genre}</span>",
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
    
    def create_genre_trends_chart(self):
        if not self.data_buffers['genre_trends']:
            return go.Figure().add_annotation(text="Waiting for data...", showarrow=False, font=dict(color="white"))
        
        recent_time = datetime.now() - timedelta(minutes=5)
        recent_genres = [g for g in self.data_buffers['genre_trends'] if g['timestamp'] > recent_time]
        
        if not recent_genres:
            return go.Figure().add_annotation(text="No data in last 5 minutes", showarrow=False, font=dict(color="white"))
        
        genre_stats = defaultdict(lambda: {'count': 0, 'total_popularity': 0})
        for genre_data in recent_genres:
            genre = genre_data['genre']
            genre_stats[genre]['count'] += 1
            genre_stats[genre]['total_popularity'] += genre_data['popularity']
        
        genres = []
        avg_popularities = []
        
        for genre, stats in genre_stats.items():
            avg_popularity = stats['total_popularity'] / stats['count']
            genres.append(genre)
            avg_popularities.append(avg_popularity)
        
        sorted_data = sorted(zip(genres, avg_popularities), key=lambda x: x[1], reverse=True)[:8]
        genres, avg_popularities = zip(*sorted_data) if sorted_data else ([], [])
        
        fig = px.bar(
            x=genres,
            y=avg_popularities,
            title="Genre Popularity Average (Last 5 Minutes)",
            color=avg_popularities,
            color_continuous_scale="viridis"
        )
        
        fig.update_layout(
            height=350,
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
        if not self.data_buffers['popularity_timeline']:
            return go.Figure().add_annotation(text="Waiting for data...", showarrow=False, font=dict(color="white"))
        
        recent_time = datetime.now() - timedelta(minutes=10)
        recent_timeline = [t for t in self.data_buffers['popularity_timeline'] if t['timestamp'] > recent_time]
        
        if not recent_timeline:
            return go.Figure().add_annotation(text="No data in last 10 minutes", showarrow=False, font=dict(color="white"))
        
        df = pd.DataFrame(recent_timeline)
        
        fig = px.scatter(
            df, 
            x='timestamp', 
            y='popularity',
            color='genre',
            title="Popularity Timeline (Last 10 Minutes)",
            hover_data=['artist', 'title', 'year']
        )
        
        fig.update_layout(
            height=350, 
            margin=dict(l=20, r=20, t=40, b=20),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color="white"),
            title_font_color="white"
        )
        return fig
    
    def create_audio_features_radar(self):
        if not self.data_buffers['audio_features']:
            return go.Figure().add_annotation(text="Waiting for data...", showarrow=False, font=dict(color="white"))
        
        recent_features = list(self.data_buffers['audio_features'])[-30:]
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
            name='Average Audio Features',
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
            title="Audio Features Profile (Last 30 Songs)",
            height=350,
            margin=dict(l=20, r=20, t=40, b=20),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color="white"),
            title_font_color="white"
        )
        
        return fig
    
    def create_real_time_events_chart(self):
        if not self.data_buffers['real_time_events']:
            return go.Figure().add_annotation(text="Waiting for real-time events...", showarrow=False, font=dict(color="white"))
        
        events = list(self.data_buffers['real_time_events'])[-15:]
        
        if not events:
            return go.Figure().add_annotation(text="No real-time events yet", showarrow=False, font=dict(color="white"))
        
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
            height=350,
            margin=dict(l=20, r=20, t=40, b=20),
            showlegend=True,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color="white"),
            title_font_color="white",
            yaxis_title="Event Index"
        )
        
        return fig
    
    def create_performance_metrics(self):
        metrics = self.performance_metrics
        
        return html.Div([
            html.Span(f"Processed: {metrics['messages_processed']}", className="metric"),
            html.Span(f"Rate: {metrics['processing_rate']:.1f} msg/sec", className="metric"),
            html.Span(f"Genres: {metrics['genres_count']}", className="metric"),
            html.Span(f"Timeless: {metrics['timeless_analysis_count']}", className="metric"),
            html.Span(f"Active Events: {metrics['active_events']}", className="metric"),
            html.Span(f"Rollbacks: {metrics['rollback_events']}", className="metric")
        ])
    
    def run(self, host='127.0.0.1', port=8050, debug=False):
        logger.info(f"Spotify Dashboard starting: http://{host}:{port}")
        
        self.app.index_string = '''
        <!DOCTYPE html>
        <html>
            <head>
                {%metas%}
                <title>Spotify Timeless Analytics Dashboard</title>
                {%favicon%}
                {%css%}
                <style>
                    body { 
                        margin: 0; 
                        font-family: "Inter", sans-serif; 
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
                    .status-indicators {
                        display: flex;
                        flex-direction: column;
                        gap: 5px;
                        align-items: center;
                    }
                    .live-indicator { 
                        color: #ff4444; 
                        font-weight: bold; 
                        font-size: 18px;
                        animation: pulse 2s infinite;
                    }
                    .timeless-indicator {
                        color: #FFD700;
                        font-weight: bold;
                        font-size: 14px;
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
                        border: 1px solid rgba(255,255,255,0.1);
                        min-width: 200px;
                    }
                    .chart-card { 
                        flex: 1; 
                        background: rgba(255,255,255,0.05); 
                        border-radius: 16px; 
                        padding: 20px;
                        border: 1px solid rgba(255,255,255,0.1);
                        min-width: 400px;
                    }
                    .chart-card-full { 
                        flex: 1; 
                        background: rgba(255,255,255,0.05); 
                        border-radius: 16px; 
                        padding: 20px;
                        border: 1px solid rgba(255,255,255,0.1);
                    }
                    
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
        self.running = False
        if hasattr(self, 'consumer'):
            self.consumer.close()

if __name__ == "__main__":
    print("SPOTIFY TIMELESS ANALYTICS DASHBOARD")
    print("=" * 50)
    
    dashboard = SpotifyDashboard()
    
    try:
        print("Dashboard starting...")
        print("URL: http://localhost:8050")
        print("Real-time + Timeless charts loading...")
        print("Timeless genre analysis active...")
        print("TIMEOUT SYSTEM: 30s auto-rollback tracking")
        print("Event success rate and volatility monitoring")
        print("Press Ctrl+C to stop")
        print("-" * 50)
        
        dashboard.run(debug=False)
    except KeyboardInterrupt:
        logger.info("Dashboard closing...")
    finally:
        dashboard.stop()
        logger.info("Dashboard closed!")