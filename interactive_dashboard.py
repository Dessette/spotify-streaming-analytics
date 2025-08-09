import dash
from dash import dcc, html, Input, Output, callback
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
import json
import threading
import time
from collections import deque, defaultdict
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SpotifySparkDashboard:
    def __init__(self, postgres_config=None):
        if postgres_config is None:
            postgres_config = {
                'host': 'localhost',
                'port': 5432,
                'database': 'spotify_analytics',
                'user': 'spotify_user',
                'password': 'spotify_pass'
            }
        
        self.postgres_config = postgres_config
        self.app = dash.Dash(__name__)
        
        # SQLAlchemy engine for pandas integration
        self.engine = create_engine(
            f"postgresql://{postgres_config['user']}:{postgres_config['password']}@"
            f"{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
        )
        
        # Data caches
        self.data_cache = {
            'timeless_metrics': None,
            'genre_trends': None,
            'song_analytics': None,
            'real_time_events': None,
            'genre_timeline_data': None,  # New cache for genre timelines
            'last_update': None,
            'cache_duration': 5
        }
        
        self.performance_metrics = {
            'total_songs': 0,
            'total_genres': 0,
            'total_artists': 0,
            'timeless_analyses': 0,
            'last_update': datetime.now(),
            'cache_hits': 0,
            'cache_misses': 0
        }
        
        self.setup_layout()
        self.setup_callbacks()
        self.start_data_refresh_thread()
        
    def get_cached_data(self, data_type, force_refresh=False):
        """Get data with caching mechanism"""
        current_time = datetime.now()
        
        if (not force_refresh and 
            self.data_cache['last_update'] and
            (current_time - self.data_cache['last_update']).total_seconds() < self.data_cache['cache_duration']):
            
            if self.data_cache[data_type] is not None:
                self.performance_metrics['cache_hits'] += 1
                return self.data_cache[data_type]
        
        self.performance_metrics['cache_misses'] += 1
        self.refresh_all_data()
        return self.data_cache.get(data_type)
    
    def refresh_all_data(self):
        """Refresh all data including new genre timeline data"""
        eng = self.engine
    
        # Existing data refresh
        self.data_cache['timeless_metrics'] = pd.read_sql("""
            SELECT tm.*, ROW_NUMBER() OVER (ORDER BY timeless_score DESC) AS ranking
            FROM timeless_genre_metrics tm
            WHERE analysis_date = (SELECT MAX(analysis_date) FROM timeless_genre_metrics)
            ORDER BY timeless_score DESC
        """, eng)
    
        self.data_cache['song_analytics'] = pd.read_sql("""
            SELECT * FROM song_analytics ORDER BY processed_at DESC LIMIT 10000
        """, eng)
    
        self.data_cache['genre_trends'] = pd.read_sql("""
            SELECT * FROM genre_trends ORDER BY analysis_window DESC LIMIT 1000
        """, eng)
    
        self.data_cache['real_time_events'] = pd.read_sql("""
            SELECT * FROM real_time_events ORDER BY created_at DESC LIMIT 1000
        """, eng)
        
        # NEW: Genre timeline data for detailed analysis
        self.data_cache['genre_timeline_data'] = self.get_genre_timeline_data()
    
        # Performance metrics
        songs = self.data_cache['song_analytics']
        self.performance_metrics['total_songs'] = len(songs)
        self.performance_metrics['total_genres'] = songs['genre'].nunique() if 'genre' in songs else 0
        self.performance_metrics['total_artists'] = songs['artist'].nunique() if 'artist' in songs else 0
        self.performance_metrics['timeless_analyses'] = self.data_cache['timeless_metrics']['genre'].nunique() \
            if not self.data_cache['timeless_metrics'].empty else 0
    
        self.data_cache['last_update'] = datetime.now()
        self.performance_metrics['last_update'] = datetime.now()

    def get_genre_timeline_data(self):
        """Get detailed timeline data for genres (like the Alternative Metal chart)"""
        try:
            # Get timeline data for top genres
            timeline_query = """
                WITH top_genres AS (
                    SELECT genre 
                    FROM timeless_genre_metrics 
                    WHERE analysis_date = (SELECT MAX(analysis_date) FROM timeless_genre_metrics)
                    ORDER BY timeless_score DESC 
                    LIMIT 10
                ),
                decade_analysis AS (
                    SELECT 
                        sa.genre,
                        FLOOR(sa.year / 5) * 5 as period_start,
                        FLOOR(sa.year / 5) * 5 + 4 as period_end,
                        COUNT(*) as song_count,
                        ROUND(AVG(sa.popularity), 1) as avg_popularity,
                        ROUND(STDDEV(sa.popularity), 1) as popularity_std,
                        MAX(sa.popularity) as max_popularity,
                        MIN(sa.popularity) as min_popularity,
                        COUNT(DISTINCT sa.artist) as unique_artists
                    FROM song_analytics sa
                    INNER JOIN top_genres tg ON LOWER(TRIM(sa.genre)) = LOWER(TRIM(tg.genre))
                    WHERE sa.year >= 1980 AND sa.year <= 2024
                    GROUP BY sa.genre, FLOOR(sa.year / 5) * 5
                    HAVING COUNT(*) >= 3
                    ORDER BY sa.genre, period_start
                )
                SELECT 
                    genre,
                    CONCAT(period_start, '-', period_end) as period_label,
                    period_start,
                    period_end,
                    song_count,
                    avg_popularity,
                    popularity_std,
                    max_popularity,
                    min_popularity,
                    unique_artists
                FROM decade_analysis
                ORDER BY genre, period_start
            """
            
            return pd.read_sql(timeline_query, self.engine)
            
        except Exception as e:
            logger.error(f"Error getting genre timeline data: {e}")
            return pd.DataFrame()
    
    def start_data_refresh_thread(self):
        """Start background thread for data refresh"""
        def refresh_loop():
            while True:
                try:
                    time.sleep(8)
                    self.refresh_all_data()
                except Exception as e:
                    logger.error(f"Background refresh error: {e}")
                    time.sleep(60)
        
        refresh_thread = threading.Thread(target=refresh_loop, daemon=True)
        refresh_thread.start()
        logger.info("Background data refresh started")
    
    def setup_layout(self):
        self.app.layout = html.Div([
            # Header
            html.Div([
                html.H1("Spotify Spark Analytics Dashboard", className="header-title"),
                html.Div([
                ], className="status-indicators"),
                html.Div(id="performance-metrics", className="performance-metrics")
            ], className="header"),
            
            html.Div([
                # Main metrics
                html.Div([
                    html.Div([dcc.Graph(id="timeless-champion-metric")], className="metric-card"),
                    html.Div([dcc.Graph(id="total-songs-metric")], className="metric-card"),
                    html.Div([dcc.Graph(id="genres-analyzed-metric")], className="metric-card"),
                ], className="metrics-row"),

                # Timeless Rankings
                html.Div([
                    html.Div([dcc.Graph(id="timeless-rankings-chart")], className="chart-card"),
                    html.Div([dcc.Graph(id="timeless-score-distribution")], className="chart-card")
                ], className="charts-row"),

                # NEW: Genre Timeline Analysis (Like Alternative Metal chart)
                html.Div([
                    html.Div([
                        html.H3("Genre Timeline Analysis", style={'color': 'white', 'textAlign': 'center'}),
                        dcc.Dropdown(
                            id='genre-selector',
                            placeholder="Select a genre for detailed timeline...",
                            style={'marginBottom': '10px', 'color': 'black'}
                        ),
                        dcc.Graph(id="genre-timeline-detailed")
                    ], className="chart-card-full")
                ], className="charts-row"),

                # Genre Analysis
                html.Div([
                    html.Div([dcc.Graph(id="genre-trends-spark")], className="chart-card"),
                    html.Div([dcc.Graph(id="popularity-trends-spark")], className="chart-card")
                ], className="charts-row"),
                
                # Advanced Analytics
                html.Div([
                    html.Div([dcc.Graph(id="timeless-timeline-detailed")], className="chart-card-full")
                ], className="charts-row"),

                # Real-time Events & Audio Features
                html.Div([
                    html.Div([dcc.Graph(id="real-time-events-spark")], className="chart-card"),
                    html.Div([dcc.Graph(id="audio-features-by-genre")], className="chart-card")
                ], className="charts-row"),
                
                # Spark Analytics Summary
                html.Div([
                    html.Div([dcc.Graph(id="spark-analytics-summary")], className="chart-card-full")
                ], className="charts-row")
            ], className="main-container"),
            
            # Update intervals
            dcc.Interval(
                id='interval-component',
                interval=5000,  # 5 seconds
                n_intervals=0
            )
        ])
    
    def setup_callbacks(self):
        @self.app.callback(
            [Output('timeless-champion-metric', 'figure'),
             Output('total-songs-metric', 'figure'),
             Output('genres-analyzed-metric', 'figure'),
             Output('timeless-rankings-chart', 'figure'),
             Output('timeless-score-distribution', 'figure'),
             Output('genre-trends-spark', 'figure'),
             Output('popularity-trends-spark', 'figure'),
             Output('timeless-timeline-detailed', 'figure'),
             Output('real-time-events-spark', 'figure'),
             Output('audio-features-by-genre', 'figure'),
             Output('spark-analytics-summary', 'figure'),
             Output('performance-metrics', 'children'),
             Output('genre-selector', 'options')],  # NEW: Genre selector options
            [Input('interval-component', 'n_intervals')]
        )
        def update_dashboard(n):
            return (
                self.create_timeless_champion_metric(),
                self.create_total_songs_metric(),
                self.create_genres_analyzed_metric(),
                self.create_timeless_rankings_chart(),
                self.create_timeless_score_distribution(),
                self.create_genre_trends_spark(),
                self.create_popularity_trends_spark(),
                self.create_timeless_timeline_detailed(),
                self.create_real_time_events_spark(),
                self.create_audio_features_by_genre(),
                self.create_spark_analytics_summary(),
                self.create_performance_metrics(),
                self.create_genre_selector_options()  # NEW
            )
        
        # NEW: Genre timeline callback
        @self.app.callback(
            Output('genre-timeline-detailed', 'figure'),
            [Input('genre-selector', 'value')]
        )
        def update_genre_timeline(selected_genre):
            return self.create_genre_timeline_chart(selected_genre)
    
    def create_genre_selector_options(self):
        """Create options for genre selector dropdown"""
        timeless_data = self.get_cached_data('timeless_metrics')
        
        if timeless_data is None or timeless_data.empty:
            return []
        
        # Get top 10 genres
        top_genres = timeless_data.head(10)
        options = [
            {'label': f"{genre.title()} (Score: {score:.1f})", 'value': genre}
            for genre, score in zip(top_genres['genre'], top_genres['timeless_score'])
        ]
        
        return options
    
    def create_genre_timeline_chart(self, selected_genre):
        """Create detailed genre timeline chart (like Alternative Metal example)"""
        if not selected_genre:
            return go.Figure().add_annotation(
                text="Select a genre to see detailed timeline analysis",
                showarrow=False,
                font=dict(color="white", size=16),
                x=0.5, y=0.5
            ).update_layout(
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                height=400
            )
        
        # Get timeline data for selected genre
        timeline_data = self.get_cached_data('genre_timeline_data')
        timeless_data = self.get_cached_data('timeless_metrics')
        
        if timeline_data is None or timeline_data.empty:
            return go.Figure().add_annotation(
                text="No timeline data available",
                showarrow=False,
                font=dict(color="white")
            )
        
        # Filter for selected genre
        genre_data = timeline_data[timeline_data['genre'].str.lower() == selected_genre.lower()]
        
        if genre_data.empty:
            return go.Figure().add_annotation(
                text=f"No timeline data found for {selected_genre}",
                showarrow=False,
                font=dict(color="white")
            )
        
        # Get timeless score for this genre
        genre_score = 0
        if not timeless_data.empty:
            genre_timeless = timeless_data[timeless_data['genre'].str.lower() == selected_genre.lower()]
            if not genre_timeless.empty:
                genre_score = genre_timeless.iloc[0]['timeless_score']
        
        # Create the chart (similar to Alternative Metal example)
        fig = go.Figure()
        
        # Add song count bars
        fig.add_trace(go.Bar(
            x=genre_data['period_label'],
            y=genre_data['song_count'],
            name='Song Count',
            marker_color='rgba(70, 130, 180, 0.7)',
            yaxis='y2',
            hovertemplate='<b>%{x}</b><br>' +
                         'Songs: %{y}<br>' +
                         '<extra></extra>'
        ))
        
        # Add popularity line
        fig.add_trace(go.Scatter(
            x=genre_data['period_label'],
            y=genre_data['avg_popularity'],
            mode='lines+markers',
            name='Avg Popularity',
            line=dict(color='#00FF7F', width=3),
            marker=dict(size=8, color='#00FF7F'),
            yaxis='y',
            hovertemplate='<b>%{x}</b><br>' +
                         'Avg Popularity: %{y:.1f}<br>' +
                         '<extra></extra>'
        ))
        
        # Update layout (similar to Alternative Metal style)
        fig.update_layout(
            title=f"{selected_genre.title()} - Timeless Journey (Score: {genre_score:.1f}/100)",
            title_font=dict(size=18, color='white'),
            height=400,
            margin=dict(l=20, r=20, t=60, b=40),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(40,40,40,0.8)',
            font=dict(color="white"),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            xaxis=dict(
                title="Time Periods",
                title_font=dict(color='white'),
                tickfont=dict(color='white'),
                gridcolor='rgba(255,255,255,0.2)'
            ),
            yaxis=dict(
                title="Average Popularity",
                title_font=dict(color='white'),
                tickfont=dict(color='white'),
                gridcolor='rgba(255,255,255,0.2)',
                side='left'
            ),
            yaxis2=dict(
                title="Song Count",
                title_font=dict(color='white'),
                tickfont=dict(color='white'),
                overlaying='y',
                side='right',
                gridcolor='rgba(255,255,255,0.1)'
            )
        )
        
        return fig
    
    # [Keep all existing chart creation methods - they remain the same]
    def create_timeless_champion_metric(self):
        timeless_data = self.get_cached_data('timeless_metrics')
        
        if timeless_data is None or timeless_data.empty:
            champion_text = "Analyzing..."
            score_text = "..."
        else:
            top_genre = timeless_data.iloc[0]
            champion_text = top_genre['genre'].title()
            score_text = f"{top_genre['timeless_score']:.1f}/100"
        
        fig = go.Figure()
        fig.add_annotation(
            text=f"<b>Timeless Genre</b><br><span style='font-size:20px; color:#FFD700'>{champion_text}</span><br><span style='font-size:16px; color:#1DB954'>{score_text}</span>",
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
    
    def create_total_songs_metric(self):
        total_songs = self.performance_metrics['total_songs']
        
        fig = go.Figure()
        fig.add_trace(go.Indicator(
            mode = "number",
            value = total_songs,
            title = {"text": "Songs Analyzed", "font": {"size": 14}},
            number = {"font": {"size": 40, "color": "#1DB954"}}
        ))
        
        fig.update_layout(height=200, margin=dict(l=20, r=20, t=40, b=20), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        return fig
    
    def create_genres_analyzed_metric(self):
        total_genres = self.performance_metrics['total_genres']
        fig = go.Figure()
        fig.add_trace(go.Indicator(mode="number", value=total_genres,
            title={"text": "Genres Analyzed"}, number={"font":{"size":40,"color":"#1DB954"}}))
        fig.update_layout(height=200, margin=dict(l=20,r=20,t=40,b=20),
                      paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        return fig
    
    def create_timeless_rankings_chart(self):
        timeless_data = self.get_cached_data('timeless_metrics')
        
        if timeless_data is None or timeless_data.empty:
            return go.Figure().add_annotation(text="No timeless data available", showarrow=False, font=dict(color="white"))
        
        top_10 = timeless_data.head(10)
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=top_10['genre'],
            y=top_10['timeless_score'],
            text=[f"{score:.1f}" for score in top_10['timeless_score']],
            textposition='outside',
            hovertemplate='<b>%{x}</b><br>' +
                         'Timeless Score: %{y:.1f}<br>' +
                         'Ranking: %{customdata}<br>' +
                         '<extra></extra>',
            customdata=top_10['ranking'],
            marker=dict(
                color=top_10['timeless_score'],
                colorscale='Viridis',
                colorbar=dict(title="Timeless Score")
            )
        ))
        
        fig.update_layout(
            title="Top 10 Timeless Genres",
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
    
    def create_timeless_score_distribution(self):
        timeless_data = self.get_cached_data('timeless_metrics')
        
        if timeless_data is None or timeless_data.empty:
            return go.Figure().add_annotation(text="No distribution data", showarrow=False, font=dict(color="white"))
        
        fig = go.Figure()
        
        fig.add_trace(go.Histogram(
            x=timeless_data['timeless_score'],
            name="Timeless Scores",
            opacity=0.7,
            nbinsx=15,
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
        
        avg_score = timeless_data['timeless_score'].mean()
        fig.add_vline(
            x=avg_score, 
            line_dash="dash", 
            line_color="red",
            annotation_text=f"Avg: {avg_score:.1f}",
            annotation_position="top"
        )
        
        return fig
    
    def create_genre_trends_spark(self):
        trends_data = self.get_cached_data('genre_trends')
        
        if trends_data is None or trends_data.empty:
            return go.Figure().add_annotation(text="No trend data available", showarrow=False, font=dict(color="white"))
        
        recent_trends = trends_data.groupby('genre').agg({
            'avg_popularity': 'mean',
            'song_count': 'sum'
        }).reset_index()
        
        recent_trends = recent_trends.sort_values('avg_popularity', ascending=False).head(10)
        
        fig = px.bar(
            recent_trends,
            x='genre',
            y='avg_popularity',
            title="Genre Popularity Trends",
            color='avg_popularity',
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
    
    def create_popularity_trends_spark(self):
        songs_data = self.get_cached_data('song_analytics')
        
        if songs_data is None or songs_data.empty:
            return go.Figure().add_annotation(text="No song data available", showarrow=False, font=dict(color="white"))
        
        songs_data['processed_at'] = pd.to_datetime(songs_data['processed_at'])
        songs_data['time_bucket'] = songs_data['processed_at'].dt.floor('10T')
        
        time_trends = songs_data.groupby(['time_bucket', 'genre']).agg({
            'popularity': 'mean'
        }).reset_index()
        
        top_genres = songs_data['genre'].value_counts().head(5).index
        time_trends_filtered = time_trends[time_trends['genre'].isin(top_genres)]
        
        fig = px.line(
            time_trends_filtered,
            x='time_bucket',
            y='popularity',
            color='genre',
            title="Popularity Trends Over Time (10-min intervals)"
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
    
    def create_timeless_timeline_detailed(self):
        timeless_data = self.get_cached_data('timeless_metrics')
        
        if timeless_data is None or timeless_data.empty:
            return go.Figure().add_annotation(text="No timeline data", showarrow=False, font=dict(color="white"))
        
        top_5 = timeless_data.head(5)
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            name='Timeless Score',
            x=top_5['genre'],
            y=top_5['timeless_score'],
            marker_color='#1DB954'
        ))
        
        fig.add_trace(go.Bar(
            name='Consistency Score',
            x=top_5['genre'],
            y=top_5['consistency_score'],
            marker_color='#1ED760'
        ))
        
        fig.add_trace(go.Bar(
            name='Longevity Score',
            x=top_5['genre'],
            y=top_5['longevity_score'],
            marker_color='#FFD700'
        ))
        
        fig.update_layout(
            title="Detailed Timeless Metrics Breakdown (Top 5 Genres)",
            xaxis_title="Genres",
            yaxis_title="Score",
            height=400,
            margin=dict(l=20, r=20, t=40, b=20),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color="white"),
            title_font_color="white",
            barmode='group'
        )
        
        fig.update_xaxes(tickangle=45)
        return fig
    
    def create_real_time_events_spark(self):
        events_data = self.get_cached_data('real_time_events')
        songs_data = self.get_cached_data('song_analytics')
        
        if events_data is None or events_data.empty:
            return go.Figure().add_annotation(text="No real-time events", showarrow=False, font=dict(color="white"))
        
        if songs_data is None or songs_data.empty:
            # Fallback to event type if no song data
            event_counts = events_data['event_type'].value_counts()
            fig = go.Figure(data=[go.Pie(
                labels=event_counts.index,
                values=event_counts.values,
                hole=.3
            )])
        else:
            # Join events with song data to get genre information
            events_with_genre = events_data.merge(
                songs_data[['title', 'artist', 'genre', 'year']].drop_duplicates(),
                left_on=['song_title', 'artist'],
                right_on=['title', 'artist'],
                how='left'
            )
            
            # Fill missing genres
            events_with_genre['genre'] = events_with_genre['genre'].fillna('Unknown')
            
            # Group by genre and create detailed information
            genre_events = events_with_genre.groupby('genre').agg({
                'event_type': lambda x: list(x),
                'song_title': lambda x: list(x),
                'artist': lambda x: list(x), 
                'year': lambda x: list(x),
                'reason': lambda x: list(x),
                'boost_amount': lambda x: list(x),
                'created_at': lambda x: list(x)
            }).reset_index()
            
            # Calculate event counts per genre
            genre_events['event_count'] = genre_events['event_type'].apply(len)
            
            # Create detailed hover text
            hover_texts = []
            for _, row in genre_events.iterrows():
                events_list = []
                for i in range(len(row['song_title'])):
                    song = row['song_title'][i] if row['song_title'][i] else 'Unknown'
                    artist = row['artist'][i] if row['artist'][i] else 'Unknown'
                    year = row['year'][i] if pd.notna(row['year'][i]) else 'Unknown'
                    event_type = row['event_type'][i] if row['event_type'][i] else 'Unknown'
                    reason = row['reason'][i] if row['reason'][i] else 'Unknown'
                    boost = row['boost_amount'][i] if pd.notna(row['boost_amount'][i]) else 0
                    
                    event_detail = f"• {song} - {artist} ({year})<br>  Event: {event_type}"
                    if reason != 'Unknown' and reason != 'nan':
                        event_detail += f" ({reason})"
                    if boost > 0:
                        event_detail += f" [+{boost}]"
                    
                    events_list.append(event_detail)
                
                # Limit to top 5 events for readability
                if len(events_list) > 5:
                    display_events = events_list[:5] + [f"... and {len(events_list) - 5} more"]
                else:
                    display_events = events_list
                
                hover_text = f"<b>{row['genre'].title()}</b><br>" + \
                           f"Total Events: {row['event_count']}<br><br>" + \
                           "<br>".join(display_events)
                
                hover_texts.append(hover_text)
            
            # Create pie chart with enhanced hover info
            fig = go.Figure(data=[go.Pie(
                labels=[genre.title() for genre in genre_events['genre']],
                values=genre_events['event_count'],
                hovertemplate='%{customdata}<extra></extra>',
                customdata=hover_texts,
                hole=.3,
                marker=dict(
                    colors=px.colors.qualitative.Set3
                )
            )])
        
        fig.update_layout(
            title="Real-time Events by Genre",
            height=350,
            margin=dict(l=20, r=20, t=40, b=20),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color="white"),
            title_font_color="white",
            showlegend=True,
            legend=dict(
                orientation="v",
                yanchor="middle",
                y=0.5,
                xanchor="left",
                x=1.01
            )
        )
        
        return fig
    
    def create_audio_features_by_genre(self):
        songs_data = self.get_cached_data('song_analytics')
        
        if songs_data is None or songs_data.empty:
            return go.Figure().add_annotation(text="No audio features data", showarrow=False, font=dict(color="white"))
        
        top_genres = songs_data['genre'].value_counts().head(5).index
        audio_features = songs_data[songs_data['genre'].isin(top_genres)].groupby('genre').agg({
            'energy': 'mean',
            'danceability': 'mean',
            'valence': 'mean',
            'acousticness': 'mean'
        }).reset_index()
        
        fig = go.Figure()
        
        for feature in ['energy', 'danceability', 'valence', 'acousticness']:
            fig.add_trace(go.Bar(
                name=feature.title(),
                x=audio_features['genre'],
                y=audio_features[feature]
            ))
        
        fig.update_layout(
            title="Audio Features by Genre",
            xaxis_title="Genres",
            yaxis_title="Average Score",
            height=350,
            margin=dict(l=20, r=20, t=40, b=20),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color="white"),
            title_font_color="white",
            barmode='group'
        )
        
        fig.update_xaxes(tickangle=45)
        return fig
    
    def create_spark_analytics_summary(self):
        timeless_data = self.get_cached_data('timeless_metrics')
        
        if timeless_data is None or timeless_data.empty:
            return go.Figure().add_annotation(text="No summary data", showarrow=False, font=dict(color="white"))
        
        top_6 = timeless_data.head(6)
        
        fig = go.Figure(data=[go.Table(
            header=dict(
                values=['Rank', 'Genre', 'Timeless Score', 'Consistency', 'Longevity', 'Decades'],
                fill_color='rgba(29, 185, 84, 0.8)',
                align='center',
                font=dict(color='white', size=14),
                height=40
            ),
            cells=dict(
                values=[
                    top_6['ranking'],
                    [genre.title() for genre in top_6['genre']],
                    [f"{score:.1f}" for score in top_6['timeless_score']],
                    [f"{score:.1f}" for score in top_6['consistency_score']],
                    [f"{score:.1f}" for score in top_6['longevity_score']],
                    top_6['decade_presence']
                ],
                fill_color='rgba(29, 185, 84, 0.3)',
                align='center',
                font=dict(color='white', size=12),
                height=35
            )
        )])
        
        fig.update_layout(
            title="Spark Analytics Summary - Top Timeless Genres",
            height=300,
            margin=dict(l=20, r=20, t=60, b=20),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color="white"),
            title_font_color="white"
        )
        
        return fig
    
    def create_performance_metrics(self):
        metrics = self.performance_metrics
        
        return html.Div([
            html.Span(f"Songs: {metrics['total_songs']}", className="metric"),
            html.Span(f"Genres: {metrics['total_genres']}", className="metric"),
            html.Span(f"Artists: {metrics['total_artists']}", className="metric"),
            html.Span(f"Timeless: {metrics['timeless_analyses']}", className="metric"),
            html.Span(f"Cache Hits: {metrics['cache_hits']}", className="metric"),
            html.Span(f"Last Update: {metrics['last_update'].strftime('%H:%M:%S')}", className="metric")
        ])
    
    def run(self, host='127.0.0.1', port=8050, debug=False):
        logger.info(f"Enhanced Spotify Dashboard starting: http://{host}:{port}")
        
        self.refresh_all_data()
        
        self.app.index_string = '''
        <!DOCTYPE html>
        <html>
            <head>
                {%metas%}
                <title>Spotify Spark Analytics Dashboard</title>
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
                        background: linear-gradient(90deg, #FF6B35, #FF8E53); 
                        padding: 20px; 
                        display: flex; 
                        justify-content: space-between; 
                        align-items: center;
                        box-shadow: 0 4px 20px rgba(255, 107, 53, 0.3);
                    }
                    .header-title { 
                        margin: 0; 
                        color: white; 
                        font-weight: 700;
                        font-size: 28px;
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

if __name__ == "__main__":
    print("ENHANCED SPOTIFY DASHBOARD WITH GENRE TIMELINE")
    print("=" * 60)
    print("🎵 NEW: Detailed Genre Timeline Analysis")
    print("📊 Interactive genre selection dropdown")
    print("📈 Alternative Metal-style charts for all genres")
    print("⚡ Real-time data updates")
    
    dashboard = SpotifySparkDashboard()
    
    try:
        print("Enhanced dashboard starting...")
        print("URL: http://localhost:8050")
        print("✨ NEW: Genre Timeline section added")
        print("🔍 Select any genre for detailed timeline analysis")
        print("📊 Similar to Alternative Metal chart style")
        print("Press Ctrl+C to stop")
        print("-" * 60)
        
        dashboard.run(debug=False)
    except KeyboardInterrupt:
        logger.info("Enhanced dashboard closing...")
    finally:
        logger.info("Enhanced dashboard closed!")