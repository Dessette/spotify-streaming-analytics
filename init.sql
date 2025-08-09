-- Spotify Analytics Database Tables
-- Run this to ensure all tables exist

-- Create users and setup permissions
DO $$
BEGIN
    -- Create spotify_user if not exists
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'spotify_user') THEN
        CREATE USER spotify_user WITH PASSWORD 'spotify_pass';
    END IF;
    
    -- Grant permissions
    GRANT ALL PRIVILEGES ON DATABASE spotify_analytics TO spotify_user;
    GRANT ALL PRIVILEGES ON SCHEMA public TO spotify_user;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO spotify_user;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO spotify_user;
    
    -- Set default privileges for future objects
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO spotify_user;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO spotify_user;
END
$$;

-- Song Analytics Table
CREATE TABLE IF NOT EXISTS song_analytics (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    artist VARCHAR(255),
    genre VARCHAR(100),
    year INTEGER,
    popularity INTEGER,
    energy INTEGER,
    danceability INTEGER,
    valence INTEGER,
    acousticness INTEGER,
    bpm INTEGER,
    length INTEGER,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    event_type VARCHAR(50),
    source_timestamp TIMESTAMP
);

-- Genre Trends Table
CREATE TABLE IF NOT EXISTS genre_trends (
    id SERIAL PRIMARY KEY,
    genre VARCHAR(100),
    avg_popularity DECIMAL(5,2),
    song_count INTEGER,
    analysis_window TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    std_popularity DECIMAL(5,2),
    max_popularity INTEGER,
    min_popularity INTEGER,
    unique_artists INTEGER
);

-- Real-time Events Table
CREATE TABLE IF NOT EXISTS real_time_events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50),
    song_title VARCHAR(255),
    artist VARCHAR(255),
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    event_id VARCHAR(100),
    boost_amount INTEGER,
    reason VARCHAR(100),
    original_popularity INTEGER,
    has_timeout BOOLEAN
);

-- Timeless Genre Metrics Table
CREATE TABLE IF NOT EXISTS timeless_genre_metrics (
    id SERIAL PRIMARY KEY,
    genre VARCHAR(100),
    analysis_date DATE,
    analysis_period_years INTEGER,
    timeless_score DECIMAL(5,2),
    consistency_score DECIMAL(5,2),
    longevity_score DECIMAL(5,2),
    variance_score DECIMAL(5,2),
    peak_stability DECIMAL(5,2),
    decade_presence INTEGER,
    year_span INTEGER,
    total_songs INTEGER,
    total_unique_artists INTEGER,
    period_stats TEXT,
    analysis_metadata TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_song_analytics_genre ON song_analytics(genre);
CREATE INDEX IF NOT EXISTS idx_song_analytics_year ON song_analytics(year);
CREATE INDEX IF NOT EXISTS idx_song_analytics_processed_at ON song_analytics(processed_at);
CREATE INDEX IF NOT EXISTS idx_genre_trends_genre ON genre_trends(genre);
CREATE INDEX IF NOT EXISTS idx_genre_trends_window ON genre_trends(analysis_window);
CREATE INDEX IF NOT EXISTS idx_real_time_events_type ON real_time_events(event_type);
CREATE INDEX IF NOT EXISTS idx_real_time_events_created ON real_time_events(created_at);
CREATE INDEX IF NOT EXISTS idx_timeless_genre ON timeless_genre_metrics(genre);
CREATE INDEX IF NOT EXISTS idx_timeless_analysis_date ON timeless_genre_metrics(analysis_date);

-- Grant permissions on new tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO spotify_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO spotify_user;