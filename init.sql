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
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    event_type VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS genre_trends (
    id SERIAL PRIMARY KEY,
    genre VARCHAR(100),
    avg_popularity DECIMAL(5,2),
    song_count INTEGER,
    analysis_window TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS real_time_events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50),
    song_title VARCHAR(255),
    artist VARCHAR(255),
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- İndeksler
CREATE INDEX IF NOT EXISTS idx_song_analytics_genre ON song_analytics(genre);
CREATE INDEX IF NOT EXISTS idx_song_analytics_year ON song_analytics(year);
CREATE INDEX IF NOT EXISTS idx_genre_trends_window ON genre_trends(analysis_window);
CREATE INDEX IF NOT EXISTS idx_real_time_events_type ON real_time_events(event_type);
