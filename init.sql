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

CREATE TABLE IF NOT EXISTS timeless_genre_metrics (
    id SERIAL PRIMARY KEY,
    genre VARCHAR(100) NOT NULL,
    analysis_date DATE NOT NULL,
    analysis_period_years INTEGER NOT NULL,
    
    timeless_score DECIMAL(5,2) NOT NULL,
    consistency_score DECIMAL(5,2) NOT NULL,
    longevity_score DECIMAL(5,2) NOT NULL,
    variance_score DECIMAL(5,2) NOT NULL,
    peak_stability DECIMAL(5,2) NOT NULL,
    
    decade_presence INTEGER NOT NULL,
    year_span INTEGER NOT NULL,
    total_songs INTEGER NOT NULL,
    unique_artists INTEGER NOT NULL,
    
    period_stats JSONB,
    analysis_metadata JSONB,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_genre_analysis UNIQUE(genre, analysis_date, analysis_period_years),
    CONSTRAINT valid_scores CHECK (
        timeless_score >= 0 AND timeless_score <= 100 AND
        consistency_score >= 0 AND consistency_score <= 100 AND
        longevity_score >= 0 AND longevity_score <= 100 AND
        variance_score >= 0 AND variance_score <= 100 AND
        peak_stability >= 0 AND peak_stability <= 100
    )
);

CREATE TABLE IF NOT EXISTS timeless_rankings_history (
    id SERIAL PRIMARY KEY,
    analysis_date DATE NOT NULL,
    analysis_period_years INTEGER NOT NULL,
    genre VARCHAR(100) NOT NULL,
    ranking_position INTEGER NOT NULL,
    timeless_score DECIMAL(5,2) NOT NULL,
    previous_position INTEGER,
    position_change INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT valid_ranking CHECK (ranking_position > 0)
);

CREATE TABLE IF NOT EXISTS genre_period_analysis (
    id SERIAL PRIMARY KEY,
    genre VARCHAR(100) NOT NULL,
    period_start_year INTEGER NOT NULL,
    period_end_year INTEGER NOT NULL,
    analysis_date DATE NOT NULL,
    
    song_count INTEGER NOT NULL,
    avg_popularity DECIMAL(5,2) NOT NULL,
    std_popularity DECIMAL(5,2) NOT NULL,
    max_popularity INTEGER NOT NULL,
    min_popularity INTEGER NOT NULL,
    unique_artists INTEGER NOT NULL,
    
    avg_energy DECIMAL(5,2),
    avg_danceability DECIMAL(5,2),
    avg_valence DECIMAL(5,2),
    avg_acousticness DECIMAL(5,2),
    
    period_metadata JSONB,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT valid_period CHECK (period_start_year <= period_end_year),
    CONSTRAINT valid_popularity CHECK (avg_popularity >= 0 AND avg_popularity <= 100)
);

CREATE TABLE IF NOT EXISTS timeless_insights (
    id SERIAL PRIMARY KEY,
    insight_type VARCHAR(50) NOT NULL,
    genre VARCHAR(100),
    insight_title VARCHAR(255) NOT NULL,
    insight_description TEXT NOT NULL,
    confidence_score DECIMAL(3,2),
    
    supporting_data JSONB,
    related_genres VARCHAR(100)[],
    
    analysis_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS real_time_timeless_impact (
    id SERIAL PRIMARY KEY,
    event_id INTEGER,
    genre VARCHAR(100) NOT NULL,
    
    boost_amount INTEGER NOT NULL,
    predicted_impact VARCHAR(20) NOT NULL,
    consistency_risk VARCHAR(20) NOT NULL,
    
    predicted_score_change DECIMAL(5,2),
    actual_score_change DECIMAL(5,2),
    
    event_reason VARCHAR(100),
    song_title VARCHAR(255),
    artist VARCHAR(255),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (event_id) REFERENCES real_time_events(id)
);

CREATE TABLE IF NOT EXISTS genre_timeless_timeline (
    id SERIAL PRIMARY KEY,
    genre VARCHAR(100) NOT NULL,
    timeline_date DATE NOT NULL,
    
    period_label VARCHAR(20) NOT NULL,
    period_start_year INTEGER NOT NULL,
    period_end_year INTEGER NOT NULL,
    
    avg_popularity DECIMAL(5,2) NOT NULL,
    song_count INTEGER NOT NULL,
    unique_artists INTEGER NOT NULL,
    stability_score DECIMAL(5,2),
    
    timeline_position INTEGER NOT NULL,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_genre_timeline UNIQUE(genre, timeline_date, period_label)
);

CREATE TABLE IF NOT EXISTS analytics_performance_metrics (
    id SERIAL PRIMARY KEY,
    metric_date DATE NOT NULL,
    metric_hour INTEGER NOT NULL,
    
    messages_processed INTEGER NOT NULL DEFAULT 0,
    timeless_analyses_completed INTEGER NOT NULL DEFAULT 0,
    genres_analyzed INTEGER NOT NULL DEFAULT 0,
    total_data_points INTEGER NOT NULL DEFAULT 0,
    
    avg_processing_rate DECIMAL(8,2),
    avg_analysis_duration DECIMAL(8,2),
    
    processing_errors INTEGER NOT NULL DEFAULT 0,
    analysis_errors INTEGER NOT NULL DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_metric_hour UNIQUE(metric_date, metric_hour)
);

CREATE INDEX IF NOT EXISTS idx_song_analytics_genre ON song_analytics(genre);
CREATE INDEX IF NOT EXISTS idx_song_analytics_year ON song_analytics(year);
CREATE INDEX IF NOT EXISTS idx_song_analytics_processed_at ON song_analytics(processed_at);
CREATE INDEX IF NOT EXISTS idx_genre_trends_window ON genre_trends(analysis_window);
CREATE INDEX IF NOT EXISTS idx_real_time_events_type ON real_time_events(event_type);
CREATE INDEX IF NOT EXISTS idx_real_time_events_created_at ON real_time_events(created_at);

CREATE INDEX IF NOT EXISTS idx_timeless_metrics_genre ON timeless_genre_metrics(genre);
CREATE INDEX IF NOT EXISTS idx_timeless_metrics_date ON timeless_genre_metrics(analysis_date);
CREATE INDEX IF NOT EXISTS idx_timeless_metrics_score ON timeless_genre_metrics(timeless_score DESC);
CREATE INDEX IF NOT EXISTS idx_timeless_metrics_period ON timeless_genre_metrics(analysis_period_years);

CREATE INDEX IF NOT EXISTS idx_rankings_history_date ON timeless_rankings_history(analysis_date);
CREATE INDEX IF NOT EXISTS idx_rankings_history_genre ON timeless_rankings_history(genre);
CREATE INDEX IF NOT EXISTS idx_rankings_history_position ON timeless_rankings_history(ranking_position);

CREATE INDEX IF NOT EXISTS idx_period_analysis_genre ON genre_period_analysis(genre);
CREATE INDEX IF NOT EXISTS idx_period_analysis_years ON genre_period_analysis(period_start_year, period_end_year);
CREATE INDEX IF NOT EXISTS idx_period_analysis_date ON genre_period_analysis(analysis_date);

CREATE INDEX IF NOT EXISTS idx_timeless_insights_type ON timeless_insights(insight_type);
CREATE INDEX IF NOT EXISTS idx_timeless_insights_genre ON timeless_insights(genre);
CREATE INDEX IF NOT EXISTS idx_timeless_insights_date ON timeless_insights(analysis_date);
CREATE INDEX IF NOT EXISTS idx_timeless_insights_active ON timeless_insights(is_active);

CREATE INDEX IF NOT EXISTS idx_rt_impact_genre ON real_time_timeless_impact(genre);
CREATE INDEX IF NOT EXISTS idx_rt_impact_created_at ON real_time_timeless_impact(created_at);
CREATE INDEX IF NOT EXISTS idx_rt_impact_prediction ON real_time_timeless_impact(predicted_impact);

CREATE INDEX IF NOT EXISTS idx_timeline_genre ON genre_timeless_timeline(genre);
CREATE INDEX IF NOT EXISTS idx_timeline_date ON genre_timeless_timeline(timeline_date);
CREATE INDEX IF NOT EXISTS idx_timeline_position ON genre_timeless_timeline(timeline_position);

CREATE INDEX IF NOT EXISTS idx_performance_date ON analytics_performance_metrics(metric_date);
CREATE INDEX IF NOT EXISTS idx_performance_hour ON analytics_performance_metrics(metric_date, metric_hour);

CREATE OR REPLACE VIEW v_top_timeless_genres AS
SELECT 
    genre,
    analysis_date,
    analysis_period_years,
    timeless_score,
    consistency_score,
    longevity_score,
    decade_presence,
    total_songs,
    ROW_NUMBER() OVER (PARTITION BY analysis_date, analysis_period_years ORDER BY timeless_score DESC) as ranking
FROM timeless_genre_metrics
WHERE analysis_date = (SELECT MAX(analysis_date) FROM timeless_genre_metrics);

CREATE OR REPLACE VIEW v_genre_stability_trends AS
SELECT 
    genre,
    analysis_date,
    timeless_score,
    consistency_score,
    LAG(timeless_score) OVER (PARTITION BY genre ORDER BY analysis_date) as previous_score,
    timeless_score - LAG(timeless_score) OVER (PARTITION BY genre ORDER BY analysis_date) as score_change
FROM timeless_genre_metrics
ORDER BY genre, analysis_date;

CREATE OR REPLACE VIEW v_most_stable_genres AS
SELECT 
    genre,
    AVG(consistency_score) as avg_consistency,
    AVG(variance_score) as avg_variance,
    AVG(peak_stability) as avg_peak_stability,
    AVG(timeless_score) as avg_timeless_score,
    COUNT(*) as analysis_count
FROM timeless_genre_metrics
GROUP BY genre
HAVING COUNT(*) >= 3
ORDER BY avg_consistency DESC, avg_variance DESC, avg_peak_stability DESC;

CREATE OR REPLACE VIEW v_recent_insights AS
SELECT 
    insight_type,
    genre,
    insight_title,
    insight_description,
    confidence_score,
    analysis_date,
    created_at,
    CASE 
        WHEN confidence_score >= 0.8 THEN 'High Confidence'
        WHEN confidence_score >= 0.6 THEN 'Medium Confidence'
        ELSE 'Low Confidence'
    END as confidence_level
FROM timeless_insights
WHERE is_active = TRUE 
AND analysis_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY confidence_score DESC, created_at DESC;

CREATE OR REPLACE FUNCTION get_genre_timeless_rank(p_genre VARCHAR(100), p_analysis_date DATE DEFAULT CURRENT_DATE)
RETURNS INTEGER AS $$
DECLARE
    genre_rank INTEGER;
BEGIN
    SELECT ranking INTO genre_rank
    FROM v_top_timeless_genres
    WHERE genre = p_genre AND analysis_date = p_analysis_date;
    
    RETURN COALESCE(genre_rank, 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_genre_timeline_summary(p_genre VARCHAR(100))
RETURNS TABLE(
    total_periods INTEGER,
    first_period VARCHAR(20),
    last_period VARCHAR(20),
    avg_popularity DECIMAL(5,2),
    best_period VARCHAR(20),
    best_period_popularity DECIMAL(5,2)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*)::INTEGER as total_periods,
        MIN(period_label) as first_period,
        MAX(period_label) as last_period,
        AVG(gt.avg_popularity)::DECIMAL(5,2) as avg_popularity,
        (SELECT period_label FROM genre_timeless_timeline gt2 
         WHERE gt2.genre = p_genre 
         ORDER BY gt2.avg_popularity DESC LIMIT 1) as best_period,
        MAX(gt.avg_popularity) as best_period_popularity
    FROM genre_timeless_timeline gt
    WHERE gt.genre = p_genre;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_timeless_metrics_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_timeless_metrics_timestamp
    BEFORE UPDATE ON timeless_genre_metrics
    FOR EACH ROW
    EXECUTE FUNCTION update_timeless_metrics_timestamp();

CREATE OR REPLACE FUNCTION insert_ranking_history()
RETURNS TRIGGER AS $$
DECLARE
    current_rank INTEGER;
    prev_rank INTEGER;
BEGIN
    SELECT COUNT(*) + 1 INTO current_rank
    FROM timeless_genre_metrics tm
    WHERE tm.analysis_date = NEW.analysis_date 
    AND tm.analysis_period_years = NEW.analysis_period_years
    AND tm.timeless_score > NEW.timeless_score;
    
    SELECT ranking_position INTO prev_rank
    FROM timeless_rankings_history trh
    WHERE trh.genre = NEW.genre 
    AND trh.analysis_period_years = NEW.analysis_period_years
    AND trh.analysis_date < NEW.analysis_date
    ORDER BY trh.analysis_date DESC
    LIMIT 1;
    
    INSERT INTO timeless_rankings_history (
        analysis_date,
        analysis_period_years,
        genre,
        ranking_position,
        timeless_score,
        previous_position,
        position_change
    ) VALUES (
        NEW.analysis_date,
        NEW.analysis_period_years,
        NEW.genre,
        current_rank,
        NEW.timeless_score,
        prev_rank,
        CASE WHEN prev_rank IS NOT NULL THEN prev_rank - current_rank ELSE NULL END
    );
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_insert_ranking_history
    AFTER INSERT ON timeless_genre_metrics
    FOR EACH ROW
    EXECUTE FUNCTION insert_ranking_history();

INSERT INTO analytics_performance_metrics (metric_date, metric_hour, messages_processed, timeless_analyses_completed, genres_analyzed)
VALUES (CURRENT_DATE, EXTRACT(hour FROM CURRENT_TIME)::INTEGER, 0, 0, 0)
ON CONFLICT (metric_date, metric_hour) DO NOTHING;