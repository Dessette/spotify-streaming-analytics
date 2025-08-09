#!/bin/bash

# PostgreSQL User Fix Script for Spotify Analytics
# Fixes the "role spotify_user does not exist" error

set -e

echo "🔧 FIXING POSTGRESQL USER ISSUE"
echo "================================"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Step 1: Check if PostgreSQL container is running
print_status "Checking PostgreSQL container status..."

if ! docker ps | grep -q "postgres"; then
    print_error "PostgreSQL container is not running!"
    print_status "Starting PostgreSQL container..."
    docker-compose up -d postgres
    sleep 10
fi

print_status "✅ PostgreSQL container is running"

# Step 2: Wait for PostgreSQL to be ready
print_status "Waiting for PostgreSQL to be ready..."
counter=0
while [ $counter -lt 30 ]; do
    if docker exec postgres pg_isready -U postgres >/dev/null 2>&1; then
        print_status "✅ PostgreSQL is ready"
        break
    fi
    echo -n "."
    sleep 2
    counter=$((counter + 2))
done

if [ $counter -ge 30 ]; then
    print_error "PostgreSQL failed to start properly"
    exit 1
fi

# Step 3: Create spotify_user manually
print_status "Creating spotify_user role and database..."

# Create the user first
print_status "Creating spotify_user..."
docker exec postgres psql -U postgres -c "
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'spotify_user') THEN
        CREATE USER spotify_user WITH PASSWORD 'spotify_pass';
        RAISE NOTICE 'User spotify_user created successfully';
    ELSE
        RAISE NOTICE 'User spotify_user already exists';
    END IF;
END
\$\$;"

# Create database if not exists
print_status "Creating database spotify_analytics..."
docker exec postgres psql -U postgres -c "
SELECT 'Database exists' WHERE EXISTS (SELECT FROM pg_database WHERE datname = 'spotify_analytics')
UNION ALL
SELECT 'Creating database' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'spotify_analytics');"

# Try to create database (will fail if exists, that's ok)
docker exec postgres psql -U postgres -c "CREATE DATABASE spotify_analytics;" 2>/dev/null || print_warning "Database spotify_analytics already exists"

# Grant permissions
print_status "Granting permissions..."
docker exec postgres psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE spotify_analytics TO spotify_user;"

if [ $? -eq 0 ]; then
    print_status "✅ spotify_user created successfully"
else
    print_error "Failed to create spotify_user"
    exit 1
fi

# Step 4: Set up database schema and permissions
print_status "Setting up database schema and permissions..."

docker exec postgres psql -U postgres -d spotify_analytics -c "
-- Grant schema permissions
GRANT ALL PRIVILEGES ON SCHEMA public TO spotify_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO spotify_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO spotify_user;

-- Set default privileges for future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO spotify_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO spotify_user;

-- Create tables if they don't exist
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
    period_stats JSONB,
    analysis_metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(genre, analysis_date, analysis_period_years)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_song_analytics_genre ON song_analytics(genre);
CREATE INDEX IF NOT EXISTS idx_song_analytics_year ON song_analytics(year);
CREATE INDEX IF NOT EXISTS idx_song_analytics_processed_at ON song_analytics(processed_at);
CREATE INDEX IF NOT EXISTS idx_genre_trends_genre ON genre_trends(genre);
CREATE INDEX IF NOT EXISTS idx_genre_trends_window ON genre_trends(analysis_window);
CREATE INDEX IF NOT EXISTS idx_real_time_events_type ON real_time_events(event_type);
CREATE INDEX IF NOT EXISTS idx_real_time_events_created ON real_time_events(created_at);
CREATE INDEX IF NOT EXISTS idx_timeless_genre ON timeless_genre_metrics(genre);
CREATE INDEX IF NOT EXISTS idx_timeless_date ON timeless_genre_metrics(analysis_date);
"

if [ $? -eq 0 ]; then
    print_status "✅ Database schema created successfully"
else
    print_error "Failed to create database schema"
    exit 1
fi

# Step 5: Test connection
print_status "Testing spotify_user connection..."

docker exec postgres psql -U spotify_user -d spotify_analytics -c "SELECT 'Connection successful!' as status;"

if [ $? -eq 0 ]; then
    print_status "✅ spotify_user connection test successful!"
else
    print_error "❌ spotify_user connection test failed"
    exit 1
fi

# Step 6: Show database info
print_status "Database setup complete! Info:"

docker exec postgres psql -U spotify_user -d spotify_analytics -c "
SELECT 
    schemaname,
    tablename,
    tableowner
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY tablename;
"

echo ""
print_status "🎉 PostgreSQL user fix completed successfully!"
print_status "Database: spotify_analytics"
print_status "User: spotify_user"
print_status "Password: spotify_pass"
print_status "All tables created with proper permissions"
print_status ""
print_status "You can now run: python3 run_spotify_system.py"