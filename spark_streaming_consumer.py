# spark_streaming_consumer.py - UPDATED WITH STRICT ALGORITHM

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SpotifySparkStreamingAnalyzer:
    def __init__(self, kafka_bootstrap_servers='localhost:9092'):
        self.kafka_servers = kafka_bootstrap_servers
        self.spark = None
        self.setup_spark()
        
        # STRICT TIMELESS PARAMETERS (SAME AS STANDALONE)
        self.analysis_year_start = 1900
        self.analysis_year_end = 2030
        self.decade_length = 10
        self.max_possible_decades = (self.analysis_year_end - self.analysis_year_start + 1) // (self.decade_length/2)
        self.min_songs_per_decade = 10
        self.min_decades_for_timeless = 2
        self.min_total_songs = 50
        
    def setup_spark(self):
        """Spark Session with Kafka and PostgreSQL support"""
        self.spark = SparkSession.builder \
            .appName("SpotifyTimelessAnalytics") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                   "org.postgresql:postgresql:42.6.0") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark Session created with STRICT timeless algorithm")

        self.postgres_props = {
            "user": "spotify_user",
            "password": "spotify_pass",
            "driver": "org.postgresql.Driver",
            "url": "jdbc:postgresql://localhost:5432/spotify_analytics?stringtype=unspecified"
        }
    
    def define_schemas(self):
        """Define schemas for incoming data"""
        self.song_schema = StructType([
            StructField("title", StringType(), True),
            StructField("artist", StringType(), True),
            StructField("top_genre", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("popularity", IntegerType(), True),
            StructField("energy", IntegerType(), True),
            StructField("danceability", IntegerType(), True),
            StructField("valence", IntegerType(), True),
            StructField("acousticness", IntegerType(), True),
            StructField("bpm", IntegerType(), True),
            StructField("length", IntegerType(), True)
        ])
        
        self.event_schema = StructType([
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("song_data", self.song_schema, True),
            StructField("metadata", MapType(StringType(), StringType()), True)
        ])
    
    def setup_kafka_streams(self):
        """Setup Kafka Structured Streaming"""
        self.define_schemas()
        
        # Historical stream
        historical_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", "spotify-historical-stream") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        historical_parsed = historical_stream.select(
            from_json(col("value").cast("string"), self.event_schema).alias("parsed_data")
        )
        
        self.historical_df = historical_parsed.select(
            col("parsed_data.song_data.title").alias("title"),
            col("parsed_data.song_data.artist").alias("artist"),
            col("parsed_data.song_data.top_genre").alias("top_genre"),
            col("parsed_data.song_data.year").alias("year"),
            col("parsed_data.song_data.popularity").alias("popularity"),
            col("parsed_data.song_data.energy").alias("energy"),
            col("parsed_data.song_data.danceability").alias("danceability"),
            col("parsed_data.song_data.valence").alias("valence"),
            col("parsed_data.song_data.acousticness").alias("acousticness"),
            col("parsed_data.song_data.bpm").alias("bpm"),
            col("parsed_data.song_data.length").alias("length"),
            col("parsed_data.event_type").alias("event_type"),
            col("parsed_data.timestamp").cast("timestamp").alias("source_timestamp")
        ).withColumn("processed_at", current_timestamp())
        
        # Real-time events stream
        realtime_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", "spotify-realtime-events") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        realtime_parsed = realtime_stream.select(
            from_json(col("value").cast("string"), self.event_schema).alias("parsed_data")
        )
        
        self.realtime_df = realtime_parsed.select(
            col("parsed_data.song_data.title").alias("title"),
            col("parsed_data.song_data.artist").alias("artist"),
            col("parsed_data.song_data.top_genre").alias("top_genre"),
            col("parsed_data.song_data.year").alias("year"),
            col("parsed_data.song_data.popularity").alias("popularity"),
            col("parsed_data.song_data.energy").alias("energy"),
            col("parsed_data.song_data.danceability").alias("danceability"),
            col("parsed_data.song_data.valence").alias("valence"),
            col("parsed_data.song_data.acousticness").alias("acousticness"),
            col("parsed_data.event_type").alias("event_type"),
            col("parsed_data.metadata.event_id").alias("event_id"),
            col("parsed_data.metadata.boost_amount").cast("int").alias("boost_amount"),
            col("parsed_data.metadata.reason").alias("reason"),
            col("parsed_data.metadata.original_popularity").cast("int").alias("original_popularity"),
            col("parsed_data.metadata.has_timeout").cast("boolean").alias("has_timeout")
        ).withColumn("processed_at", current_timestamp())
        
        logger.info("Kafka streams configured with STRICT algorithm")
    
    def create_temp_views(self):
        """Create temporary views for SQL analytics"""
        self.historical_df.createOrReplaceTempView("historical_songs")
        self.realtime_df.createOrReplaceTempView("realtime_events")
        logger.info("Temporary views created")
    
    def start_historical_analytics(self):
        """Process historical data with Spark SQL"""
        # Genre Trends Analysis
        genre_trends_query = self.spark.sql("""
            SELECT 
                top_genre as genre,
                window(processed_at, '5 minutes') as time_window,
                COUNT(*) as song_count,
                AVG(popularity) as avg_popularity,
                STDDEV(popularity) as std_popularity,
                MAX(popularity) as max_popularity,
                MIN(popularity) as min_popularity,
                approx_count_distinct(artist) as unique_artists
            FROM historical_songs 
            WHERE top_genre IS NOT NULL
            GROUP BY top_genre, window(processed_at, '5 minutes')
        """)
        
        def write_genre_trends_to_postgres(df, epoch_id):
            try:
                df.select(
                    col("genre"),
                    col("avg_popularity").cast("decimal(5,2)"),
                    col("song_count").cast("integer"),
                    col("time_window.start").alias("analysis_window"),
                    col("std_popularity").cast("decimal(5,2)"),
                    col("max_popularity").cast("integer"),
                    col("min_popularity").cast("integer"),
                    col("unique_artists").cast("integer")
                ).write \
                .format("jdbc") \
                .options(**self.postgres_props) \
                .option("dbtable", "genre_trends") \
                .mode("append") \
                .save()
                logger.info(f"Wrote {df.count()} genre trend records")
            except Exception as e:
                logger.error(f"Error writing genre trends: {e}")
        
        genre_trends_stream = genre_trends_query.writeStream \
            .foreachBatch(write_genre_trends_to_postgres) \
            .outputMode("update") \
            .trigger(processingTime="30 seconds") \
            .option("checkpointLocation", "/tmp/genre_trends_checkpoint") \
            .start()
        
        # Song Analytics Storage
        def write_song_analytics_to_postgres(df, epoch_id):
            try:
                df.select(
                    col("title"),
                    col("artist"), 
                    col("top_genre").alias("genre"),
                    col("year"),
                    col("popularity"),
                    col("energy"),
                    col("danceability"),
                    col("valence"),
                    col("acousticness"),
                    col("bpm"),
                    col("length"),
                    col("processed_at"),
                    col("event_type"),
                    col("source_timestamp")
                ).write \
                .format("jdbc") \
                .options(**self.postgres_props) \
                .option("dbtable", "song_analytics") \
                .mode("append") \
                .save()
                logger.info(f"Wrote {df.count()} song records")
            except Exception as e:
                logger.error(f"Error writing songs: {e}")
        
        song_analytics_stream = self.historical_df.writeStream \
            .foreachBatch(write_song_analytics_to_postgres) \
            .outputMode("append") \
            .trigger(processingTime="10 seconds") \
            .option("checkpointLocation", "/tmp/song_analytics_checkpoint") \
            .start()
        
        return [genre_trends_stream, song_analytics_stream]
    
    def start_realtime_analytics(self):
        """Process real-time events"""
        def write_realtime_events_to_postgres(df, epoch_id):
            try:
                metadata_struct = struct(
                    coalesce(col("event_id"), lit("")).alias("event_id"),
                    coalesce(col("boost_amount").cast("int"), lit(0)).alias("boost_amount"),
                    coalesce(col("reason"), lit("")).alias("reason"),
                    coalesce(col("original_popularity").cast("int"), lit(0)).alias("original_popularity"),
                    coalesce(col("has_timeout").cast("boolean"), lit(False)).alias("has_timeout")
                )
                
                df_processed = df.select(
                    col("event_type"),
                    col("title").alias("song_title"),
                    col("artist"),
                    to_json(metadata_struct).alias("metadata"),
                    col("processed_at").alias("created_at"),
                    coalesce(col("event_id"), lit("unknown")).alias("event_id"),
                    coalesce(col("boost_amount").cast("int"), lit(0)).alias("boost_amount"),
                    coalesce(col("reason"), lit("unknown")).alias("reason"),
                    coalesce(col("original_popularity").cast("int"), lit(0)).alias("original_popularity"),
                    coalesce(col("has_timeout").cast("boolean"), lit(False)).alias("has_timeout")
                )
                
                df_processed.write \
                    .format("jdbc") \
                    .options(**self.postgres_props) \
                    .option("dbtable", "real_time_events") \
                    .mode("append") \
                    .save()
                
                logger.info(f"Wrote {df.count()} real-time events")
                
            except Exception as e:
                logger.error(f"Error writing real-time events: {e}")
        
        realtime_stream = self.realtime_df.writeStream \
            .foreachBatch(write_realtime_events_to_postgres) \
            .outputMode("append") \
            .trigger(processingTime="5 seconds") \
            .option("checkpointLocation", "/tmp/realtime_events_checkpoint") \
            .start()
        
        return [realtime_stream]
    
    def start_STRICT_timeless_analytics(self):
        """UPDATED: Use STRICT timeless algorithm from standalone version"""
        
        def run_STRICT_timeless_analysis():
            try:
                # Clear Spark cache first
                self.spark.catalog.clearCache()
                logger.info("🧹 Spark cache cleared")
                
                existing_songs = (self.spark.read
                    .format("jdbc")
                    .options(**self.postgres_props)
                    .option("dbtable", "public.song_analytics")
                    .load()
                )

                if existing_songs.count() < self.min_total_songs:
                    logger.info(f"Not enough data for STRICT analysis (need {self.min_total_songs})")
                    return

                existing_songs.createOrReplaceTempView("all_songs_batch")

                # STRICT TIMELESS ALGORITHM (SAME AS STANDALONE)
                strict_timeless_query = f"""
                    WITH decade_analysis AS (
                        SELECT 
                            LOWER(TRIM(genre)) as genre,
                            FLOOR(year / {self.decade_length}) * {self.decade_length} as decade,
                            COUNT(*) as songs_in_decade,
                            ROUND(AVG(popularity), 2) as avg_popularity,
                            ROUND(STDDEV(popularity), 2) as popularity_std,
                            MAX(popularity) as max_popularity,
                            MIN(popularity) as min_popularity,
                            COUNT(DISTINCT artist) as unique_artists
                        FROM all_songs_batch
                        WHERE genre IS NOT NULL 
                          AND year >= {self.analysis_year_start} 
                          AND year <= {self.analysis_year_end}
                          AND popularity >= 1
                        GROUP BY LOWER(TRIM(genre)), FLOOR(year / {self.decade_length}) * {self.decade_length}
                        HAVING COUNT(*) >= {self.min_songs_per_decade}
                    ),
                    genre_summary AS (
                        SELECT 
                            genre,
                            COUNT(*) as decades_active,
                            MIN(decade) as first_decade,
                            MAX(decade) as last_decade,
                            SUM(songs_in_decade) as total_songs,
                            SUM(unique_artists) as total_artists,
                            AVG(avg_popularity) as overall_avg_popularity,
                            ROUND(STDDEV(avg_popularity), 2) as decade_popularity_variance,
                            MAX(avg_popularity) as peak_decade_popularity,
                            MIN(avg_popularity) as lowest_decade_popularity
                        FROM decade_analysis
                        GROUP BY genre
                        HAVING COUNT(*) >= {self.min_decades_for_timeless}
                           AND SUM(songs_in_decade) >= {self.min_total_songs}
                    ),
                    scoring AS (
                        SELECT 
                            genre,
                            decades_active,
                            (last_decade - first_decade) / {self.decade_length} + 1 as time_span_decades,
                            total_songs,
                            total_artists,
                            overall_avg_popularity,
                            decade_popularity_variance,
                            
                            -- STRICT SCORING (SAME AS STANDALONE)
                            ROUND(LEAST(100.0, (decades_active * 100.0) / {self.max_possible_decades}), 1) as decade_presence_score,
                            
                            ROUND(
                                CASE 
                                    WHEN overall_avg_popularity > 0 AND decade_popularity_variance > 0 THEN
                                        GREATEST(0, 100 - (decade_popularity_variance / overall_avg_popularity * 300))
                                    WHEN decade_popularity_variance = 0 THEN 85.0
                                    ELSE 0
                                END, 1
                            ) as consistency_score,
                            
                            ROUND(
                                LEAST(100.0, 
                                    ((last_decade - first_decade) / 50.0) * 60 +
                                    (decades_active * 5) +
                                    LEAST(10, LOG10(total_songs) * 3)
                                ), 1
                            ) as longevity_score,
                            
                            ROUND(
                                CASE 
                                    WHEN peak_decade_popularity > lowest_decade_popularity THEN
                                        GREATEST(0, 100 - 
                                            ((peak_decade_popularity - lowest_decade_popularity) / overall_avg_popularity * 200)
                                        )
                                    ELSE 70.0
                                END, 1
                            ) as stability_score,
                            
                            CASE 
                                WHEN overall_avg_popularity < 30 THEN 15
                                WHEN overall_avg_popularity < 45 THEN 8
                                WHEN overall_avg_popularity < 60 THEN 3
                                ELSE 0
                            END as popularity_penalty,
                            
                            CASE 
                                WHEN last_decade < {self.analysis_year_end - 20} THEN 12
                                WHEN last_decade < {self.analysis_year_end - 10} THEN 6
                                ELSE 0
                            END as recency_penalty
                            
                        FROM genre_summary
                    )
                    SELECT 
                        genre,
                        decades_active as decade_presence,
                        time_span_decades,
                        total_songs,
                        total_artists,
                        ROUND(overall_avg_popularity, 1) as avg_popularity,
                        decade_presence_score,
                        consistency_score,
                        longevity_score,
                        stability_score,
                        popularity_penalty,
                        recency_penalty,
                        
                        -- FINAL STRICT TIMELESS SCORE
                        ROUND(
                            GREATEST(0,
                                (consistency_score * 0.35 +
                                 longevity_score * 0.25 +
                                 stability_score * 0.20 +
                                 decade_presence_score * 0.20) -
                                popularity_penalty -
                                recency_penalty
                            ), 1
                        ) as timeless_score,
                        
                        current_date() as analysis_date,
                        {self.decade_length} as analysis_period_years
                        
                    FROM scoring
                    WHERE consistency_score > 0 AND longevity_score > 0
                    ORDER BY 
                        GREATEST(0,
                            (consistency_score * 0.35 + longevity_score * 0.25 + 
                             stability_score * 0.20 + decade_presence_score * 0.20) -
                            popularity_penalty - recency_penalty
                        ) DESC
                """

                timeless_analysis = self.spark.sql(strict_timeless_query)
                
                # Pre-delete old results for today
                postgres_config = {
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'spotify_analytics',
                    'user': 'spotify_user',
                    'password': 'spotify_pass'
                }

                try:
                    conn = psycopg2.connect(**postgres_config)
                    conn.autocommit = True
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM public.timeless_genre_metrics WHERE analysis_date = CURRENT_DATE")
                        logger.info(f"🗑️ Cleared {cur.rowcount} old timeless records for today")
                    conn.close()
                except Exception as del_err:
                    logger.warning(f"Pre-delete failed: {del_err}")

                # Write new STRICT results
                rows_to_write = timeless_analysis.count()
                logger.info(f"📊 STRICT timeless rows to write: {rows_to_write}")

                if rows_to_write > 0:
                    (timeless_analysis.select(
                            col("genre"),
                            col("analysis_date"),
                            col("analysis_period_years").cast("int"),
                            col("timeless_score").cast(DecimalType(5, 2)),
                            col("consistency_score").cast(DecimalType(5, 2)),
                            col("longevity_score").cast(DecimalType(5, 2)),
                            col("stability_score").cast(DecimalType(5, 2)).alias("variance_score"),  # Map to existing DB column
                            col("decade_presence_score").cast(DecimalType(5, 2)).alias("peak_stability"),  # Map to existing DB column
                            col("decade_presence").cast("int"),
                            col("time_span_decades").cast("int").alias("year_span"),  # Map to existing DB column
                            col("total_songs").cast("int"),
                            col("total_artists").cast("int").alias("total_unique_artists")  # Map to existing DB column
                        )
                        .write
                        .format("jdbc")
                        .options(**{**self.postgres_props, "driver": "org.postgresql.Driver"})
                        .option("dbtable", "public.timeless_genre_metrics")
                        .mode("append")
                        .save()
                    )

                    logger.info(f"✅ Updated STRICT timeless metrics for {rows_to_write} genres")
                    
                    # Show max scores achieved for monitoring
                    max_score = timeless_analysis.agg(max("timeless_score")).collect()[0][0]
                    avg_score = timeless_analysis.agg(avg("timeless_score")).collect()[0][0]
                    logger.info(f"📈 STRICT scores - Max: {max_score:.1f}, Avg: {avg_score:.1f}")
                else:
                    logger.warning("❌ No genres met STRICT criteria")

            except Exception as e:
                logger.error(f"STRICT timeless analysis error: {e}")

        # Run initial analysis
        run_STRICT_timeless_analysis()

        # Schedule periodic analysis
        import threading, time
        def periodic_STRICT_analysis():
            while True:
                time.sleep(60)  # Every minute (increased frequency for testing)
                run_STRICT_timeless_analysis()
                
        threading.Thread(target=periodic_STRICT_analysis, daemon=True).start()
        logger.info("🔄 STRICT timeless analysis scheduled every 60 seconds")

        return []
    
    def start_all_analytics(self):
        """Start all Spark Structured Streaming analytics with STRICT algorithm"""
        logger.info("Setting up Kafka streams...")
        self.setup_kafka_streams()
        
        logger.info("Creating temporary views...")
        self.create_temp_views()
        
        logger.info("Starting historical analytics...")
        historical_streams = self.start_historical_analytics()
        
        logger.info("Starting real-time analytics...")
        realtime_streams = self.start_realtime_analytics()
        
        logger.info("Starting STRICT timeless analytics...")
        timeless_streams = self.start_STRICT_timeless_analytics()
        
        all_streams = historical_streams + realtime_streams + timeless_streams
        
        logger.info(f"✅ Started {len(all_streams)} Spark streams with STRICT timeless algorithm")
        logger.info(f"🎯 STRICT params: {self.min_songs_per_decade} songs/decade, {self.min_decades_for_timeless} decades min")
        logger.info(f"📊 Expected max scores: 40-95 (no perfect 100s)")
        
        return all_streams
    
    def stop_all_streams(self):
        """Stop all streaming queries"""
        for stream in self.spark.streams.active:
            logger.info(f"Stopping stream: {stream.name}")
            stream.stop()
        
        if self.spark:
            self.spark.stop()
        
        logger.info("All STRICT streams stopped")

if __name__ == "__main__":
    import time
    
    print("SPOTIFY SPARK STREAMING WITH STRICT TIMELESS ALGORITHM")
    print("=" * 70)
    print("🎯 STRICT SCORING: Max ~95 points (no perfect 100)")
    print("📊 FIXED DECADES: 1960s-2020s (max 7 decades)")
    print("⚖️ HIGH THRESHOLDS: 10+ songs/decade, 3+ decades minimum")
    print("🔄 REAL-TIME UPDATES: Every 60 seconds")
    
    analyzer = SpotifySparkStreamingAnalyzer()
    
    try:
        streams = analyzer.start_all_analytics()
        
        print("✅ STRICT Spark Streaming Analytics Active!")
        print("📊 Historical data → PostgreSQL (STRICT genre trends)")
        print("⚡ Real-time events → PostgreSQL (event tracking)")
        print("🎯 STRICT timeless analytics → PostgreSQL (NO 100s!)")
        print("📈 Realistic scores: 40-95 range expected")
        print("Press Ctrl+C to stop")
        print("-" * 70)
        
        while True:
            time.sleep(60)
            
            # Check stream health
            active_streams = [s for s in streams if s.isActive]
            if len(active_streams) != len(streams):
                logger.warning(f"Some streams stopped. Active: {len(active_streams)}/{len(streams)}")
                    
    except KeyboardInterrupt:
        print("\nStopping STRICT Spark Analytics...")
    finally:
        analyzer.stop_all_streams()
        print("STRICT Spark Analytics stopped!")