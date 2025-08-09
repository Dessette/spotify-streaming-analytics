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
        logger.info("Spark Session created with Kafka and PostgreSQL support")

        # PostgreSQL connection properties
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
        """Setup Kafka Structured Streaming - FIXED VERSION"""
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
        
        # Parse JSON with explicit column extraction - FIXED
        historical_parsed = historical_stream.select(
            from_json(col("value").cast("string"), self.event_schema).alias("parsed_data")
        )
        
        # Extract fields explicitly without star expansion - FIXED
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
        
        # Real-time events stream - FIXED
        realtime_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", "spotify-realtime-events") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse real-time events with explicit extraction - FIXED
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
        
        logger.info("Kafka streams configured successfully")
    
    def create_temp_views(self):
        """Create temporary views for SQL analytics"""
        # Create streaming temp views
        self.historical_df.createOrReplaceTempView("historical_songs")
        self.realtime_df.createOrReplaceTempView("realtime_events")
        
        logger.info("Temporary views created")
    
    def start_historical_analytics(self):
        """Process historical data with Spark SQL - STREAMING COMPATIBLE"""
        
        # 1. Genre Trends Analysis - FIXED for streaming
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
        
        # Write to PostgreSQL
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
        
        # 2. Song Analytics Storage - SIMPLIFIED
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
        """Process real-time events with Spark SQL - JDBC COMPATIBLE VERSION"""
        
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
                
                # Write to PostgreSQL
                df_processed.write \
                    .format("jdbc") \
                    .options(**self.postgres_props) \
                    .option("dbtable", "real_time_events") \
                    .mode("append") \
                    .save()
                
                logger.info(f"Wrote {df.count()} real-time events")
                
            except Exception as e:
                logger.error(f"Error writing real-time events: {e}")
                # Fallback: Try simpler version without metadata
                try:
                    df.select(
                        col("event_type"),
                        col("title").alias("song_title"),
                        col("artist"),
                        lit('{}').alias("metadata"),  # Empty JSON
                        col("processed_at").alias("created_at"),
                        coalesce(col("event_id"), lit("unknown")).alias("event_id"),
                        coalesce(col("boost_amount"), lit(0)).alias("boost_amount"),
                        coalesce(col("reason"), lit("unknown")).alias("reason"),
                        coalesce(col("original_popularity").cast("int"), lit(0)).alias("original_popularity"),
                        coalesce(col("has_timeout").cast("boolean"), lit(False)).alias("has_timeout")
                    ).write \
                    .format("jdbc") \
                    .options(**self.postgres_props) \
                    .option("dbtable", "real_time_events") \
                    .mode("append") \
                    .save()
                    
                    logger.info(f"Wrote {df.count()} real-time events (fallback mode)")
                except Exception as e2:
                    logger.error(f"Fallback write also failed: {e2}")
        
        realtime_stream = self.realtime_df.writeStream \
            .foreachBatch(write_realtime_events_to_postgres) \
            .outputMode("append") \
            .trigger(processingTime="5 seconds") \
            .option("checkpointLocation", "/tmp/realtime_events_checkpoint") \
            .start()
        
        return [realtime_stream]
    
    def start_timeless_analytics(self):

        def run_timeless_batch_analysis():
            try:
                # 1) Kaynak veriyi oku
                existing_songs = (self.spark.read
                    .format("jdbc")
                    .options(**self.postgres_props)
                    .option("dbtable", "public.song_analytics")
                    .load()
                )

                # ✅ Eşik değerini düşür (ör. 10)
                if existing_songs.count() < 10:
                    logger.info("Not enough data for timeless analysis")
                    return

                existing_songs.createOrReplaceTempView("all_songs_batch")

                # 2) Analiz (genre normalize + total_unique_artists üret)
                timeless_analysis = self.spark.sql("""
                    WITH base AS (
                      SELECT lower(trim(genre)) AS genre_norm, *
                      FROM all_songs_batch
                      WHERE genre IS NOT NULL AND year IS NOT NULL
                    ),
                    genre_stats AS (
                      SELECT 
                        genre_norm AS genre,
                        COUNT(*) AS total_songs,
                        AVG(popularity) AS avg_popularity,
                        STDDEV(popularity) AS std_popularity,
                        COUNT(DISTINCT year) AS year_span,
                        approx_count_distinct(artist) AS total_unique_artists,
                        current_date() AS analysis_date
                      FROM base
                      GROUP BY genre_norm
                      HAVING COUNT(*) >= 3
                    )
                    SELECT 
                      genre,
                      analysis_date,
                      5 AS analysis_period_years,
                      LEAST(100.0, avg_popularity + year_span * 2) AS timeless_score,
                      CASE 
                        WHEN avg_popularity > 0 THEN GREATEST(0.0, 100.0 - (std_popularity / avg_popularity * 100.0))
                        ELSE 0.0
                      END AS consistency_score,
                      LEAST(100.0, year_span * 5.0) AS longevity_score,
                      GREATEST(0.0, 100.0 - std_popularity) AS variance_score,
                      avg_popularity AS peak_stability,
                      LEAST(20, year_span) AS decade_presence,
                      year_span,
                      total_songs,
                      total_unique_artists
                    FROM genre_stats
                    ORDER BY timeless_score DESC
                """)

                postgres_config = {
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'spotify_analytics',
                    'user': 'spotify_user',
                    'password': 'spotify_pass'
                }

                # ✅ Pre-delete: hata verse bile yazmayı engellemesin
                try:
                    genres = [r["genre"] for r in timeless_analysis.select("genre").distinct().collect()]
                    if genres:
                        conn = psycopg2.connect(
                            host=postgres_config['host'],
                            port=postgres_config['port'],
                            database=postgres_config['database'],  # dbname alias'ı OK
                            user=postgres_config['user'],
                            password=postgres_config['password']
                        )
                        conn.autocommit = True
                        with conn.cursor() as cur:
                            cur.execute("""
                                DELETE FROM public.timeless_genre_metrics
                                WHERE analysis_date = CURRENT_DATE
                                  AND genre = ANY(%s)
                            """, (genres,))
                        conn.close()
                except Exception as del_err:
                    logger.warning(f"Pre-delete skipped: {del_err}")

                # 3) Skor kolonlarında NaN/NULL → 0.0
                for c in ["timeless_score","consistency_score","longevity_score","variance_score","peak_stability"]:
                    timeless_analysis = timeless_analysis.withColumn(
                        c, nanvl(coalesce(col(c), lit(0.0)), lit(0.0))
                    )

                # 4) Yazmadan önce kaç satır gideceğini logla
                rows_to_write = timeless_analysis.count()
                logger.info(f"Timeless rows to write: {rows_to_write}")

                # 5) Tip/isimleri tabloya hizala ve yaz
                (timeless_analysis.select(
                        col("genre"),
                        col("analysis_date"),
                        col("analysis_period_years").cast("int"),
                        col("timeless_score").cast(DecimalType(5, 2)),
                        col("consistency_score").cast(DecimalType(5, 2)),
                        col("longevity_score").cast(DecimalType(5, 2)),
                        col("variance_score").cast(DecimalType(5, 2)),
                        col("peak_stability").cast(DecimalType(5, 2)),
                        col("decade_presence").cast("int"),
                        col("year_span").cast("int"),
                        col("total_songs").cast("int"),
                        col("total_unique_artists").cast("int")
                    )
                    .write
                    .format("jdbc")
                    .options(**{**self.postgres_props, "driver": "org.postgresql.Driver"})  # güvence
                    .option("dbtable", "public.timeless_genre_metrics")
                    .mode("append")
                    .save()
                )

                logger.info(f"Updated timeless metrics for {rows_to_write} rows")

            except Exception as e:
                logger.error(f"Timeless analysis error: {e}")

        # İlk analizi hemen çalıştır
        run_timeless_batch_analysis()

        # Periyodik
        import threading, time
        def periodic_timeless_analysis():
            while True:
                time.sleep(25)
                run_timeless_batch_analysis()
        threading.Thread(target=periodic_timeless_analysis, daemon=True).start()

        return []

    
    def start_all_analytics(self):
        """Start all Spark Structured Streaming analytics"""
        logger.info("Setting up Kafka streams...")
        self.setup_kafka_streams()
        
        logger.info("Creating temporary views...")
        self.create_temp_views()
        
        logger.info("Starting historical analytics...")
        historical_streams = self.start_historical_analytics()
        
        logger.info("Starting real-time analytics...")
        realtime_streams = self.start_realtime_analytics()
        
        logger.info("Starting timeless analytics...")
        timeless_streams = self.start_timeless_analytics()
        
        all_streams = historical_streams + realtime_streams + timeless_streams
        
        logger.info(f"Started {len(all_streams)} Spark Structured Streaming queries")
        return all_streams
    
    def run_batch_analytics(self):
        """Run batch analytics queries"""
        logger.info("Running batch analytics...")
        
        # Read existing data from PostgreSQL for complex analytics
        existing_songs = self.spark.read \
            .format("jdbc") \
            .options(**self.postgres_props) \
            .option("dbtable", "song_analytics") \
            .load()
        
        existing_songs.createOrReplaceTempView("all_songs")
        
        # Advanced Analytics Queries
        
        # 1. Audio Features Analysis by Genre
        audio_features_analysis = self.spark.sql("""
            SELECT 
                genre,
                AVG(energy) as avg_energy,
                AVG(danceability) as avg_danceability, 
                AVG(valence) as avg_valence,
                AVG(acousticness) as avg_acousticness,
                STDDEV(energy) as energy_variance,
                COUNT(*) as song_count,
                current_timestamp() as analysis_date
            FROM all_songs
            WHERE genre IS NOT NULL
            GROUP BY genre
            HAVING COUNT(*) >= 10
            ORDER BY song_count DESC
        """)
        
        # 2. Year-over-Year Genre Popularity Trends
        yearly_trends = self.spark.sql("""
            SELECT 
                genre,
                year,
                AVG(popularity) as avg_popularity,
                COUNT(*) as song_count,
                COUNT(DISTINCT artist) as unique_artists,
                ROW_NUMBER() OVER (PARTITION BY year ORDER BY AVG(popularity) DESC) as yearly_rank
            FROM all_songs
            WHERE genre IS NOT NULL AND year IS NOT NULL
            GROUP BY genre, year
            HAVING COUNT(*) >= 3
            ORDER BY year DESC, avg_popularity DESC
        """)
        
        # 3. Artist Dominance Analysis
        artist_analysis = self.spark.sql("""
            SELECT 
                artist,
                genre,
                COUNT(*) as song_count,
                AVG(popularity) as avg_popularity,
                MAX(popularity) as peak_popularity,
                COUNT(DISTINCT year) as years_active,
                current_timestamp() as analysis_date
            FROM all_songs
            WHERE artist IS NOT NULL AND genre IS NOT NULL
            GROUP BY artist, genre
            HAVING COUNT(*) >= 5
            ORDER BY avg_popularity DESC, song_count DESC
        """)
        
        # Show results
        logger.info("=== AUDIO FEATURES BY GENRE ===")
        audio_features_analysis.show(10, truncate=False)
        
        logger.info("=== YEARLY GENRE TRENDS (Recent) ===")
        yearly_trends.filter(col("year") >= 2015).show(20, truncate=False)
        
        logger.info("=== TOP ARTISTS BY GENRE ===")
        artist_analysis.show(15, truncate=False)
        
        return {
            'audio_features': audio_features_analysis,
            'yearly_trends': yearly_trends,
            'artist_analysis': artist_analysis
        }
    
    def stop_all_streams(self):
        """Stop all streaming queries"""
        for stream in self.spark.streams.active:
            logger.info(f"Stopping stream: {stream.name}")
            stream.stop()
        
        if self.spark:
            self.spark.stop()
        
        logger.info("All streams stopped")

if __name__ == "__main__":
    import time
    
    print("SPOTIFY SPARK STRUCTURED STREAMING ANALYTICS")
    print("=" * 60)
    
    analyzer = SpotifySparkStreamingAnalyzer()
    
    try:
        # Start all streaming analytics
        streams = analyzer.start_all_analytics()
        
        print("✅ Spark Structured Streaming Analytics Active!")
        print("📊 Historical data → PostgreSQL (genre trends, song analytics)")
        print("⚡ Real-time events → PostgreSQL (event tracking)")
        print("🎯 Timeless analytics → PostgreSQL (genre metrics)")
        print("📈 Advanced SQL analytics every 30-60 seconds")
        print("🔄 Batch analytics available")
        print("Press Ctrl+C to stop")
        print("-" * 60)
        
        # Run batch analytics every 5 minutes
        last_batch_time = time.time()
        
        while True:
            time.sleep(30)
            
            # Check stream health
            active_streams = [s for s in streams if s.isActive]
            if len(active_streams) != len(streams):
                logger.warning(f"Some streams stopped. Active: {len(active_streams)}/{len(streams)}")
            
            # Run batch analytics every 5 minutes
            current_time = time.time()
            if current_time - last_batch_time > 300:  # 5 minutes
                try:
                    analyzer.run_batch_analytics()
                    last_batch_time = current_time
                except Exception as e:
                    logger.error(f"Batch analytics error: {e}")
                    
    except KeyboardInterrupt:
        print("\nStopping Spark Analytics...")
    finally:
        analyzer.stop_all_streams()
        print("Spark Analytics stopped!")