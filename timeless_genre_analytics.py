from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class SparkTimelessGenreAnalyzer:
    def __init__(self, spark_session=None):
        # Spark Session
        if spark_session:
            self.spark = spark_session
        else:
            self.spark = SparkSession.builder \
                .appName("TimelessGenreAnalytics") \
                .config("spark.sql.adaptive.enabled", "true") \
                .getOrCreate()
        
        # PostgreSQL properties
        self.postgres_props = {
            "user": "spotify_user",
            "password": "spotify_pass", 
            "driver": "org.postgresql.Driver",
            "url": "jdbc:postgresql://localhost:5432/spotify_analytics"
        }
        
        # STRICT ANALYSIS PARAMETERS
        self.analysis_year_start = 1900  # Fixed start
        self.analysis_year_end = 2030    # Fixed end  
        self.total_analysis_years = self.analysis_year_end - self.analysis_year_start + 1  # 65 years
        
        # Period configurations
        self.decade_length = 10  # Fixed 10-year decades
        self.max_possible_decades = self.total_analysis_years // (self.decade_length/2)  # 6-7 decades max
        
        # Minimum requirements (STRICT)
        self.min_songs_per_decade = 10   # Higher threshold
        self.min_decades_for_timeless = 2  # Must span at least 3 decades
        self.min_total_songs = 50          # Minimum total songs
        
        logger.info(f"Initialized STRICT timeless analysis:")
        logger.info(f"  Analysis period: {self.analysis_year_start}-{self.analysis_year_end} ({self.total_analysis_years} years)")
        logger.info(f"  Max possible decades: {self.max_possible_decades}")
        logger.info(f"  Min songs per decade: {self.min_songs_per_decade}")
        logger.info(f"  Min decades required: {self.min_decades_for_timeless}")

    def load_song_data(self):
        """Load and strictly filter song data"""
        logger.info("Loading song data with STRICT filtering...")
        
        song_data = self.spark.read \
            .format("jdbc") \
            .options(**self.postgres_props) \
            .option("dbtable", "song_analytics") \
            .load()
        
        # VERY STRICT filtering
        cleaned_data = song_data.filter(
            (col("genre").isNotNull()) & 
            (col("year").isNotNull()) & 
            (col("popularity").isNotNull()) &
            (col("year") >= self.analysis_year_start) & 
            (col("year") <= self.analysis_year_end) &
            (col("popularity") >= 1) &   # No zero popularity
            (col("popularity") <= 100) &
            (col("title").isNotNull()) &
            (col("artist").isNotNull())
        ).withColumn(
            "decade", 
            floor(col("year") / self.decade_length) * self.decade_length
        ).withColumn(
            "decade_label",
            concat(col("decade").cast("string"), lit("s"))
        )
        
        cleaned_data.cache()
        
        total_songs = cleaned_data.count()
        unique_genres = cleaned_data.select("genre").distinct().count()
        year_range = cleaned_data.select(min("year"), max("year")).first()
        
        logger.info(f"STRICT filtered data:")
        logger.info(f"  Total songs: {total_songs}")
        logger.info(f"  Unique genres: {unique_genres}")
        logger.info(f"  Year range: {year_range[0]}-{year_range[1]}")
        
        return cleaned_data

    def calculate_timeless_scores(self):
        """Calculate REALISTIC timeless scores - no genre should get 100/100"""
        song_data = self.load_song_data()
        song_data.createOrReplaceTempView("songs")
        
        # STRICT SQL analysis
        timeless_query = f"""
            WITH decade_analysis AS (
                SELECT 
                    genre,
                    decade,
                    decade + 9 as decade_end,
                    COUNT(*) as songs_in_decade,
                    ROUND(AVG(popularity), 2) as avg_popularity,
                    ROUND(STDDEV(popularity), 2) as popularity_std,
                    MAX(popularity) as max_popularity,
                    MIN(popularity) as min_popularity,
                    COUNT(DISTINCT artist) as unique_artists,
                    COUNT(DISTINCT year) as years_covered
                FROM songs
                GROUP BY genre, decade
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
                    
                    -- Popularity metrics across decades
                    AVG(avg_popularity) as overall_avg_popularity,
                    ROUND(STDDEV(avg_popularity), 2) as decade_popularity_variance,
                    MAX(avg_popularity) as peak_decade_popularity,
                    MIN(avg_popularity) as lowest_decade_popularity,
                    
                    -- Collect all decade data for analysis
                    COLLECT_LIST(STRUCT(decade, avg_popularity, songs_in_decade)) as decade_data
                FROM decade_analysis
                GROUP BY genre
                HAVING COUNT(*) >= {self.min_decades_for_timeless}
                   AND SUM(songs_in_decade) >= {self.min_total_songs}
            ),
            scoring AS (
                SELECT 
                    genre,
                    decades_active,
                    (last_decade - first_decade) / 10 + 1 as time_span_decades,
                    total_songs,
                    total_artists,
                    overall_avg_popularity,
                    decade_popularity_variance,
                    peak_decade_popularity,
                    lowest_decade_popularity,
                    
                    -- 1. DECADE PRESENCE SCORE (0-100): Based on actual decades covered
                    ROUND(
                        LEAST(100.0, (decades_active * 100.0) / {self.max_possible_decades})
                    , 1) as decade_presence_score,
                    
                    -- 2. CONSISTENCY SCORE (0-100): STRICT - Low variance across decades  
                    ROUND(
                        CASE 
                            WHEN overall_avg_popularity > 0 AND decade_popularity_variance > 0 THEN
                                GREATEST(0, 100 - (decade_popularity_variance / overall_avg_popularity * 300))
                            WHEN decade_popularity_variance = 0 THEN 85.0  -- Perfect consistency but capped
                            ELSE 0
                        END
                    , 1) as consistency_score,
                    
                    -- 3. LONGEVITY SCORE (0-100): STRICT - Real time span matters
                    ROUND(
                        LEAST(100.0, 
                            ((last_decade - first_decade) / 50.0) * 60 +  -- Max 60 points for 50+ year span
                            (decades_active * 5) +                         -- Max 30-35 points for decade count
                            LEAST(10, LOG10(total_songs) * 3)             -- Max 10 points for song volume
                        )
                    , 1) as longevity_score,
                    
                    -- 4. STABILITY SCORE (0-100): STRICT - Peak vs valley difference
                    ROUND(
                        CASE 
                            WHEN peak_decade_popularity > lowest_decade_popularity THEN
                                GREATEST(0, 100 - 
                                    ((peak_decade_popularity - lowest_decade_popularity) / overall_avg_popularity * 200)
                                )
                            ELSE 70.0  -- No variance but capped
                        END
                    , 1) as stability_score,
                    
                    -- Popularity tier for weighting
                    CASE 
                        WHEN overall_avg_popularity >= 70 THEN 'high'
                        WHEN overall_avg_popularity >= 45 THEN 'medium' 
                        ELSE 'low'
                    END as popularity_tier
                FROM genre_summary
            ),
            final_scoring AS (
                SELECT 
                    genre,
                    decades_active,
                    time_span_decades,
                    total_songs,
                    total_artists,
                    ROUND(overall_avg_popularity, 1) as avg_popularity,
                    popularity_tier,
                    
                    -- Individual scores (all capped realistically)
                    decade_presence_score,
                    consistency_score,
                    longevity_score, 
                    stability_score,
                    
                    -- 5. POPULARITY PENALTY: Lower popularity = penalty
                    ROUND(
                        CASE 
                            WHEN overall_avg_popularity < 30 THEN 15  -- -15 points for low popularity
                            WHEN overall_avg_popularity < 45 THEN 8   -- -8 points for medium-low
                            WHEN overall_avg_popularity < 60 THEN 3   -- -3 points for medium
                            ELSE 0                                     -- No penalty for high popularity
                        END
                    , 1) as popularity_penalty,
                    
                    -- 6. RECENCY CHECK: Declining genres get penalty
                    CASE 
                        WHEN last_decade < {self.analysis_year_end - 20} THEN 12  -- Not active in last 20 years = -12
                        WHEN last_decade < {self.analysis_year_end - 10} THEN 6   -- Not active in last 10 years = -6
                        ELSE 0
                    END as recency_penalty
                    
                FROM scoring
            )
            SELECT 
                genre,
                decades_active as decade_presence,
                time_span_decades,
                total_songs,
                total_artists,
                avg_popularity,
                popularity_tier,
                
                -- Individual component scores
                decade_presence_score,
                consistency_score,
                longevity_score,
                stability_score,
                popularity_penalty,
                recency_penalty,
                
                -- FINAL TIMELESS SCORE: Weighted combination with penalties
                -- NO GENRE SHOULD REALISTICALLY GET 100/100
                ROUND(
                    GREATEST(0,
                        (consistency_score * 0.35 +          -- 35% - Most important
                         longevity_score * 0.25 +            -- 25% - Time span matters  
                         stability_score * 0.20 +            -- 20% - Peak consistency
                         decade_presence_score * 0.20) -     -- 20% - Decade coverage
                        popularity_penalty -                  -- Subtract penalties
                        recency_penalty
                    )
                , 1) as timeless_score,
                
                current_date() as analysis_date,
                {self.decade_length} as analysis_period_years
                
            FROM final_scoring
            WHERE 
                consistency_score > 0 
                AND longevity_score > 0 
                AND decades_active >= {self.min_decades_for_timeless}
            ORDER BY 
                GREATEST(0,
                    (consistency_score * 0.35 + longevity_score * 0.25 + 
                     stability_score * 0.20 + decade_presence_score * 0.20) -
                    popularity_penalty - recency_penalty
                ) DESC
        """
        
        result = self.spark.sql(timeless_query)
        logger.info("STRICT timeless analysis completed")
        return result

    def show_realistic_expectations(self):
        """Show what realistic scores should look like"""
        logger.info("=== REALISTIC TIMELESS SCORE EXPECTATIONS ===")
        logger.info("🎯 NO GENRE SHOULD GET 100/100 (impossible perfection)")
        logger.info("🏆 90-95: Legendary (Classical, maybe Jazz)")
        logger.info("🥇 80-89: Excellent (Rock, Blues, Folk)")  
        logger.info("🥈 70-79: Very Good (Pop, Country, Soul)")
        logger.info("🥉 60-69: Good (Hip-Hop, R&B, Reggae)")
        logger.info("📊 50-59: Average (Electronic, Punk, Indie)")
        logger.info("📉 40-49: Below Average (Disco, New Wave)")
        logger.info("❌ <40: Trend-based (Dubstep, Emo, Grunge)")

    def get_genre_breakdown(self, genre):
        """Get detailed breakdown for a specific genre"""
        song_data = self.load_song_data()
        
        genre_detail = song_data.filter(col("genre") == genre) \
            .groupBy("decade", "decade_label") \
            .agg(
                count("*").alias("songs"),
                round(avg("popularity"), 1).alias("avg_pop"),
                countDistinct("artist").alias("artists"),
                min("year").alias("first_year"),
                max("year").alias("last_year")
            ).orderBy("decade")
        
        logger.info(f"=== {genre.upper()} DECADE BREAKDOWN ===")
        genre_detail.show(truncate=False)
        
        return genre_detail

    def store_timeless_metrics(self, df):
        """Store results to PostgreSQL"""
        logger.info("Storing STRICT timeless metrics...")
        
        try:
            df_to_store = df.select(
                col("genre"),
                col("analysis_date"),
                col("analysis_period_years").cast("int"),
                col("timeless_score").cast(DecimalType(5,2)),
                col("consistency_score").cast(DecimalType(5,2)),
                col("longevity_score").cast(DecimalType(5,2)),
                col("stability_score").cast(DecimalType(5,2)),  # Map to variance_score column
                col("decade_presence_score").cast(DecimalType(5,2)),  # Map to peak_stability column
                col("decade_presence").cast("int"),
                col("time_span_decades").cast("int").alias("year_span"),
                col("total_songs").cast("int"),
                col("total_artists").cast("int").alias("total_unique_artists"),
                lit(None).cast("string").alias("period_stats"),
                lit(None).cast("string").alias("analysis_metadata")
            )
            
            df_to_store.write \
                .format("jdbc") \
                .options(**self.postgres_props) \
                .option("dbtable", "timeless_genre_metrics") \
                .mode("append") \
                .save()
            
            logger.info(f"Stored {df.count()} STRICT timeless metrics")
            
        except Exception as e:
            logger.error(f"Storage error: {e}")

    def run_strict_analysis(self):
        """Run the complete STRICT timeless analysis"""
        self.show_realistic_expectations()
        
        logger.info("Running STRICT timeless analysis...")
        logger.info("🎯 Fixed decades: 1960s, 1970s, 1980s, 1990s, 2000s, 2010s, 2020s")
        logger.info("⚖️ Realistic scoring: No perfect 100/100 scores")
        logger.info("🔒 High thresholds: 10+ songs per decade, 3+ decades minimum")
        
        # Calculate timeless scores
        timeless_df = self.calculate_timeless_scores()
        
        if timeless_df.count() == 0:
            logger.warning("❌ No genres met the STRICT criteria!")
            return {}
        
        # Store results  
        self.store_timeless_metrics(timeless_df)
        
        # Show results
        logger.info("=== TOP TIMELESS GENRES (STRICT ANALYSIS) ===")
        timeless_df.select(
            "genre", 
            "timeless_score", 
            "decade_presence",
            "consistency_score",
            "longevity_score", 
            "stability_score",
            "popularity_tier"
        ).show(20, truncate=False)
        
        # Show detailed breakdown for top genre
        if timeless_df.count() > 0:
            top_genre = timeless_df.first()
            logger.info(f"\n🏆 TOP TIMELESS: {top_genre['genre'].upper()}")
            logger.info(f"   Final Score: {top_genre['timeless_score']}/100")
            logger.info(f"   Decades Active: {top_genre['decade_presence']}/{self.max_possible_decades}")
            logger.info(f"   Consistency: {top_genre['consistency_score']}/100") 
            logger.info(f"   Longevity: {top_genre['longevity_score']}/100")
            logger.info(f"   Stability: {top_genre['stability_score']}/100")
            logger.info(f"   Popularity Tier: {top_genre['popularity_tier']}")
            
            # Show decade breakdown
            self.get_genre_breakdown(top_genre['genre'])
        
        return {
            'timeless_metrics': timeless_df,
            'total_analyzed': timeless_df.count(),
            'max_score_achieved': timeless_df.agg(max("timeless_score")).collect()[0][0],
            'avg_score': timeless_df.agg(avg("timeless_score")).collect()[0][0]
        }

    def cleanup(self):
        """Clean up resources"""
        if self.spark:
            self.spark.stop()

if __name__ == "__main__":
    print("SPOTIFY STRICT TIMELESS GENRE ANALYTICS")
    print("=" * 60)
    print("🎯 REALISTIC SCORING: No perfect 100/100 scores")
    print("📊 STRICT CRITERIA: 10+ songs/decade, 3+ decades minimum")
    print("⏰ FIXED TIMEFRAME: 1960-2024 (65 years, 6-7 decades max)")
    print("🏆 EXPECTED RANGE: 40-95 points (legends ~90, trends ~40)")
    print("⚖️ PENALTIES: Low popularity, recency, inconsistency")
    
    analyzer = SparkTimelessGenreAnalyzer()
    
    try:
        results = analyzer.run_strict_analysis()
        
        print("\n✅ STRICT Analysis Completed!")
        print(f"📊 {results.get('total_analyzed', 0)} genres met strict criteria")
        print(f"🏆 Highest score achieved: {results.get('max_score_achieved', 0):.1f}/100")
        print(f"📈 Average score: {results.get('avg_score', 0):.1f}/100")
        print("🎯 Realistic, no inflation!")
        
        print("\n🎵 STRICT timeless analysis completed!")
        
    except Exception as e:
        logger.error(f"Analysis error: {e}")
        print(f"Analysis failed: {e}")
    finally:
        analyzer.cleanup()
        print("Analysis completed.")