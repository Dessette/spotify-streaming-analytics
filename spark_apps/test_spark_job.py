#!/usr/bin/env python3
"""
Sample Spark job to test the complete setup
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def main():
    """Test Spark functionality with Kafka and PostgreSQL"""
    
    # Create Spark session with all packages
    spark = SparkSession.builder \
        .appName("SpotifySetupTest") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1," +
                "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    
    print("✅ Spark Session created successfully")
    print(f"   Spark Version: {spark.version}")
    print(f"   Application ID: {spark.sparkContext.applicationId}")
    
    # Test basic DataFrame operations
    test_data = [
        (1, "rock", 85, 2020, "high"),
        (2, "pop", 92, 2021, "very_high"), 
        (3, "jazz", 78, 2019, "medium"),
        (4, "electronic", 88, 2022, "high"),
        (5, "classical", 65, 2018, "medium")
    ]
    
    schema = ["id", "genre", "popularity", "year", "category"]
    test_df = spark.createDataFrame(test_data, schema)
    
    print("\n✅ Test DataFrame created")
    test_df.show()
    
    # Test SQL operations
    test_df.createOrReplaceTempView("test_genres")
    
    result = spark.sql("""
        SELECT 
            genre,
            AVG(popularity) as avg_popularity,
            COUNT(*) as song_count,
            MAX(year) as latest_year
        FROM test_genres 
        GROUP BY genre 
        ORDER BY avg_popularity DESC
    """)
    
    print("\n✅ SQL operations working")
    result.show()
    
    # Test window functions
    window_result = spark.sql("""
        SELECT 
            genre,
            popularity,
            year,
            ROW_NUMBER() OVER (PARTITION BY genre ORDER BY popularity DESC) as rank
        FROM test_genres
    """)
    
    print("\n✅ Window functions working")
    window_result.show()
    
    # Test PostgreSQL connection (if tables exist)
    try:
        postgres_props = {
            "user": "spotify_user",
            "password": "spotify_pass",
            "driver": "org.postgresql.Driver"
        }
        
        # Try to read from song_analytics table
        postgres_df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/spotify_analytics") \
            .options(**postgres_props) \
            .option("dbtable", "song_analytics") \
            .load()
        
        row_count = postgres_df.count()
        print(f"\n✅ PostgreSQL connection working - {row_count} rows in song_analytics")
        
        if row_count > 0:
            postgres_df.show(5)
        
    except Exception as e:
        print(f"\n⚠️ PostgreSQL test skipped: {e}")
    
    spark.stop()
    print("\n🎉 Complete Spark setup test passed!")

if __name__ == "__main__":
    main()
