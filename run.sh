#!/bin/bash

echo "🎵 Spotify Real-time Analytics Sistemi Başlatılıyor..."

# Docker containers'ı başlat
echo "📦 Docker containers başlatılıyor..."
docker-compose up -d

# Kafka'nın hazır olmasını bekle
echo "⏳ Kafka'nın hazır olması bekleniyor..."
sleep 30

# Kafka topic'lerini oluştur
echo "📂 Kafka topic'leri oluşturuluyor..."
docker exec kafka kafka-topics --create --topic spotify-historical-stream --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3
docker exec kafka kafka-topics --create --topic spotify-realtime-events --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3
docker exec kafka kafka-topics --create --topic spotify-analytics-results --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3

# Topic'leri listele
echo "📋 Oluşturulan topic'ler:"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

echo "✅ Sistem hazır!"
echo ""
echo "🔗 Erişim URL'leri:"
echo "   📊 Kafka UI: http://localhost:8080"
echo "   ⚡ Spark Master: http://localhost:8081"
echo "   📓 Jupyter Notebook: http://localhost:8888"
echo "   🎛️ Dashboard: http://localhost:8050 (manuel olarak başlatın)"
echo ""
echo "📝 Sonraki adımlar:"
echo "   1. Python gereksinimlerini yükleyin: pip install -r requirements.txt"
echo "   2. Producer'ı başlatın: python kafka_producer.py"
echo "   3. Spark Streaming'i başlatın: python spark_streaming_consumer.py"
echo "   4. Dashboard'u başlatın: python interactive_dashboard.py"