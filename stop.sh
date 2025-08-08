# stop.sh - Sistemi durdurmak için
#!/bin/bash

echo "🛑 Spotify Analytics Sistemi durduruluyor..."

# Python uygulamalarını durdur
echo "🐍 Python uygulamaları sonlandırılıyor..."
pkill -f "kafka_producer.py"
pkill -f "spark_streaming_consumer.py" 
pkill -f "interactive_dashboard.py"

# Docker containers'ı durdur
echo "📦 Docker containers durduruluyor..."
docker-compose down

echo "🧹 Temizlik yapılıyor..."
docker system prune -f

echo "✅ Sistem başarıyla durduruldu!"