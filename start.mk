# Makefile - Kolay kullanım için
.PHONY: help start stop restart clean install dashboard producer consumer logs status

help:  ## Bu yardım mesajını göster
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Python gereksinimlerini yükle
	pip install -r requirements.txt
	pip install jupyter

start:  ## Tüm sistemi başlat
	chmod +x run.sh && ./run.sh

stop:  ## Tüm sistemi durdur
	chmod +x stop.sh && ./stop.sh

restart: stop start  ## Sistemi yeniden başlat

clean:  ## Docker volumes'ları da temizle
	docker-compose down -v
	docker system prune -af

dashboard:  ## Sadece dashboard'u başlat
	python interactive_dashboard.py

producer:  ## Sadece Kafka producer'ı başlat
	python kafka_producer.py

consumer:  ## Sadece Spark consumer'ı başlat
	python spark_streaming_consumer.py

logs:  ## Docker container loglarını göster
	docker-compose logs -f

status:  ## Sistem durumunu kontrol et
	@echo "🔍 Sistem Durumu:"
	@echo "Docker Containers:"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
	@echo ""
	@echo "Kafka Topics:"
	@docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null || echo "Kafka henüz hazır değil"

test:  ## Sistem testini çalıştır
	@echo "🧪 Sistem testi başlıyor..."
	@echo "1. Kafka bağlantısı test ediliyor..."
	@docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null && echo "✅ Kafka OK" || echo "❌ Kafka FAIL"
	@echo "2. Spark bağlantısı test ediliyor..."
	@curl -s http://localhost:8081 > /dev/null && echo "✅ Spark OK" || echo "❌ Spark FAIL"
	@echo "3. Kafka UI test ediliyor..."
	@curl -s http://localhost:8080 > /dev/null && echo "✅ Kafka UI OK" || echo "❌ Kafka UI FAIL"

# Default target
.DEFAULT_GOAL := help