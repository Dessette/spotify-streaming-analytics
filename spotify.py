from pyspark import SparkContext, SparkConf
from operator import add
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from collections import defaultdict
import os

# Matplotlib için Türkçe karakter desteği
plt.rcParams['font.family'] = ['DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# Grafiklerin kaydedileceği klasörü oluştur
if not os.path.exists('graphs'):
    os.makedirs('graphs')

# Seaborn stil ayarları
sns.set_style("whitegrid")
plt.style.use('seaborn-v0_8')

# Spark yapılandırması
conf = SparkConf().setAppName("WordCount").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Log seviyesini düşür
sc.setLogLevel("ERROR")


import kagglehub
import os

# Download latest version
path = kagglehub.dataset_download("iamsumat/spotify-top-2000s-mega-dataset")

print("Path to dataset files:", path)

# Dataset dosyalarını listele
dataset_files = os.listdir(path)
print("Available files:", dataset_files)

# CSV dosyasını bul
csv_file = None
for file in dataset_files:
    if file.endswith('.csv'):
        csv_file = os.path.join(path, file)
        break

if csv_file is None:
    print("CSV dosyası bulunamadı!")
    sc.stop()
    exit()

print(f"CSV file found: {csv_file}")

def parse_line(line):
    """CSV satırını parse ederek dictionary döndür"""
    try:
        # CSV parsing için daha güvenli bir yöntem kullan
        import csv
        from io import StringIO
        
        # CSV reader kullanarak parse et
        reader = csv.reader(StringIO(line))
        fields = next(reader)
        
        if len(fields) < 15:  # En az 15 alan olmalı
            return None
            
        return {
            'index': int(fields[0]),
            'title': fields[1].strip(),
            'artist': fields[2].strip(),
            'top_genre': fields[3].strip(),
            'year': int(fields[4]),
            'bpm': int(fields[5]),
            'energy': int(fields[6]),
            'danceability': int(fields[7]),
            'loudness': int(fields[8]),
            'liveness': int(fields[9]),
            'valence': int(fields[10]),
            'length': int(fields[11]),
            'acousticness': int(fields[12]),
            'speechiness': int(fields[13]),
            'popularity': int(fields[14])
        }
    except Exception as e:
        return None

def create_line_chart(data_dict, content_type, title):
    """Yıllar arası trend için çizgi grafiği oluştur"""
    plt.figure(figsize=(12, 8))
    
    years = sorted(data_dict.keys())
    values = [data_dict[year] for year in years]
    
    plt.plot(years, values, marker='o', linewidth=2.5, markersize=8)
    plt.title(f'{title} - Yıllar Arası Trend', fontsize=16, fontweight='bold')
    plt.xlabel('Yıl', fontsize=12)
    plt.ylabel('Toplam Popülerlik Skoru', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    filename = f'graphs/{content_type}_trend.png'
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"   📊 Trend grafiği kaydedildi: {filename}")

def create_bar_chart(year_data, content_type, year, top_n=10):
    """Belirli bir yıl için bar grafiği oluştur"""
    # Veriyi sırala ve ilk top_n tanesini al
    sorted_data = sorted(year_data, key=lambda x: x[1], reverse=True)[:top_n]
    
    if not sorted_data:
        return
    
    labels = [str(item[0][1]) for item in sorted_data]  # content değeri
    values = [item[1] for item in sorted_data]  # popülerlik skoru
    
    plt.figure(figsize=(14, 8))
    colors = plt.cm.viridis(np.linspace(0, 1, len(labels)))
    
    bars = plt.bar(range(len(labels)), values, color=colors)
    plt.title(f'{content_type.title()} - {year} Yılı En Popüler {top_n}', fontsize=16, fontweight='bold')
    plt.xlabel(content_type.title(), fontsize=12)
    plt.ylabel('Toplam Popülerlik Skoru', fontsize=12)
    
    # X eksenindeki etiketleri döndür
    plt.xticks(range(len(labels)), labels, rotation=45, ha='right')
    
    # Bar üzerine değerleri yaz
    for i, bar in enumerate(bars):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}', ha='center', va='bottom', fontsize=10)
    
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    
    filename = f'graphs/{content_type}_{year}_top{top_n}.png'
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"   📊 {year} bar grafiği kaydedildi: {filename}")

def create_heatmap(all_years_data, content_type):
    """Tüm yıllar için heatmap oluştur"""
    # Veriyi pandas DataFrame'e dönüştür
    heatmap_data = defaultdict(lambda: defaultdict(int))
    
    for year, year_data in all_years_data.items():
        year_list = list(year_data)
        # En popüler 15 tanesini al
        sorted_year_data = sorted(year_list, key=lambda x: x[1], reverse=True)[:15]
        
        for ((y, content_val), popularity) in sorted_year_data:
            heatmap_data[str(content_val)][year] = popularity
    
    # DataFrame oluştur
    df = pd.DataFrame(heatmap_data).T.fillna(0)
    
    if df.empty or df.shape[0] < 2:
        print(f"   ⚠️  {content_type} için yeterli veri yok, heatmap atlanıyor")
        return
    
    plt.figure(figsize=(16, 10))
    sns.heatmap(df, annot=False, cmap='YlOrRd', cbar_kws={'label': 'Popülerlik Skoru'})
    plt.title(f'{content_type.title()} Popülerlik Heatmap - Yıllar Arası', fontsize=16, fontweight='bold')
    plt.xlabel('Yıl', fontsize=12)
    plt.ylabel(content_type.title(), fontsize=12)
    plt.xticks(rotation=45)
    plt.yticks(rotation=0)
    plt.tight_layout()
    
    filename = f'graphs/{content_type}_heatmap.png'
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"   📊 Heatmap kaydedildi: {filename}")

try:
    # CSV dosyasını oku
    data = sc.textFile(csv_file)
    header = data.first()

    
    """
    Content
    Index: ID
    Title: Name of the Track
    Artist: Name of the Artist
    Top Genre: Genre of the track
    Year: Release Year of the track
    Beats per Minute(BPM): The tempo of the song
    Energy: The energy of a song - the higher the value, the more energtic. song
    Danceability: The higher the value, the easier it is to dance to this song.
    Loudness: The higher the value, the louder the song.
    Valence: The higher the value, the more positive mood for the song.
    Length: The duration of the song.
    Acoustic: The higher the value the more acoustic the song is.
    Speechiness: The higher the value the more spoken words the song contains
    Popularity: The higher the value the more popular the song is.    """
# Spotify kullanıcılarının zaman içerisindeki
# müzik türü tercihlerindeki değişimleri analiz etmek.

    contents = ['artist', 'top_genre', 'bpm', 'energy', 'danceability', 
                'loudness', 'liveness', 'valence', 'length', 'acousticness', 'speechiness']
        
    # Header'ı filtrele ve parse et
    spotify_data = data.filter(lambda line: line != header).map(parse_line).filter(lambda x: x is not None)
    spotify_data = data.filter()
    spotify_data.cache()  # Veriyi önbelleğe al

    for content in contents:
        # Yıllara göre en popüler müzik türlerini bul
        top_contents_per_year = spotify_data.map(lambda x: ( (x['year'],x[content]) , x['popularity'] ))\
                                        .reduceByKey(add)\
                                        .sortBy(lambda x: x[1], ascending=False)\
                                        .groupBy(lambda x: x[0][0])\
                                        .sortByKey()\
                                        .collect()
        

        print(f"\n🎵 === {content.upper()} ANALİZİ === 🎵")
        
        # Grafik verileri için dictionary'ler
        all_years_data = {}
        trend_data = {}
        
        for year, content_data in top_contents_per_year:
            print(f"\n{year} yılı:")
            
            # ResultIterable'ı listeye çevir
            content_list = list(content_data)
            all_years_data[year] = content_list
            
            # Trend verisi için o yılın toplam popülerliğini hesapla
            total_popularity = sum([pop for ((y,c),pop) in content_list])
            trend_data[year] = total_popularity
            
            # Terminal çıktısını koru
            print("  Sortlama olmadan:", end=" ")
            unsorted_top5 = [f"{c}({pop})" for ((y,c),pop) in content_list[:10]]
            print(', '.join(unsorted_top5))
            
            # Her yıl için bar grafik oluştur (sadece önemli yıllar için)
            if year % 5 == 0:  # Her 5 yılda bir
                create_bar_chart(content_list, content, year)
        
        # Genel trend grafiği oluştur
        content_title = content.replace('_', ' ').title()
        create_line_chart(trend_data, content, content_title)
        
        # Heatmap oluştur
        create_heatmap(all_years_data, content)
        
        print(f"✅ {content} analizi tamamlandı, grafikler 'graphs/' klasörüne kaydedildi.")
            
    
except Exception as e:
    print(f"Hata: {e}")
    
finally:
    # SparkContext'i kapat
    sc.stop()
