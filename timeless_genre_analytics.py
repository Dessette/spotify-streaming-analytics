import pandas as pd
import numpy as np
from collections import defaultdict, deque
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass
from typing import Dict, List, Tuple
import json

logger = logging.getLogger(__name__)

@dataclass
class GenreTimelessMetric:
    genre: str
    decade_presence: int
    consistency_score: float
    longevity_score: float
    variance_score: float
    peak_stability: float
    timeless_score: float
    period_stats: Dict
    last_updated: datetime

class TimelessGenreAnalyzer:
    def __init__(self, analysis_periods=None):
        if analysis_periods is None:
            analysis_periods = [5, 10]
            
        self.analysis_periods = analysis_periods
        self.genre_data_buffer = deque(maxlen=5000)  # Küçültüldü
        self.timeless_metrics = {}
        self.historical_rankings = deque(maxlen=50)  # Küçültüldü
        
        # Hızlı cache
        self.analysis_cache = {
            'last_analysis': None,
            'cache_duration': 30,  # 30 saniye cache
            'cached_results': {}
        }
        
        # Minimum requirements azaltıldı
        self.min_songs_per_period = 5  # 10'dan 5'e
        self.min_periods_for_timeless = 2  # 3'ten 2'ye
        
    def add_song_data(self, song_data: Dict):
        try:
            # Event tracking yoksa oluştur (backward compatibility)
            if not hasattr(self, 'event_tracking'):
                self.event_tracking = {
                    'processed_events': 0,
                    'rollback_events': 0,
                    'surge_events': 0,
                    'historical_events': 0,
                    'event_impact_history': deque(maxlen=100)
                }
            
            event_type = song_data.get('event_type', 'historical')
            
            processed_data = {
                'timestamp': datetime.now(),
                'title': song_data.get('title', 'Unknown'),
                'artist': song_data.get('artist', 'Unknown'),
                'genre': song_data.get('top_genre', 'unknown'),
                'year': int(song_data.get('year', 2000)),
                'popularity': float(song_data.get('popularity', 0)),
                'energy': float(song_data.get('energy', 0)),
                'danceability': float(song_data.get('danceability', 0)),
                'valence': float(song_data.get('valence', 0)),
                'event_type': event_type
            }
            
            # Event type'a göre işleme
            if event_type == 'rollback_original':
                self.handle_rollback_event(processed_data)
            elif event_type == 'popularity_surge':
                self.handle_surge_event(processed_data)
            else:
                self.handle_historical_event(processed_data)
            
            self.genre_data_buffer.append(processed_data)
            
            # Event tracking güncelle
            self.event_tracking['processed_events'] += 1
            if event_type == 'rollback_original':
                self.event_tracking['rollback_events'] += 1
            elif event_type == 'popularity_surge':
                self.event_tracking['surge_events'] += 1
            else:
                self.event_tracking['historical_events'] += 1
            
        except Exception as e:
            logger.error(f"Song data processing error: {e}")
    
    def handle_rollback_event(self, processed_data):
        """Rollback event'leri için özel işleme"""
        try:
            # Rollback event impact'ini kaydet
            impact_record = {
                'timestamp': datetime.now(),
                'event_type': 'rollback',
                'genre': processed_data['genre'],
                'title': processed_data['title'],
                'artist': processed_data['artist'],
                'popularity': processed_data['popularity'],
                'impact': 'negative_correction'  # Popularity düşüş
            }
            
            self.event_tracking['event_impact_history'].append(impact_record)
            
            logger.debug(f"Rollback processed: {processed_data['title']} by {processed_data['artist']}")
            
        except Exception as e:
            logger.error(f"Rollback event handling error: {e}")
    
    def handle_surge_event(self, processed_data):
        """Popularity surge event'leri için özel işleme"""
        try:
            # Surge event impact'ini kaydet
            impact_record = {
                'timestamp': datetime.now(),
                'event_type': 'surge',
                'genre': processed_data['genre'],
                'title': processed_data['title'],
                'artist': processed_data['artist'],
                'popularity': processed_data['popularity'],
                'impact': 'positive_boost'  # Popularity artış
            }
            
            self.event_tracking['event_impact_history'].append(impact_record)
            
            logger.debug(f"Surge processed: {processed_data['title']} by {processed_data['artist']}")
            
        except Exception as e:
            logger.error(f"Surge event handling error: {e}")
    
    def handle_historical_event(self, processed_data):
        """Normal historical event'ler için standart işleme"""
        try:
            # Historical event'ler için impact kaydı (opsiyonel)
            if self.event_tracking['processed_events'] % 100 == 0:  # Her 100 event'te bir log
                impact_record = {
                    'timestamp': datetime.now(),
                    'event_type': 'historical',
                    'genre': processed_data['genre'],
                    'impact': 'neutral'
                }
                self.event_tracking['event_impact_history'].append(impact_record)
            
        except Exception as e:
            logger.error(f"Historical event handling error: {e}")
    
    def calculate_period_stats(self, genre_songs: List[Dict], period_years: int) -> Dict:
        if not genre_songs:
            return {}
            
        years = sorted(set(song['year'] for song in genre_songs))
        if not years:
            return {}
            
        min_year, max_year = min(years), max(years)
        year_range = max_year - min_year + 1
        
        periods = []
        current_start = min_year
        
        while current_start <= max_year:
            period_end = min(current_start + period_years - 1, max_year)
            period_songs = [s for s in genre_songs 
                          if current_start <= s['year'] <= period_end]
            
            if len(period_songs) >= self.min_songs_per_period:
                period_stats = {
                    'start_year': current_start,
                    'end_year': period_end,
                    'song_count': len(period_songs),
                    'avg_popularity': np.mean([s['popularity'] for s in period_songs]),
                    'std_popularity': np.std([s['popularity'] for s in period_songs]),
                    'max_popularity': max(s['popularity'] for s in period_songs),
                    'min_popularity': min(s['popularity'] for s in period_songs),
                    'unique_artists': len(set(s['artist'] for s in period_songs)),
                    'avg_energy': np.mean([s['energy'] for s in period_songs]),
                    'avg_danceability': np.mean([s['danceability'] for s in period_songs]),
                    'avg_valence': np.mean([s['valence'] for s in period_songs])
                }
                periods.append(period_stats)
            
            current_start += period_years
        
        return {
            'periods': periods,
            'total_periods': len(periods),
            'year_span': year_range,
            'total_songs': len(genre_songs)
        }
    
    def calculate_consistency_score(self, period_stats: Dict) -> float:
        periods = period_stats.get('periods', [])
        if len(periods) < 2:
            return 0.0
            
        popularities = [p['avg_popularity'] for p in periods]
        pop_mean = np.mean(popularities)
        pop_variance = np.var(popularities) if len(popularities) > 1 else 0
        
        consistency_score = max(0, 100 - (pop_variance / pop_mean * 100) if pop_mean > 0 else 0)
        
        return min(100, consistency_score)
    
    def calculate_longevity_score(self, period_stats: Dict) -> float:
        periods = period_stats.get('periods', [])
        if not periods:
            return 0.0
            
        decade_presence = len(periods)
        year_span = period_stats.get('year_span', 0)
        year_bonus = min(50, year_span * 2)
        period_bonus = min(30, decade_presence * 10)
        total_songs = period_stats.get('total_songs', 0)
        song_bonus = min(20, np.log10(total_songs + 1) * 10) if total_songs > 0 else 0
        
        longevity_score = year_bonus + period_bonus + song_bonus
        return min(100, longevity_score)
    
    def calculate_peak_stability(self, period_stats: Dict) -> float:
        periods = period_stats.get('periods', [])
        if len(periods) < 2:
            return 0.0
            
        popularities = [p['avg_popularity'] for p in periods]
        sorted_pops = sorted(popularities, reverse=True)
        threshold = sorted_pops[len(sorted_pops)//2] if len(sorted_pops) > 1 else sorted_pops[0]
        
        peak_periods = [p for p in periods if p['avg_popularity'] >= threshold]
        
        if len(peak_periods) < 2:
            return 50.0
            
        peak_pops = [p['avg_popularity'] for p in peak_periods]
        peak_variance = np.var(peak_pops)
        peak_mean = np.mean(peak_pops)
        
        if peak_mean > 0:
            stability_score = max(0, 100 - (peak_variance / peak_mean * 200))
        else:
            stability_score = 0
            
        return min(100, stability_score)
    
    def calculate_timeless_score(self, consistency: float, longevity: float, 
                               peak_stability: float, period_count: int) -> float:
        weights = {
            'consistency': 0.35,
            'longevity': 0.30,
            'peak_stability': 0.25,
            'period_bonus': 0.10
        }
        
        if period_count < self.min_periods_for_timeless:
            return 0.0
            
        period_bonus = min(100, period_count * 20)
        
        timeless_score = (
            consistency * weights['consistency'] +
            longevity * weights['longevity'] +
            peak_stability * weights['peak_stability'] +
            period_bonus * weights['period_bonus']
        )
        
        return min(100, timeless_score)
    
    def analyze_genre_timelessness(self, force_refresh=False) -> Dict[str, GenreTimelessMetric]:
        current_time = datetime.now()
        
        # Cache kontrolü
        if (not force_refresh and 
            self.analysis_cache['last_analysis'] and
            (current_time - self.analysis_cache['last_analysis']).total_seconds() < 
            self.analysis_cache['cache_duration']):
            return self.analysis_cache['cached_results']
        
        all_songs = list(self.genre_data_buffer)
        if len(all_songs) < 20:  # Minimum veri azaltıldı
            return {}
        
        genre_groups = defaultdict(list)
        for song in all_songs:
            genre_groups[song['genre']].append(song)
        
        results = {}
        
        for genre, songs in genre_groups.items():
            if len(songs) < self.min_songs_per_period * 2:
                continue
                
            try:
                best_score = 0
                best_period_stats = None
                best_period_years = None
                
                for period_years in self.analysis_periods:
                    period_stats = self.calculate_period_stats(songs, period_years)
                    
                    if period_stats.get('total_periods', 0) < self.min_periods_for_timeless:
                        continue
                    
                    consistency = self.calculate_consistency_score(period_stats)
                    longevity = self.calculate_longevity_score(period_stats)
                    peak_stability = self.calculate_peak_stability(period_stats)
                    
                    popularities = [p['avg_popularity'] for p in period_stats.get('periods', [])]
                    if len(popularities) > 1:
                        variance = np.var(popularities)
                        mean_pop = np.mean(popularities)
                        variance_score = max(0, 100 - (variance / mean_pop * 50)) if mean_pop > 0 else 0
                    else:
                        variance_score = 0
                    
                    timeless_score = self.calculate_timeless_score(
                        consistency, longevity, peak_stability, 
                        period_stats.get('total_periods', 0)
                    )
                    
                    if timeless_score > best_score:
                        best_score = timeless_score
                        best_period_stats = period_stats
                        best_period_years = period_years
                
                if best_period_stats and best_score > 0:
                    metric = GenreTimelessMetric(
                        genre=genre,
                        decade_presence=best_period_stats.get('total_periods', 0),
                        consistency_score=self.calculate_consistency_score(best_period_stats),
                        longevity_score=self.calculate_longevity_score(best_period_stats),
                        variance_score=variance_score,
                        peak_stability=self.calculate_peak_stability(best_period_stats),
                        timeless_score=best_score,
                        period_stats=best_period_stats,
                        last_updated=current_time
                    )
                    
                    results[genre] = metric
                    
            except Exception as e:
                logger.error(f"Genre {genre} analizi hatası: {e}")
                continue
        
        self.analysis_cache['last_analysis'] = current_time
        self.analysis_cache['cached_results'] = results
        self.timeless_metrics = results
        
        if results:
            ranking = {
                'timestamp': current_time,
                'top_timeless': sorted(results.items(), 
                                     key=lambda x: x[1].timeless_score, 
                                     reverse=True)[:10]
            }
            self.historical_rankings.append(ranking)
        
        return results
    
    def get_top_timeless_genres(self, limit=10) -> List[Tuple[str, GenreTimelessMetric]]:
        if not self.timeless_metrics:
            self.analyze_genre_timelessness()
            
        sorted_genres = sorted(
            self.timeless_metrics.items(),
            key=lambda x: x[1].timeless_score,
            reverse=True
        )
        
        return sorted_genres[:limit]
    
    def get_genre_timeline(self, genre: str) -> Dict:
        if genre not in self.timeless_metrics:
            return {}
            
        metric = self.timeless_metrics[genre]
        periods = metric.period_stats.get('periods', [])
        
        timeline = []
        for period in periods:
            timeline.append({
                'period': f"{period['start_year']}-{period['end_year']}",
                'start_year': period['start_year'],
                'end_year': period['end_year'],
                'avg_popularity': round(period['avg_popularity'], 2),
                'song_count': period['song_count'],
                'artists': period['unique_artists'],
                'stability': round(100 - period['std_popularity'], 2) if period['std_popularity'] > 0 else 100
            })
        
        return {
            'genre': genre,
            'timeline': timeline,
            'overall_metrics': {
                'timeless_score': round(metric.timeless_score, 2),
                'consistency': round(metric.consistency_score, 2),
                'longevity': round(metric.longevity_score, 2),
                'peak_stability': round(metric.peak_stability, 2)
            }
        }
    
    def get_analytics_summary(self) -> Dict:
        # Event tracking yoksa oluştur (backward compatibility)
        if not hasattr(self, 'event_tracking'):
            self.event_tracking = {
                'processed_events': 0,
                'rollback_events': 0,
                'surge_events': 0,
                'historical_events': 0,
                'event_impact_history': deque(maxlen=100)
            }
            
        if not self.timeless_metrics:
            return {}
            
        top_timeless = self.get_top_timeless_genres(5)
        all_scores = [m.timeless_score for m in self.timeless_metrics.values()]
        decade_counts = [m.decade_presence for m in self.timeless_metrics.values()]
        
        summary = {
            'total_genres_analyzed': len(self.timeless_metrics),
            'top_timeless_genres': [
                {
                    'genre': genre,
                    'score': round(metric.timeless_score, 2),
                    'decades': metric.decade_presence,
                    'consistency': round(metric.consistency_score, 2)
                }
                for genre, metric in top_timeless
            ],
            'statistics': {
                'avg_timeless_score': round(np.mean(all_scores), 2) if all_scores else 0,
                'max_timeless_score': round(max(all_scores), 2) if all_scores else 0,
                'avg_decade_presence': round(np.mean(decade_counts), 2) if decade_counts else 0,
                'total_data_points': len(self.genre_data_buffer)
            },
            'event_system': {
                'processed_events': self.event_tracking['processed_events'],
                'rollback_events': self.event_tracking['rollback_events'],
                'surge_events': self.event_tracking['surge_events'],
                'historical_events': self.event_tracking['historical_events'],
                'rollback_percentage': round((self.event_tracking['rollback_events'] / max(1, self.event_tracking['surge_events'])) * 100, 1),
                'recent_impact_summary': self.get_recent_impact_summary()
            },
            'last_analysis': self.analysis_cache['last_analysis'].isoformat() if self.analysis_cache['last_analysis'] else None
        }
        
        return summary
    
    def get_recent_impact_summary(self) -> Dict:
        # Event tracking yoksa oluştur (backward compatibility)
        if not hasattr(self, 'event_tracking'):
            self.event_tracking = {
                'processed_events': 0,
                'rollback_events': 0,
                'surge_events': 0,
                'historical_events': 0,
                'event_impact_history': deque(maxlen=100)
            }
            
        try:
            recent_impacts = list(self.event_tracking['event_impact_history'])[-20:]  # Son 20 event
            
            if not recent_impacts:
                return {'surge_count': 0, 'rollback_count': 0, 'affected_genres': []}
            
            surge_count = len([i for i in recent_impacts if i['impact'] == 'positive_boost'])
            rollback_count = len([i for i in recent_impacts if i['impact'] == 'negative_correction'])
            
            # Etkilenen genre'ler
            affected_genres = list(set([i['genre'] for i in recent_impacts if i['event_type'] in ['surge', 'rollback']]))
            
            return {
                'surge_count': surge_count,
                'rollback_count': rollback_count,
                'affected_genres': affected_genres[:5],  # Top 5
                'net_impact': surge_count - rollback_count
            }
            
        except Exception as e:
            logger.error(f"Recent impact summary error: {e}")
            return {'surge_count': 0, 'rollback_count': 0, 'affected_genres': []}
    
    def get_event_analytics(self) -> Dict:
        # Event tracking yoksa oluştur (backward compatibility)
        if not hasattr(self, 'event_tracking'):
            self.event_tracking = {
                'processed_events': 0,
                'rollback_events': 0,
                'surge_events': 0,
                'historical_events': 0,
                'event_impact_history': deque(maxlen=100)
            }
            
        try:
            total_events = self.event_tracking['processed_events']
            surge_events = self.event_tracking['surge_events']
            rollback_events = self.event_tracking['rollback_events']
            
            # Event success rate (rollback olmayan surge'lar)
            active_surges = surge_events - rollback_events
            success_rate = (active_surges / max(1, surge_events)) * 100 if surge_events > 0 else 0
            
            # Genre impact distribution
            recent_impacts = list(self.event_tracking['event_impact_history'])[-50:]  # Son 50
            genre_impact_count = defaultdict(int)
            for impact in recent_impacts:
                if impact['event_type'] in ['surge', 'rollback']:
                    genre_impact_count[impact['genre']] += 1
            
            return {
                'total_events': total_events,
                'surge_events': surge_events,
                'rollback_events': rollback_events,
                'active_surges': active_surges,
                'success_rate': round(success_rate, 1),
                'most_impacted_genres': sorted(genre_impact_count.items(), key=lambda x: x[1], reverse=True)[:5],
                'event_frequency': {
                    'historical_percentage': round((self.event_tracking['historical_events'] / max(1, total_events)) * 100, 1),
                    'surge_percentage': round((surge_events / max(1, total_events)) * 100, 1),
                    'rollback_percentage': round((rollback_events / max(1, total_events)) * 100, 1)
                }
            }
            
        except Exception as e:
            logger.error(f"Event analytics error: {e}")
            return {}
    
    def get_trending_timeless_changes(self) -> List[Dict]:
        if len(self.historical_rankings) < 2:
            return []
            
        current = self.historical_rankings[-1]['top_timeless']
        previous = self.historical_rankings[-2]['top_timeless']
        
        current_ranks = {genre: i for i, (genre, _) in enumerate(current)}
        previous_ranks = {genre: i for i, (genre, _) in enumerate(previous)}
        
        changes = []
        for genre, current_rank in current_ranks.items():
            if genre in previous_ranks:
                rank_change = previous_ranks[genre] - current_rank
                if abs(rank_change) > 0:
                    changes.append({
                        'genre': genre,
                        'current_rank': current_rank + 1,
                        'previous_rank': previous_ranks[genre] + 1,
                        'change': rank_change,
                        'change_text': f"+{rank_change}" if rank_change > 0 else f"{rank_change}"
                    })
        
        return sorted(changes, key=lambda x: abs(x['change']), reverse=True)[:5]
    
    def get_genre_volatility_analysis(self) -> Dict:
        # Event tracking yoksa oluştur (backward compatibility)
        if not hasattr(self, 'event_tracking'):
            self.event_tracking = {
                'processed_events': 0,
                'rollback_events': 0,
                'surge_events': 0,
                'historical_events': 0,
                'event_impact_history': deque(maxlen=100)
            }
            
        try:
            recent_impacts = list(self.event_tracking['event_impact_history'])[-100:]
            
            # Genre başına volatility hesapla
            genre_volatility = defaultdict(lambda: {'surge_count': 0, 'rollback_count': 0, 'volatility_score': 0})
            
            for impact in recent_impacts:
                if impact['event_type'] == 'surge':
                    genre_volatility[impact['genre']]['surge_count'] += 1
                elif impact['event_type'] == 'rollback':
                    genre_volatility[impact['genre']]['rollback_count'] += 1
            
            # Volatility score hesapla (surge + rollback sayısı)
            for genre, data in genre_volatility.items():
                data['volatility_score'] = data['surge_count'] + data['rollback_count']
                data['stability_ratio'] = data['rollback_count'] / max(1, data['surge_count'])
            
            # En volatile genre'leri sırala
            most_volatile = sorted(
                [(genre, data) for genre, data in genre_volatility.items()],
                key=lambda x: x[1]['volatility_score'],
                reverse=True
            )[:5]
            
            return {
                'most_volatile_genres': [
                    {
                        'genre': genre,
                        'volatility_score': data['volatility_score'],
                        'surge_count': data['surge_count'],
                        'rollback_count': data['rollback_count'],
                        'stability_ratio': round(data['stability_ratio'], 2)
                    }
                    for genre, data in most_volatile
                ],
                'total_volatile_events': len(recent_impacts),
                'analysis_window': '100 recent events'
            }
            
        except Exception as e:
            logger.error(f"Volatility analysis error: {e}")
            return {'most_volatile_genres': [], 'total_volatile_events': 0}


if __name__ == "__main__":
    analyzer = TimelessGenreAnalyzer()
    
    test_genres = ['rock', 'pop', 'jazz', 'classical', 'hip-hop']
    test_years = list(range(1960, 2024))
    
    import random
    
    # Test verisi ekle (historical + surge + rollback)
    for i in range(200):
        genre = random.choice(test_genres)
        year = random.choice(test_years)
        
        if genre in ['jazz', 'classical']:
            base_popularity = 45 + random.gauss(0, 5)
        else:
            base_popularity = random.randint(20, 80)
            
        # Event type'ı belirle
        event_type = 'historical'
        if i % 20 == 0:  # %5 surge event
            event_type = 'popularity_surge'
            base_popularity = min(100, base_popularity + random.randint(15, 30))
        elif i % 25 == 0:  # %4 rollback event  
            event_type = 'rollback_original'
            
        test_song = {
            'title': f"Song {random.randint(1000, 9999)}",
            'artist': f"Artist {random.randint(100, 999)}",
            'top_genre': genre,
            'year': year,
            'popularity': max(0, min(100, base_popularity)),
            'energy': random.randint(20, 90),
            'danceability': random.randint(20, 90),
            'valence': random.randint(20, 90),
            'event_type': event_type
        }
        
        analyzer.add_song_data(test_song)
    
    results = analyzer.analyze_genre_timelessness()
    top_timeless = analyzer.get_top_timeless_genres()
    event_analytics = analyzer.get_event_analytics()
    volatility_analysis = analyzer.get_genre_volatility_analysis()
    
    print("TOP TIMELESS GENRES:")
    for i, (genre, metric) in enumerate(top_timeless, 1):
        print(f"{i}. {genre.upper()}: {metric.timeless_score:.1f}/100")
    
    print(f"\nEVENT ANALYTICS:")
    print(f"Total Events: {event_analytics.get('total_events', 0)}")
    print(f"Surge Events: {event_analytics.get('surge_events', 0)}")
    print(f"Rollback Events: {event_analytics.get('rollback_events', 0)}")
    print(f"Success Rate: {event_analytics.get('success_rate', 0)}%")
    
    print(f"\nMOST VOLATILE GENRES:")
    for genre_data in volatility_analysis.get('most_volatile_genres', [])[:3]:
        print(f"{genre_data['genre']}: {genre_data['volatility_score']} events")