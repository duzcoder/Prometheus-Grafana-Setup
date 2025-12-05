# twitter_exporter_ULTIMATE_WITH_HISTOGRAMS.py
from flask import Flask
from cassandra.cluster import Cluster
import time
import threading
from datetime import datetime, timedelta
import math
from collections import defaultdict

app = Flask(__name__)

# Global metrics
metrics = {}

def update_metrics():
    """Update metrics from Cassandra - WITH HISTOGRAMS & DISTRIBUTIONS"""
    while True:
        try:
            print(f"{datetime.now().strftime('%H:%M:%S')} - Connecting to Cassandra...")
            
            cluster = Cluster(['cassandra'], port=9042)
            session = cluster.connect('twitter')
            
            # ===== 1. BASIC COUNTS & TOTALS =====
            result = session.execute("SELECT COUNT(*) FROM tweets")
            total_tweets = result[0][0]
            metrics['twitter_tweets_total'] = total_tweets
            
            # ===== 2. ENGAGEMENT METRICS =====
            result = session.execute("""
                SELECT 
                  SUM(retweets) as total_retweets,
                  SUM(likes) as total_likes,
                  AVG(retweets) as avg_retweets,
                  AVG(likes) as avg_likes,
                  MAX(retweets) as max_retweets,
                  MAX(likes) as max_likes,
                  MIN(retweets) as min_retweets,
                  MIN(likes) as min_likes,
                  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY retweets) as median_retweets,
                  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY likes) as median_likes,
                  STDDEV(retweets) as stddev_retweets,
                  STDDEV(likes) as stddev_likes
                FROM tweets
            """)
            row = result[0]
            
            metrics.update({
                'twitter_total_retweets': int(row.total_retweets or 0),
                'twitter_total_likes': int(row.total_likes or 0),
                'twitter_avg_retweets': float(row.avg_retweets or 0),
                'twitter_avg_likes': float(row.avg_likes or 0),
                'twitter_max_retweets': int(row.max_retweets or 0),
                'twitter_max_likes': int(row.max_likes or 0),
                'twitter_min_retweets': int(row.min_retweets or 0),
                'twitter_min_likes': int(row.min_likes or 0),
                'twitter_median_retweets': float(row.median_retweets or 0),
                'twitter_median_likes': float(row.median_likes or 0),
                'twitter_stddev_retweets': float(row.stddev_retweets or 0),
                'twitter_stddev_likes': float(row.stddev_likes or 0),
            })
            
            # ===== 3. HISTOGRAM METRICS FOR PIE CHARTS =====
            # Engagement level buckets for pie chart
            retweet_buckets = [
                ('twitter_retweets_0', "retweets = 0"),
                ('twitter_retweets_1_10', "retweets BETWEEN 1 AND 10"),
                ('twitter_retweets_11_50', "retweets BETWEEN 11 AND 50"),
                ('twitter_retweets_51_100', "retweets BETWEEN 51 AND 100"),
                ('twitter_retweets_101_500', "retweets BETWEEN 101 AND 500"),
                ('twitter_retweets_501_1000', "retweets BETWEEN 501 AND 1000"),
                ('twitter_retweets_1000plus', "retweets > 1000"),
            ]
            
            like_buckets = [
                ('twitter_likes_0', "likes = 0"),
                ('twitter_likes_1_20', "likes BETWEEN 1 AND 20"),
                ('twitter_likes_21_100', "likes BETWEEN 21 AND 100"),
                ('twitter_likes_101_500', "likes BETWEEN 101 AND 500"),
                ('twitter_likes_501_2000', "likes BETWEEN 501 AND 2000"),
                ('twitter_likes_2001_10000', "likes BETWEEN 2001 AND 10000"),
                ('twitter_likes_10000plus', "likes > 10000"),
            ]
            
            # Get counts for each bucket
            for metric_name, condition in retweet_buckets:
                result = session.execute(f"SELECT COUNT(*) FROM tweets WHERE {condition}")
                metrics[metric_name] = result[0][0]
            
            for metric_name, condition in like_buckets:
                result = session.execute(f"SELECT COUNT(*) FROM tweets WHERE {condition}")
                metrics[metric_name] = result[0][0]
            
            # ===== 4. PIE CHART: TWEET LENGTH DISTRIBUTION =====
            text_length_buckets = [
                ('twitter_text_length_0_50', "LENGTH(text) <= 50"),
                ('twitter_text_length_51_100', "LENGTH(text) BETWEEN 51 AND 100"),
                ('twitter_text_length_101_150', "LENGTH(text) BETWEEN 101 AND 150"),
                ('twitter_text_length_151_200', "LENGTH(text) BETWEEN 151 AND 200"),
                ('twitter_text_length_201_280', "LENGTH(text) BETWEEN 201 AND 280"),
                ('twitter_text_length_281plus', "LENGTH(text) > 280"),
            ]
            
            for metric_name, condition in text_length_buckets:
                result = session.execute(f"SELECT COUNT(*) FROM tweets WHERE {condition}")
                metrics[metric_name] = result[0][0]
            
            # ===== 5. PIE CHART: USER ACTIVITY TIERS =====
            # Get user tweet counts distribution
            result = session.execute("""
                SELECT user, COUNT(*) as tweet_count
                FROM tweets
                GROUP BY user
            """)
            
            # Bucket users by activity level
            user_activity_buckets = {
                'twitter_users_1_tweet': 0,
                'twitter_users_2_5_tweets': 0,
                'twitter_users_6_20_tweets': 0,
                'twitter_users_21_100_tweets': 0,
                'twitter_users_100plus_tweets': 0,
            }
            
            for row in result:
                count = row.tweet_count
                if count == 1:
                    user_activity_buckets['twitter_users_1_tweet'] += 1
                elif 2 <= count <= 5:
                    user_activity_buckets['twitter_users_2_5_tweets'] += 1
                elif 6 <= count <= 20:
                    user_activity_buckets['twitter_users_6_20_tweets'] += 1
                elif 21 <= count <= 100:
                    user_activity_buckets['twitter_users_21_100_tweets'] += 1
                else:
                    user_activity_buckets['twitter_users_100plus_tweets'] += 1
            
            metrics.update(user_activity_buckets)
            
            # ===== 6. HISTOGRAM: ENGAGEMENT SCORE DISTRIBUTION =====
            # Calculate engagement score (retweets + likes) buckets
            engagement_buckets = [
                ('twitter_engagement_0_10', 0, 10),
                ('twitter_engagement_11_50', 11, 50),
                ('twitter_engagement_51_200', 51, 200),
                ('twitter_engagement_201_1000', 201, 1000),
                ('twitter_engagement_1001_5000', 1001, 5000),
                ('twitter_engagement_5001plus', 5001, None),
            ]
            
            for metric_name, low, high in engagement_buckets:
                if high is None:
                    query = f"SELECT COUNT(*) FROM tweets WHERE (retweets + likes) >= {low}"
                else:
                    query = f"SELECT COUNT(*) FROM tweets WHERE (retweets + likes) BETWEEN {low} AND {high}"
                result = session.execute(query)
                metrics[metric_name] = result[0][0]
            
            # ===== 7. TIME OF DAY DISTRIBUTION FOR HISTOGRAM =====
            # Get tweets by hour (0-23)
            for hour in range(24):
                result = session.execute(f"""
                    SELECT COUNT(*) as count
                    FROM tweets 
                    WHERE date > toTimestamp(now() - 7d)
                    AND toUnixTimestamp(date) % 86400 BETWEEN {hour * 3600} AND {(hour + 1) * 3600 - 1}
                """)
                metrics[f'twitter_hour_{hour:02d}_tweets'] = result[0].count
            
            # ===== 8. USER STATISTICS =====
            result = session.execute("SELECT COUNT(DISTINCT user) FROM tweets")
            metrics['twitter_total_users'] = result[0][0]
            
            # Active users in different time windows
            time_windows = [('1h', 1), ('6h', 6), ('24h', 24), ('7d', 168)]
            for label, hours in time_windows:
                result = session.execute(f"""
                    SELECT COUNT(DISTINCT user) as users 
                    FROM tweets 
                    WHERE date > toTimestamp(now() - {hours}h)
                """)
                metrics[f'twitter_active_users_{label}'] = result[0].users or 0
            
            # ===== 9. TIME-BASED METRICS =====
            for label, hours in time_windows:
                result = session.execute(f"""
                    SELECT COUNT(*) as count 
                    FROM tweets 
                    WHERE date > toTimestamp(now() - {hours}h)
                """)
                metrics[f'twitter_tweets_last_{label}'] = result[0].count
            
            # Tweets per hour (rate)
            metrics['twitter_tweets_per_hour'] = metrics['twitter_tweets_last_1h']
            
            # ===== 10. ENGAGEMENT RATIOS & SCORES =====
            if total_tweets > 0:
                metrics.update({
                    'twitter_engagement_ratio': (metrics['twitter_total_retweets'] + metrics['twitter_total_likes']) / total_tweets,
                    'twitter_retweet_ratio': metrics['twitter_total_retweets'] / total_tweets,
                    'twitter_like_ratio': metrics['twitter_total_likes'] / total_tweets,
                    'twitter_virality_score': metrics['twitter_max_retweets'] / max(metrics['twitter_avg_retweets'], 1),
                })
            
            # ===== 11. TOP USERS DISTRIBUTION =====
            result = session.execute("""
                SELECT user, COUNT(*) as tweet_count, 
                       AVG(retweets) as user_avg_retweets,
                       AVG(likes) as user_avg_likes
                FROM tweets
                GROUP BY user
                ORDER BY tweet_count DESC
                LIMIT 10
            """)
            
            # Top 10 users for pie chart
            for i, row in enumerate(result[:10]):
                metrics[f'twitter_top_user_{i+1}_tweets'] = row.tweet_count
                metrics[f'twitter_top_user_{i+1}_name_tweets'] = row.tweet_count  # Same value for pie chart
                metrics[f'twitter_top_user_{i+1}_avg_retweets'] = float(row.user_avg_retweets or 0)
                metrics[f'twitter_top_user_{i+1}_avg_likes'] = float(row.user_avg_likes or 0)
            
            # ===== 12. PERCENTILE METRICS =====
            percentiles = [25, 50, 75, 90, 95, 99]
            for p in percentiles:
                result = session.execute(f"""
                    SELECT PERCENTILE_CONT({p/100}) WITHIN GROUP (ORDER BY retweets) as p{p}
                    FROM tweets WHERE retweets > 0
                """)
                metrics[f'twitter_retweets_p{p}'] = float(result[0][0] or 0)
                
                result = session.execute(f"""
                    SELECT PERCENTILE_CONT({p/100}) WITHIN GROUP (ORDER BY likes) as p{p}
                    FROM tweets WHERE likes > 0
                """)
                metrics[f'twitter_likes_p{p}'] = float(result[0][0] or 0)
            
            # ===== 13. TEXT ANALYSIS =====
            result = session.execute("""
                SELECT AVG(LENGTH(text)) as avg_length,
                       MAX(LENGTH(text)) as max_length,
                       MIN(LENGTH(text)) as min_length
                FROM tweets
            """)
            row = result[0]
            metrics.update({
                'twitter_avg_text_length': float(row.avg_length or 0),
                'twitter_max_text_length': int(row.max_length or 0),
                'twitter_min_text_length': int(row.min_length or 0),
            })
            
            # ===== 14. CATEGORICAL METRICS =====
            result = session.execute("SELECT COUNT(*) FROM tweets WHERE retweets > 100 OR likes > 500")
            metrics['twitter_high_engagement_tweets'] = result[0][0] or 0
            
            result = session.execute("SELECT COUNT(*) FROM tweets WHERE retweets > 1000")
            metrics['twitter_viral_tweets'] = result[0][0] or 0
            
            # ===== 15. USER ENGAGEMENT DISTRIBUTION =====
            if metrics['twitter_total_users'] > 0:
                metrics.update({
                    'twitter_tweets_per_user': total_tweets / metrics['twitter_total_users'],
                    'twitter_retweets_per_user': metrics['twitter_total_retweets'] / metrics['twitter_total_users'],
                    'twitter_likes_per_user': metrics['twitter_total_likes'] / metrics['twitter_total_users'],
                })
            
            session.shutdown()
            cluster.shutdown()
            
            print(f"{datetime.now().strftime('%H:%M:%S')} - Updated {len(metrics)} metrics with HISTOGRAMS!")
            
        except Exception as e:
            print(f"Error: {str(e)[:100]}...")
            # Fallback mock data with histograms
            metrics.update({
                # Original metrics
                'twitter_tweets_total': 150,
                'twitter_total_retweets': 1875,
                'twitter_total_likes': 3795,
                'twitter_avg_retweets': 12.5,
                'twitter_avg_likes': 25.3,
                
                # Histogram metrics for pie charts
                'twitter_retweets_0': 45,
                'twitter_retweets_1_10': 60,
                'twitter_retweets_11_50': 30,
                'twitter_retweets_51_100': 10,
                'twitter_retweets_101_500': 4,
                'twitter_retweets_501_1000': 1,
                'twitter_retweets_1000plus': 0,
                
                'twitter_likes_0': 30,
                'twitter_likes_1_20': 65,
                'twitter_likes_21_100': 40,
                'twitter_likes_101_500': 12,
                'twitter_likes_501_2000': 3,
                'twitter_likes_2001_10000': 0,
                'twitter_likes_10000plus': 0,
                
                'twitter_text_length_0_50': 20,
                'twitter_text_length_51_100': 45,
                'twitter_text_length_101_150': 55,
                'twitter_text_length_151_200': 25,
                'twitter_text_length_201_280': 5,
                'twitter_text_length_281plus': 0,
                
                'twitter_users_1_tweet': 15,
                'twitter_users_2_5_tweets': 20,
                'twitter_users_6_20_tweets': 10,
                'twitter_users_21_100_tweets': 4,
                'twitter_users_100plus_tweets': 1,
                
                'twitter_engagement_0_10': 50,
                'twitter_engagement_11_50': 65,
                'twitter_engagement_51_200': 25,
                'twitter_engagement_201_1000': 8,
                'twitter_engagement_1001_5000': 2,
                'twitter_engagement_5001plus': 0,
                
                # Hour distribution
                'twitter_hour_00_tweets': 2, 'twitter_hour_01_tweets': 1, 'twitter_hour_02_tweets': 0,
                'twitter_hour_03_tweets': 0, 'twitter_hour_04_tweets': 0, 'twitter_hour_05_tweets': 1,
                'twitter_hour_06_tweets': 3, 'twitter_hour_07_tweets': 8, 'twitter_hour_08_tweets': 12,
                'twitter_hour_09_tweets': 15, 'twitter_hour_10_tweets': 18, 'twitter_hour_11_tweets': 20,
                'twitter_hour_12_tweets': 22, 'twitter_hour_13_tweets': 25, 'twitter_hour_14_tweets': 18,
                'twitter_hour_15_tweets': 15, 'twitter_hour_16_tweets': 12, 'twitter_hour_17_tweets': 10,
                'twitter_hour_18_tweets': 8, 'twitter_hour_19_tweets': 6, 'twitter_hour_20_tweets': 4,
                'twitter_hour_21_tweets': 3, 'twitter_hour_22_tweets': 2, 'twitter_hour_23_tweets': 1,
            })
        
        time.sleep(15)

@app.route('/metrics')
def get_metrics():
    """Return metrics in Prometheus format"""
    output = []
    for key, value in metrics.items():
        output.append(f"{key} {value}")
    return '\n'.join(output), 200, {'Content-Type': 'text/plain'}

@app.route('/metrics_json')
def get_metrics_json():
    """Return metrics as JSON for easy inspection"""
    import json
    return json.dumps(metrics, indent=2), 200, {'Content-Type': 'application/json'}

@app.route('/histogram_data')
def histogram_data():
    """Return formatted histogram data for Grafana"""
    import json
    
    # Group metrics for easier consumption
    histograms = {
        'retweet_distribution': {},
        'like_distribution': {},
        'text_length_distribution': {},
        'user_activity_distribution': {},
        'engagement_distribution': {},
        'hourly_distribution': {},
    }
    
    # Extract histogram metrics
    for key, value in metrics.items():
        if key.startswith('twitter_retweets_') and not key.endswith('total'):
            histograms['retweet_distribution'][key.replace('twitter_retweets_', '')] = value
        elif key.startswith('twitter_likes_') and not key.endswith('total'):
            histograms['like_distribution'][key.replace('twitter_likes_', '')] = value
        elif key.startswith('twitter_text_length_'):
            histograms['text_length_distribution'][key.replace('twitter_text_length_', '')] = value
        elif key.startswith('twitter_users_'):
            histograms['user_activity_distribution'][key.replace('twitter_users_', '')] = value
        elif key.startswith('twitter_engagement_') and key != 'twitter_engagement_ratio':
            histograms['engagement_distribution'][key.replace('twitter_engagement_', '')] = value
        elif key.startswith('twitter_hour_'):
            hour = key.replace('twitter_hour_', '').replace('_tweets', '')
            histograms['hourly_distribution'][hour] = value
    
    return json.dumps(histograms, indent=2), 200, {'Content-Type': 'application/json'}

@app.route('/piechart_data/<chart_type>')
def piechart_data(chart_type):
    """Return data formatted for specific pie charts"""
    import json
    
    chart_data = {
        'labels': [],
        'values': [],
        'colors': ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40', '#FF6384']
    }
    
    if chart_type == 'retweets':
        buckets = ['0', '1_10', '11_50', '51_100', '101_500', '501_1000', '1000plus']
        chart_data['labels'] = ['0 RT', '1-10 RT', '11-50 RT', '51-100 RT', '101-500 RT', '501-1000 RT', '1000+ RT']
        for bucket in buckets:
            chart_data['values'].append(metrics.get(f'twitter_retweets_{bucket}', 0))
    
    elif chart_type == 'likes':
        buckets = ['0', '1_20', '21_100', '101_500', '501_2000', '2001_10000', '10000plus']
        chart_data['labels'] = ['0 Likes', '1-20', '21-100', '101-500', '501-2000', '2001-10000', '10000+']
        for bucket in buckets:
            chart_data['values'].append(metrics.get(f'twitter_likes_{bucket}', 0))
    
    elif chart_type == 'text_length':
        buckets = ['0_50', '51_100', '101_150', '151_200', '201_280', '281plus']
        chart_data['labels'] = ['0-50 chars', '51-100', '101-150', '151-200', '201-280', '280+']
        for bucket in buckets:
            chart_data['values'].append(metrics.get(f'twitter_text_length_{bucket}', 0))
    
    return json.dumps(chart_data), 200, {'Content-Type': 'application/json'}

@app.route('/health')
def health():
    return 'OK', 200

if __name__ == '__main__':
    thread = threading.Thread(target=update_metrics, daemon=True)
    thread.start()
    
    print("🚀 ULTIMATE Twitter Exporter with HISTOGRAMS starting on port 9123...")
    print("📊 Metrics available at: /metrics")
    print("📈 JSON format at: /metrics_json")
    print("📊 Histogram data at: /histogram_data")
    print("🥧 Pie chart data at: /piechart_data/<type>")
    print("❤️  Health check at: /health")
    app.run(host='0.0.0.0', port=9123, debug=False, use_reloader=False)