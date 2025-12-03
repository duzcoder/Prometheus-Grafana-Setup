##📊 Twitter Metrics Dashboard — Prometheus + Grafana + Cassandra

A complete Big Data analytics pipeline that collects Twitter metrics, stores tweet data in Cassandra, exposes processed metrics through a custom Prometheus exporter, and visualizes everything in Grafana.

This project showcases data engineering + monitoring + visualization skills with real-time dashboards and rich analytics.

###🚀 Features

✨ Twitter Data Collection

    -Custom Python script/exporter

    -Dockerized service exposing metrics at :9123/metrics

    -Cassandra Database

    -Stores raw tweet data

    -Runs via Docker

    -Connects seamlessly with the exporter

###📈 Prometheus Monitoring

Scrapes metrics from:

    ✔ Twitter Exporter (9123)

    ✔ Cassandra JMX Exporter (9500)

    ✔ Node Exporter (9100)

###📊 Grafana Dashboards

    -Includes visualizations for:

    -Tweet volume

    -Likes & retweets

    -Engagement distribution

    -Top users

    -Hourly tweet patterns

    -Active users

    -Viral tweets


###🐳 Running the Project (Docker Compose)
Start all services:
```docker compose up -d```

| Service            | Port | Description               |
| ------------------ | ---- | ------------------------- |
| twitter-exporter   | 9123 | Custom Prometheus metrics |
| node-exporter      | 9100 | Host system metrics       |
| cassandra-exporter | 9500 | Cassandra JMX metrics     |
| prometheus         | 9090 | Prometheus UI & storage   |
| grafana            | 3000 | Dashboards visualization  |

###🧰 Technologies Used

    -Python (custom exporter)

    -Cassandra (NoSQL storage)

    -Prometheus (metrics scraping & DB)

    -Grafana (visualization)

    -Node Exporter / JMX Exporter

    -Docker & Docker Compose

###📚 Use Cases

    -This project is ideal for:

    -Big Data class projects

    -Monitoring demonstrations

    -Real-time analytics practice

    -Building Grafana dashboards

    -Learning Prometheus exporter development