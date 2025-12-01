# 🐦 Tweets Dashboard – Big Data Project

This project is a **Big Data dashboard** that collects and visualizes Twitter data. It uses **Apache Cassandra** to store tweets and **Grafana** to create real-time dashboards showcasing tweet statistics.  

Optional system monitoring is included using **Node Exporter** and **cAdvisor**.

---

## 🗂️ Project Overview

- **Data Source:** Twitter (tweets collected via API or dataset) 🐦  
- **Database:** Apache Cassandra – highly scalable, NoSQL database for storing tweets 💾  
- **Visualization:** Grafana – dashboard to display tweet statistics and trends 📊  
- **Monitoring:** Node Exporter & cAdvisor for system and container metrics 🖥️  

**Example Metrics on Dashboard:**
- Number of tweets per user  
- Most liked/retweeted tweets  
and more

---

## 🛠️ Services Setup

### Cassandra

Stores all tweets in a distributed, scalable database. Each tweet record can include:

```text
tweet_id | user | text | retweets | likes | date
