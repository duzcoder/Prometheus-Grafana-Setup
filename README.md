# 🖥️ Monitoring Stack: Node Exporter & cAdvisor

This repository contains a **Docker Compose** setup to monitor your system and Docker containers using **Prometheus Node Exporter** and **cAdvisor**.  
Get real-time metrics for your system, containers, and hardware! 🚀

---

## 🛠️ Services

### 1️⃣ Node Exporter

**Image:** `prom/node-exporter:v1.8.2`  
**Purpose:** Collects hardware and OS metrics from your host machine.  

**Features:**
- Mounts `/proc`, `/sys`, and `/` in **read-only** mode 🔒
- Excludes system paths from filesystem metrics ❌
- Uses **host networking** for direct access 🌐
- Auto-restarts unless stopped 🔁

**Docker Compose snippet:**
```yaml
nodeexporter:
  image: prom/node-exporter:v1.8.2
  container_name: nodeexporter
  volumes:
    - /proc:/host/proc:ro
    - /sys:/host/sys:ro
    - /:/rootfs:ro
  command:
    - '--path.procfs=/host/proc'
    - '--path.rootfs=/rootfs'
    - '--path.sysfs=/host/sys'
    - '--collector.filesystem.mount-points-exclude=^/(sys|proc|dev|host|etc)($$|/)'
  restart: unless-stopped
  network_mode: host
  labels:
    org.label-schema.group: "monitoring"
