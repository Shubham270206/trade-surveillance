# Real-Time Trade Surveillance System

A real-time trade surveillance system that detects spoofing, wash trading, and anomalous market behaviour using a multi-layer detection engine combining rule-based logic, statistical analysis, and Isolation Forest. Built on Kafka, PostgreSQL, FastAPI, and Streamlit with SHAP-powered alert explainability.

> Inspired by production surveillance systems at major financial institutions.

---

## Architecture

```
Trade Simulator
      ↓
Kafka (raw_trades)
      ↓
Detection Engine
   ├── Rule Layer       — spoofing, wash trading, layering
   ├── Statistical Layer — z-score volume spikes, order imbalance
   └── ML Layer         — Isolation Forest + SHAP explainability
      ↓
PostgreSQL (trades, alerts, trader_scores)
      ↓
FastAPI (REST + WebSocket)
      ↓
Streamlit Dashboard (live feed, charts, risk scores)
```

---

## Tech Stack

| Layer         | Technology                          |
|---------------|-------------------------------------|
| Streaming     | Apache Kafka 3.9 (KRaft mode)       |
| Detection     | Python — rules + scipy + scikit-learn |
| Explainability| SHAP                                |
| Database      | PostgreSQL 18 (partitioned schema)  |
| API           | FastAPI + WebSocket                 |
| Dashboard     | Streamlit + Plotly                  |
| Simulation    | Pure Python                         |

---

## Setup

### Prerequisites
- Python 3.11+
- Java 21+ (for Kafka)
- PostgreSQL 16+
- Apache Kafka 3.9 (KRaft mode — no Zookeeper needed)

### 1. Clone the repo
```bash
git clone https://github.com/Shubham270206/trade-surveillance.git
cd trade-surveillance
```

### 2. Create virtual environment
```bash
python -m venv venv
venv\Scripts\activate      # Windows
source venv/bin/activate   # Mac/Linux
pip install -r requirements.txt
```

### 3. Configure environment
Copy `.env.example` to `.env` and fill in your credentials:
```
POSTGRES_USER=surveillance
POSTGRES_PASSWORD=surveillance123
POSTGRES_DB=trades_db
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_TRADES=raw_trades
KAFKA_TOPIC_ALERTS=trade_alerts
```

### 4. Set up PostgreSQL
```bash
psql -U postgres -c "CREATE USER surveillance WITH PASSWORD 'surveillance123';"
psql -U postgres -c "CREATE DATABASE trades_db OWNER surveillance;"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE trades_db TO surveillance;"
psql -U postgres -d trades_db -f db/init.sql
```

### 5. Start Kafka (KRaft mode)
```bash
# One-time storage format
kafka-storage.bat format -t (kafka-storage.bat random-uuid) -c C:\kafka\config\kraft\server.properties

# Start Kafka (leave this window open)
kafka-server-start.bat C:\kafka\config\kraft\server.properties
```

### 6. Run the pipeline

**Terminal 1 — DB writer (consumes Kafka, writes to Postgres):**
```bash
python src/run_db_writer.py
```

**Terminal 2 — Simulator (produces trade events):**
```bash
python src/run_simulator.py
```

### 7. Run integration tests
```bash
python src/test_week1.py
```

---

## Project Structure

```
trade-surveillance/
├── db/
│   └── init.sql              # PostgreSQL schema
├── src/
│   ├── models.py             # TradeEvent dataclass
│   ├── simulator.py          # Normal trader simulation
│   ├── manipulators.py       # Spoofer + wash trader patterns
│   ├── kafka_pipeline.py     # Kafka producer + consumer
│   ├── run_simulator.py      # Simulator entry point
│   ├── run_db_writer.py      # DB writer entry point
│   └── test_week1.py         # Week 1 integration tests
├── .env                      # Credentials (not committed)
├── requirements.txt
└── README.md
```

---

## Detection Patterns

### Spoofing
Places large orders (3000–8000 shares) with 87% cancellation rate within 50–200ms, then executes a small real order on the opposite side to profit from the moved price.

### Wash Trading
Two coordinated accounts execute matching buy/sell pairs on the same symbol at nearly identical prices within 1–3 seconds — artificially inflating volume.

### Anomaly Detection *(Week 3)*
Isolation Forest trained on normal trader behaviour. Flags deviations with SHAP-powered explanations showing which features drove the anomaly score.

---

## Backtesting Results *(Week 4)*

| Alert Type  | Precision | Recall | F1   |
|-------------|-----------|--------|------|
| Spoofing    | —         | —      | —    |
| Wash Trade  | —         | —      | —    |
| Anomaly     | —         | —      | —    |

*Results will be filled in after Week 4 backtesting run.*

---

## Roadmap

- [x] Week 1 — Kafka pipeline + PostgreSQL + trade simulator
- [ ] Week 2 — Rule-based + statistical detection layers
- [ ] Week 3 — Isolation Forest + SHAP + FastAPI
- [ ] Week 4 — Streamlit dashboard + backtesting

---

## Author
Shubham Sinha — [github.com/Shubham270206](https://github.com/Shubham270206)
