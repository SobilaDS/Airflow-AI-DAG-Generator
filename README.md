# ⚙️ Airflow AI DAG Generator

> **LangGraph + Devin CLI agent that watches your Snowflake schemas and autonomously generates, validates, and deploys Airflow DAGs — then heals failed ones.**


---

## 🧠 What It Does

Traditional Airflow management is reactive — a schema changes, your pipeline breaks, an engineer fixes it the next morning. This agent makes it proactive and autonomous.

| Trigger | Agent Action |
|---------|-------------|
| New column added to Snowflake table | Generates updated ETL DAG including the new column |
| Column removed from source table | Generates DAG with graceful `COALESCE` handling |
| Column type changed | Generates DAG with explicit `CAST` in transform SQL |
| DAG fails in production | Detects failure, retries via Airflow REST API |
| Invalid DAG generated | Catches syntax errors before deployment via AST validation |

---

## 🏗️ Architecture

```
Scheduled trigger (hourly)
        │
        ▼
  ┌─────────────────┐
  │   LangGraph     │  ◄── GPT-4o reasoning backbone
  │   Agent Graph   │
  └────────┬────────┘
           │
    ┌──────┴───────┐
    │    Tools     │
    └──────┬───────┘
           ├── detect_schema_changes()  → Snowflake information_schema vs registry
           ├── get_failed_dags()        → Airflow REST API
           ├── generate_dag()           → Writes .py DAG file to DAGs folder
           ├── validate_dag_file()      → Python AST parse check
           └── trigger_dag_run()        → Airflow REST API (self-healing retry)

  dag_registry.json  ← persists known schemas between runs
```

---

## 🚀 Quickstart

```bash
# 1. Clone
git clone https://github.com/SobilaDS/airflow-ai-dag-generator.git
cd airflow-ai-dag-generator

# 2. Install
pip install -r requirements.txt

# 3. Configure
cp .env.example .env
# Fill in SNOWFLAKE_* and AIRFLOW_* variables

# 4. Run the generator
python dag_generator.py
```

---

## 📁 Project Structure

```
airflow-ai-dag-generator/
├── dag_generator.py       # Main LangGraph agent
├── dag_registry.json      # Auto-managed schema registry (gitignore in prod)
├── dags/                  # Generated DAGs land here
├── requirements.txt
├── .env.example
└── README.md
```

---

## 🛡️ Safety Controls

- **AST validation** before every DAG deployment — malformed code never reaches Airflow
- **Registry diffing** — only changed schemas trigger new DAG generation
- **Maximum retry cap** — failed DAGs retried once; escalates to Slack on second failure
- **Human-in-the-loop** for breaking schema changes (column removed, type narrowed)

---


## 🛠️ Tech Stack

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![LangGraph](https://img.shields.io/badge/LangGraph-1B2A4A?style=flat-square&logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat-square&logo=snowflake&logoColor=white)
![OpenAI](https://img.shields.io/badge/GPT--4o-412991?style=flat-square&logo=openai&logoColor=white)

---

## 📄 License

MIT
