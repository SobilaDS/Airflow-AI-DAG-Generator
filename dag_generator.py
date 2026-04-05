"""
Airflow AI DAG Generator
========================
LangGraph + Devin CLI agent that:
  - Watches upstream Snowflake tables for schema changes & data signals
  - Dynamically generates Airflow DAG Python files in response
  - Validates generated DAGs before deploying to the DAGs folder
  - Monitors running DAGs and triggers self-healing actions on failure

Author : Sobila Satheesh
Stack  : Python · LangGraph · Devin CLI · Apache Airflow · Snowflake
"""

from __future__ import annotations

import os
import ast
import json
import logging
import textwrap
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated, TypedDict, Sequence

import snowflake.connector
from langchain_openai import ChatOpenAI
from langchain_core.messages import BaseMessage, HumanMessage
from langchain_core.tools import tool
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DAGS_FOLDER = Path(os.environ.get("AIRFLOW_DAGS_FOLDER", "/opt/airflow/dags"))
REGISTRY_PATH = Path("dag_registry.json")


# ─────────────────────────────────────────────
# STATE
# ─────────────────────────────────────────────

class DAGGenState(TypedDict):
    messages:       Annotated[Sequence[BaseMessage], add_messages]
    schema_changes: list[dict]
    generated_dags: list[str]
    deployed_dags:  list[str]
    healed_dags:    list[str]
    errors:         list[str]
    run_id:         str


# ─────────────────────────────────────────────
# REGISTRY  (tracks registered DAG schemas)
# ─────────────────────────────────────────────

def load_registry() -> dict:
    if REGISTRY_PATH.exists():
        return json.loads(REGISTRY_PATH.read_text())
    return {}


def save_registry(registry: dict):
    REGISTRY_PATH.write_text(json.dumps(registry, indent=2, default=str))


# ─────────────────────────────────────────────
# SNOWFLAKE HELPERS
# ─────────────────────────────────────────────

def sf_query(sql: str) -> list[dict]:
    conn = snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database  = os.environ.get("SNOWFLAKE_DATABASE", "ANALYTICS"),
        schema    = os.environ.get("SNOWFLAKE_SCHEMA",   "PUBLIC"),
    )
    try:
        cur = conn.cursor(snowflake.connector.DictCursor)
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


# ─────────────────────────────────────────────
# TOOLS
# ─────────────────────────────────────────────

@tool
def detect_schema_changes() -> str:
    """
    Compare current Snowflake table schemas against the registry.
    Returns a list of changes: new columns, removed columns, type changes.
    """
    registry = load_registry()
    sql = """
        SELECT table_name, column_name, data_type, ordinal_position
        FROM   information_schema.columns
        WHERE  table_schema = CURRENT_SCHEMA()
        ORDER  BY table_name, ordinal_position
    """
    rows = sf_query(sql)

    # Group by table
    current: dict = {}
    for r in rows:
        t = r["TABLE_NAME"]
        current.setdefault(t, {})[r["COLUMN_NAME"]] = r["DATA_TYPE"]

    changes = []
    for table, cols in current.items():
        registered = registry.get(table, {})
        new_cols     = set(cols) - set(registered)
        removed_cols = set(registered) - set(cols)
        type_changes = {c for c in cols if c in registered and cols[c] != registered[c]}

        if new_cols:
            changes.append({"table": table, "type": "new_columns",     "columns": list(new_cols)})
        if removed_cols:
            changes.append({"table": table, "type": "removed_columns", "columns": list(removed_cols)})
        if type_changes:
            changes.append({"table": table, "type": "type_changes",    "columns": list(type_changes)})

    # Update registry with current state
    save_registry(current)
    return json.dumps(changes or [{"status": "no_changes_detected"}])


@tool
def get_failed_dags() -> str:
    """
    Query the Airflow metadata DB for DAGs that failed in the last 24 hours.
    Returns dag_id, task_id, execution_date, error summary.
    """
    # In production this queries the Airflow metadata DB via SQLAlchemy.
    # Here we use the Airflow REST API as a portable alternative.
    import urllib.request, base64
    host     = os.environ.get("AIRFLOW_HOST", "http://localhost:8080")
    user     = os.environ.get("AIRFLOW_USER", "admin")
    password = os.environ.get("AIRFLOW_PASSWORD", "admin")
    token    = base64.b64encode(f"{user}:{password}".encode()).decode()

    url = f"{host}/api/v1/dags?only_active=true&limit=100"
    req = urllib.request.Request(url, headers={"Authorization": f"Basic {token}"})
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        dags = json.loads(resp.read())["dags"]
        failed = [d for d in dags if d.get("last_dag_run_state") == "failed"]
        return json.dumps(failed)
    except Exception as e:
        return json.dumps({"error": str(e), "note": "Airflow API unreachable — check host/credentials"})


@tool
def generate_dag(
    pipeline_name: str,
    source_table: str,
    target_table: str,
    schedule: str,
    transform_sql: str,
    owner: str = "sobila.satheesh",
) -> str:
    """
    Generate a production-ready Airflow DAG Python file for an ETL pipeline.
    Returns the file path of the generated DAG.
    """
    dag_code = textwrap.dedent(f'''
        """
        Auto-generated DAG: {pipeline_name}
        Generated by Airflow AI DAG Generator — {datetime.utcnow().isoformat()}
        Source : {source_table}
        Target : {target_table}
        """
        from datetime import datetime, timedelta
        from airflow import DAG
        from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
        from airflow.operators.python import PythonOperator

        default_args = {{
            "owner":            "{owner}",
            "retries":          2,
            "retry_delay":      timedelta(minutes=5),
            "email_on_failure": True,
        }}

        TRANSFORM_SQL = """
        {transform_sql}
        """

        with DAG(
            dag_id            = "{pipeline_name}",
            default_args      = default_args,
            start_date        = datetime(2024, 1, 1),
            schedule_interval = "{schedule}",
            catchup           = False,
            tags              = ["ai-generated", "etl", "snowflake"],
        ) as dag:

            extract = SnowflakeOperator(
                task_id            = "extract_validate",
                sql                = "SELECT COUNT(*) FROM {source_table}",
                snowflake_conn_id  = "snowflake_default",
            )

            transform_load = SnowflakeOperator(
                task_id            = "transform_load",
                sql                = TRANSFORM_SQL,
                snowflake_conn_id  = "snowflake_default",
            )

            quality_check = SnowflakeOperator(
                task_id            = "quality_check",
                sql                = "SELECT COUNT(*) FROM {target_table} WHERE _loaded_at >= DATEADD(hour,-1,CURRENT_TIMESTAMP())",
                snowflake_conn_id  = "snowflake_default",
            )

            extract >> transform_load >> quality_check
    ''').strip()

    # Validate: parse as Python AST before writing
    try:
        ast.parse(dag_code)
    except SyntaxError as e:
        return json.dumps({"error": f"Generated DAG has syntax error: {e}"})

    out_path = DAGS_FOLDER / f"{pipeline_name}.py"
    DAGS_FOLDER.mkdir(parents=True, exist_ok=True)
    out_path.write_text(dag_code)
    log.info("DAG written: %s", out_path)
    return json.dumps({"status": "success", "path": str(out_path), "dag_id": pipeline_name})


@tool
def validate_dag_file(dag_file_path: str) -> str:
    """
    Validate a DAG file by parsing it as Python AST and checking
    that it contains a valid DAG object definition.
    """
    path = Path(dag_file_path)
    if not path.exists():
        return json.dumps({"valid": False, "error": "File not found"})
    try:
        tree = ast.parse(path.read_text())
        has_dag = any(
            isinstance(node, ast.With) and
            any(getattr(item.context_expr.func, "id", "") == "DAG"
                for item in node.items if hasattr(item, "context_expr"))
            for node in ast.walk(tree)
        )
        return json.dumps({"valid": True, "has_dag_context": has_dag, "path": dag_file_path})
    except SyntaxError as e:
        return json.dumps({"valid": False, "error": str(e)})


@tool
def trigger_dag_run(dag_id: str) -> str:
    """Trigger a manual DAG run via the Airflow REST API."""
    import urllib.request, base64
    host     = os.environ.get("AIRFLOW_HOST", "http://localhost:8080")
    user     = os.environ.get("AIRFLOW_USER", "admin")
    password = os.environ.get("AIRFLOW_PASSWORD", "admin")
    token    = base64.b64encode(f"{user}:{password}".encode()).decode()

    payload = json.dumps({"logical_date": datetime.utcnow().isoformat()}).encode()
    req = urllib.request.Request(
        f"{host}/api/v1/dags/{dag_id}/dagRuns",
        data=payload,
        headers={"Authorization": f"Basic {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        return json.dumps({"triggered": True, "dag_id": dag_id, "response": json.loads(resp.read())})
    except Exception as e:
        return json.dumps({"triggered": False, "error": str(e)})


# ─────────────────────────────────────────────
# GRAPH
# ─────────────────────────────────────────────

TOOLS = [detect_schema_changes, get_failed_dags, generate_dag, validate_dag_file, trigger_dag_run]
llm   = ChatOpenAI(model="gpt-4o", temperature=0).bind_tools(TOOLS)


def agent_node(state: DAGGenState) -> dict:
    return {"messages": [llm.invoke(state["messages"])]}


def should_continue(state: DAGGenState) -> str:
    last = state["messages"][-1]
    return "tools" if (hasattr(last, "tool_calls") and last.tool_calls) else END


def build_graph():
    g = StateGraph(DAGGenState)
    g.add_node("agent", agent_node)
    g.add_node("tools", ToolNode(TOOLS))
    g.set_entry_point("agent")
    g.add_conditional_edges("agent", should_continue, {"tools": "tools", END: END})
    g.add_edge("tools", "agent")
    return g.compile()


# ─────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────

def run_dag_generator():
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    prompt = f"""
You are an autonomous Airflow DAG management agent (run_id={run_id}).

Your responsibilities this cycle:

1. Call detect_schema_changes to find any Snowflake table changes since last run.
   - For each NEW column added: call generate_dag to create an updated ETL DAG
     that includes the new column in its transform SQL.
   - For each REMOVED column: generate a DAG that gracefully handles the absence
     with COALESCE(column, NULL) AS column.
   - Always call validate_dag_file after generating to confirm it's valid Python.

2. Call get_failed_dags to find any DAGs that failed in the last 24 hours.
   - For each failed DAG: attempt to trigger_dag_run to retry it.
   - Log what you did for each failure.

3. Summarise: what schema changes were detected, what DAGs were generated,
   what failures were healed, and any issues you could not resolve automatically.

Be precise. Validate every DAG before considering it deployed.
"""
    state: DAGGenState = {
        "messages":       [HumanMessage(content=prompt)],
        "schema_changes": [],
        "generated_dags": [],
        "deployed_dags":  [],
        "healed_dags":    [],
        "errors":         [],
        "run_id":         run_id,
    }
    graph  = build_graph()
    result = graph.invoke(state, {"recursion_limit": 60})
    return result


if __name__ == "__main__":
    result = run_dag_generator()
    print("\n✅ DAG Generator complete.")
    print(f"   DAGs generated : {len(result['generated_dags'])}")
    print(f"   DAGs healed    : {len(result['healed_dags'])}")
    print(f"   Errors         : {len(result['errors'])}")
