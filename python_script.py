import os
import sys
import json
import time
import uuid
import socket
import logging
from datetime import datetime, timezone

import pyodbc

# =========================
# Configurações
# =========================

OUTPUT_FILE = os.environ.get(
    "MSSQL_DD_OUTPUT_FILE",
    "/var/log/datadog/mssql_custom/mssql_active_requests.log"
)

POLL_INTERVAL_SECONDS = int(os.environ.get("MSSQL_DD_POLL_INTERVAL", "60"))

DBS = [
    {
        "name": "riachu_fin",
        "server": "riachu_fin",
        "port": 1433,
        "username": "datadog",
        "password": "",
        "driver": "{ODBC Driver 18 for SQL Server}",
        "extra": "TrustServerCertificate=yes;"
    },
    {
        "name": "ecommercedb",
        "server": "ecommercedb",
        "port": 1433,
        "username": "datadog",
        "password": "",
        "driver": "{ODBC Driver 17 for SQL Server}",
        "extra": "Encrypt=no;TrustServerCertificate=yes;"
    }
]

QUERY = r"""
select
    ROW_NUMBER() OVER(ORDER BY req.wait_time desc) as rownum,
    req.session_id,
    req.status,
    req.blocking_session_id,
    st.text,
    req.command,
    substring(
        REPLACE(
            REPLACE(
                SUBSTRING(
                    st.text,
                    (req.statement_start_offset/2) + 1,
                    ((CASE req.statement_end_offset
                        WHEN -1 THEN DATALENGTH(st.text)
                        ELSE req.statement_end_offset
                      END - req.statement_start_offset)/2) + 1
                ),
                CHAR(10), ' '
            ),
            CHAR(13), ' '
        ),
        1,
        4000
    ) AS statement_text,
    case
        when req.transaction_isolation_level = 0 then 'Unspecified'
        when req.transaction_isolation_level = 1 then 'ReadUncomitted'
        when req.transaction_isolation_level = 2 then 'ReadCommitted'
        when req.transaction_isolation_level = 3 then 'Repeatable'
        when req.transaction_isolation_level = 4 then 'Serializable'
        when req.transaction_isolation_level = 5 then 'Snapshot'
    end as isolation,
    db_name(req.database_id) as databasename,
    ss.login_name,
    ss.login_time,
    req.start_time,
    req.wait_time,
    req.wait_type,
    req.last_wait_type,
    sc.scheduler_id,
    ss.host_name,
    cn.client_net_address,
    ss.program_name,
    req.logical_reads,
    req.reads,
    mgr.grant_time,
    mgr.granted_memory_kb,
    req.cpu_time as cpu_time_request,
    ss.cpu_time as cpu_time_cumulative,
    pl.query_plan
from sys.dm_exec_requests req
inner join sys.dm_exec_sessions ss
    on ss.session_id = req.session_id
left join sys.dm_exec_connections cn
    on cn.session_id = ss.session_id
left join sys.dm_exec_query_memory_grants mgr
    on mgr.session_id = req.session_id
left join sys.dm_os_schedulers sc
    on sc.scheduler_id = req.scheduler_id
outer apply sys.dm_exec_sql_text(req.sql_handle) st
outer apply sys.dm_exec_query_plan(req.plan_handle) pl
where ss.is_user_process = 1
  and req.session_id <> @@spid
  and ss.status = 'running'
order by req.logical_reads desc
"""

# Para evitar um largo aumento de tamanho do log.
MAX_TEXT_LEN = int(os.environ.get("MSSQL_DD_MAX_TEXT_LEN", "4000"))
MAX_QUERY_PLAN_LEN = int(os.environ.get("MSSQL_DD_MAX_QUERY_PLAN_LEN", "16000"))

# =========================
# Logging local do script
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# =========================
# Helpers
# =========================

def ensure_output_dir(path: str) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)


def safe_str(value):
    if value is None:
        return None
    try:
        return str(value)
    except Exception:
        return repr(value)


def truncate(value, limit):
    if value is None:
        return None
    s = safe_str(value)
    if s is None:
        return None
    if len(s) <= limit:
        return s
    return s[:limit] + "...[truncated]"


def isoformat_or_none(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc).isoformat()
        return value.isoformat()
    return safe_str(value)


def build_connection_string(db_cfg: dict) -> str:
    return (
        f"DRIVER={db_cfg['driver']};"
        f"SERVER={db_cfg['server']},{db_cfg['port']};"
        f"UID={db_cfg['username']};"
        f"PWD={db_cfg['password']};"
        f"{db_cfg['extra']}"
    )


def row_to_event(db_name: str, columns: list, row) -> dict:
    data = dict(zip(columns, row))

    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ddsource": "mssql_custom_query",
        "service": "mssql",
        "host": socket.gethostname(),
        "db_instance": db_name,
        "event_type": "sqlserver_active_request",
        "event_id": str(uuid.uuid4()),

        "rownum": data.get("rownum"),
        "session_id": data.get("session_id"),
        "status": safe_str(data.get("status")),
        "blocking_session_id": data.get("blocking_session_id"),
        "sql_text": truncate(data.get("text"), MAX_TEXT_LEN),
        "command": safe_str(data.get("command")),
        "statement_text": truncate(data.get("statement_text"), MAX_TEXT_LEN),
        "isolation": safe_str(data.get("isolation")),
        "databasename": safe_str(data.get("databasename")),
        "login_name": safe_str(data.get("login_name")),
        "login_time": isoformat_or_none(data.get("login_time")),
        "request_start_time": isoformat_or_none(data.get("start_time")),
        "wait_time_ms": data.get("wait_time"),
        "wait_type": safe_str(data.get("wait_type")),
        "last_wait_type": safe_str(data.get("last_wait_type")),
        "scheduler_id": data.get("scheduler_id"),
        "client_host_name": safe_str(data.get("host_name")),
        "client_net_address": safe_str(data.get("client_net_address")),
        "program_name": safe_str(data.get("program_name")),
        "logical_reads": data.get("logical_reads"),
        "reads": data.get("reads"),
        "grant_time": isoformat_or_none(data.get("grant_time")),
        "granted_memory_kb": data.get("granted_memory_kb"),
        "cpu_time_request_ms": data.get("cpu_time_request"),
        "cpu_time_cumulative_ms": data.get("cpu_time_cumulative"),
        "query_plan": truncate(data.get("query_plan"), MAX_QUERY_PLAN_LEN),
    }

    return event


def write_json_line(path: str, payload: dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")


def collect_once(output_file: str) -> int:
    total_rows = 0

    for db_cfg in DBS:
        conn = None
        db_name = db_cfg["name"]
        try:
            conn_str = build_connection_string(db_cfg)
            logging.info("Conectando em %s", db_name)
            conn = pyodbc.connect(conn_str, timeout=15)
            conn.timeout = 60

            cursor = conn.cursor()
            cursor.execute(QUERY)

            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

            if not rows:
                summary = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "ddsource": "mssql_custom_query",
                    "service": "mssql",
                    "host": socket.gethostname(),
                    "db_instance": db_name,
                    "event_type": "sqlserver_active_request_summary",
                    "result_count": 0,
                    "message": f"No running user requests found for {db_name}"
                }
                write_json_line(output_file, summary)
                logging.info("Sem linhas para %s", db_name)
                continue

            for row in rows:
                event = row_to_event(db_name, columns, row)
                write_json_line(output_file, event)
                total_rows += 1

            summary = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "ddsource": "mssql_custom_query",
                "service": "mssql",
                "host": socket.gethostname(),
                "db_instance": db_name,
                "event_type": "sqlserver_active_request_summary",
                "result_count": len(rows),
                "message": f"Collected {len(rows)} running user requests from {db_name}"
            }
            write_json_line(output_file, summary)

            logging.info("Coletadas %s linhas de %s", len(rows), db_name)

        except Exception as exc:
            error_event = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "ddsource": "mssql_custom_query",
                "service": "mssql",
                "host": socket.gethostname(),
                "db_instance": db_name,
                "event_type": "sqlserver_active_request_error",
                "error": safe_str(exc),
                "message": f"Failed collecting from {db_name}"
            }
            write_json_line(output_file, error_event)
            logging.exception("Erro ao coletar de %s", db_name)

        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    return total_rows


def main():
    ensure_output_dir(OUTPUT_FILE)

    run_mode = os.environ.get("MSSQL_DD_RUN_MODE", "once").strip().lower()

    if run_mode == "loop":
        logging.info("Modo loop ativo. Intervalo: %s segundos", POLL_INTERVAL_SECONDS)
        while True:
            collect_once(OUTPUT_FILE)
            time.sleep(POLL_INTERVAL_SECONDS)
    else:
        logging.info("Modo once ativo")
        collect_once(OUTPUT_FILE)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
