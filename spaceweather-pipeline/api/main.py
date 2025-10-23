from fastapi import FastAPI, HTTPException, Query
import os, duckdb

app = FastAPI(title="Spaceweather API")
DB = os.environ.get("DUCKDB_PATH", "/warehouse/spaceweather.duckdb")


def _connect(read_only: bool = True):
    return duckdb.connect(DB, read_only=read_only)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/datasets")
def datasets():
    try:
        with _connect() as con:
            rows = con.execute(
                """
                SELECT dataset_id, scope, array_agg(DISTINCT parameter) AS parameters
                FROM gold__hapi_timeseries
                GROUP BY dataset_id, scope
                ORDER BY dataset_id, scope
                """
            ).fetchall()
    except duckdb.Error:
        rows = []

    return {
        "healthcheck": True,
        "hapi": [
            {"dataset_id": r[0], "scope": r[1], "parameters": list(r[2] or [])}
            for r in rows
        ],
    }


@app.get("/sql")
def sql(q: str = "select now()"):
    with _connect(read_only=False) as con:
        return {"rows": con.execute(q).fetchall()}


@app.get("/messages")
def messages(limit: int = 50):
    try:
        with _connect() as con:
            q = f"""
                SELECT ts, source, api_status, api_detail, ingest_ts
                FROM gold__messages
                ORDER BY ingest_ts DESC
                LIMIT {int(limit)}
            """
            rows = con.execute(q).fetchall()
    except duckdb.Error as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "rows": [
            {
                "ts": r[0],
                "source": r[1],
                "api_status": r[2],
                "api_detail": r[3],
                "ingest_ts": str(r[4]),
            }
            for r in rows
        ]
    }


@app.get("/hapi/datasets")
def hapi_datasets():
    data = datasets()["hapi"]
    return {"datasets": data}


@app.get("/hapi/data")
def hapi_data(
    dataset_id: str = Query(..., description="HAPI dataset identifier"),
    parameter: str | None = Query(None, description="Parameter to filter on"),
    limit: int = Query(500, ge=1, le=5000),
):
    sql = """
        SELECT dataset_id, scope, ts, ingest_ts, parameter, value_num, value_raw
        FROM gold__hapi_timeseries
        WHERE dataset_id = ?
        {parameter_clause}
        ORDER BY ts DESC
        LIMIT ?
    """
    parameter_clause = ""
    args = [dataset_id]
    if parameter:
        parameter_clause = "AND parameter = ?"
        args.append(parameter)
    args.append(limit)
    query = sql.format(parameter_clause=parameter_clause)

    try:
        with _connect() as con:
            rows = con.execute(query, args).fetchall()
    except duckdb.Error as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "rows": [
            {
                "dataset_id": r[0],
                "scope": r[1],
                "ts": r[2],
                "ingest_ts": r[3],
                "parameter": r[4],
                "value": r[5],
                "value_raw": r[6],
            }
            for r in rows
        ]
    }
