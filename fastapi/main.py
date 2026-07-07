from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool

pool = SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    host="postgres",
    database="nectar",
    user="admin",
    password="admin123"
)

app = FastAPI(title="Nectar IoT API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/")
def root():
    return {"message": "Nectar IoT Platform API"}

@app.get("/sites/{site_id}/energy")
def site_energy(site_id: str):
    conn = pool.getconn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT t.date, SUM(f.energy_kwh) as total_energy
            FROM fact_energy f
            JOIN dim_time t ON f.time_id = t.time_id
            WHERE f.site_id = %s
            GROUP BY t.date
            ORDER BY t.date DESC
        """, (site_id,))
        results = cur.fetchall()
        return results
    finally:
        pool.putconn(conn)

@app.get("/assets/{asset_id}/health")
def asset_health(asset_id: str):
    conn = pool.getconn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT temperature, humidity, vibration, power_consumption, operating_mode, ingestion_timestamp
            FROM fact_telemetry
            WHERE asset_id = %s
            ORDER BY ingestion_timestamp DESC
            LIMIT 1
        """, (asset_id,))
        result = cur.fetchone()
        return result
    finally:
        pool.putconn(conn)

@app.get("/sites/{site_id}/assets")
def site_assets(site_id: str):
    conn = pool.getconn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT a.asset_id, a.asset_name, a.asset_type, b.building_id
            FROM dim_asset a
            JOIN dim_building b ON a.building_id = b.building_id
            WHERE b.site_id = %s
        """, (site_id,))
        results = cur.fetchall()
        return results
    finally:
        pool.putconn(conn)

@app.get("/metrics")
def metrics():
    return {"status": "healthy"}
