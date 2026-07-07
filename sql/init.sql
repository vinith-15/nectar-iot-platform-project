CREATE DATABASE nectar;
\c nectar;

CREATE TABLE dim_site (site_id VARCHAR(50) PRIMARY KEY, site_name VARCHAR(100), region VARCHAR(50), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE dim_building (building_id VARCHAR(50) PRIMARY KEY, site_id VARCHAR(50) REFERENCES dim_site(site_id), building_name VARCHAR(100), floors INTEGER, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE dim_asset (asset_id VARCHAR(50) PRIMARY KEY, building_id VARCHAR(50) REFERENCES dim_building(building_id), asset_name VARCHAR(100), asset_type VARCHAR(50), manufacturer VARCHAR(50), installation_date DATE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE dim_time (time_id SERIAL PRIMARY KEY, date DATE NOT NULL, hour INTEGER NOT NULL, minute INTEGER, UNIQUE(date, hour, minute));
CREATE TABLE fact_telemetry (telemetry_id BIGSERIAL PRIMARY KEY, time_id INTEGER REFERENCES dim_time(time_id), site_id VARCHAR(50) REFERENCES dim_site(site_id), building_id VARCHAR(50) REFERENCES dim_building(building_id), asset_id VARCHAR(50) REFERENCES dim_asset(asset_id), temperature DECIMAL(5,2), humidity DECIMAL(5,2), pressure DECIMAL(5,2), vibration DECIMAL(5,2), power_consumption DECIMAL(10,2), operating_mode VARCHAR(20), ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE fact_energy (energy_id BIGSERIAL PRIMARY KEY, time_id INTEGER REFERENCES dim_time(time_id), site_id VARCHAR(50) REFERENCES dim_site(site_id), building_id VARCHAR(50) REFERENCES dim_building(building_id), asset_id VARCHAR(50) REFERENCES dim_asset(asset_id), energy_kwh DECIMAL(10,2), updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE fact_event (event_id VARCHAR(50) PRIMARY KEY, time_id INTEGER REFERENCES dim_time(time_id), asset_id VARCHAR(50) REFERENCES dim_asset(asset_id), event_type VARCHAR(50), severity VARCHAR(20), message TEXT, ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
CREATE INDEX idx_telemetry_time ON fact_telemetry(time_id);
CREATE INDEX idx_telemetry_asset ON fact_telemetry(asset_id);
CREATE INDEX idx_energy_time ON fact_energy(time_id);
CREATE INDEX idx_event_asset ON fact_event(asset_id);
