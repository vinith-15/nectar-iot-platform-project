from neo4j import GraphDatabase
import random

uri = "bolt://localhost:7687"
user = "neo4j"
password = "neo4j123"

driver = GraphDatabase.driver(uri, auth=(user, password))

def create_hierarchy(tx):
    tx.run("MATCH (n) DETACH DELETE n")
    sites = ['site-001','site-002','site-003']
    for site in sites:
        tx.run("CREATE (s:Site {id: $id, name: $name})", id=site, name=f"Site {site}")
    buildings = {
        'site-001': ['bldg-1','bldg-2'],
        'site-002': ['bldg-3'],
        'site-003': ['bldg-4','bldg-5','bldg-6']
    }
    assets = {
        'bldg-1': ['asset-101','asset-102'],
        'bldg-2': ['asset-201','asset-202','asset-203'],
        'bldg-3': ['asset-301'],
        'bldg-4': ['asset-401','asset-402'],
        'bldg-5': ['asset-501'],
        'bldg-6': ['asset-601','asset-602','asset-603']
    }
    for site, bldgs in buildings.items():
        for bldg in bldgs:
            tx.run("MATCH (s:Site {id: $site_id}) CREATE (b:Building {id: $bldg_id, name: $bldg_name}) "
                   "CREATE (s)-[:CONTAINS]->(b)", site_id=site, bldg_id=bldg, bldg_name=f"Building {bldg}")
            for asset in assets.get(bldg, []):
                asset_type = random.choice(['AHU','Chiller','Pump','Sensor'])
                tx.run("MATCH (b:Building {id: $bldg_id}) CREATE (a:Asset {id: $asset_id, type: $asset_type, name: $asset_name}) "
                       "CREATE (b)-[:CONTAINS]->(a)",
                       bldg_id=bldg, asset_id=asset, asset_type=asset_type, asset_name=f"Asset {asset}")

if __name__ == "__main__":
    with driver.session() as session:
        session.execute_write(create_hierarchy)
        print("Neo4j hierarchy created.")
