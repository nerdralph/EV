import requests
import sqlite3
import json
import time
from datetime import datetime

# Configuration
API_BASE_URL = "https://api-v1-blue.chargelab.io/core/v1/chargers"
CHARGERS = [
    "NSP-BRI-01",
    "NSP-BRI-02",
    "NSP-MAS-01",
    "NSP-MAS-02",
    "NSP-MEM-01",
    "NSP-MEM-02",
    "NSP-MIL-01",
    "NSP-MIL-02",
    "NSP-PIC-01",
    "NSP-PIC-02",
    "NSP-WHY-01",
    "NSP-WHY-02",
    "NSP-WIN-01",
    "NSP-WIN-02"
]
DB_NAME = "chargelab_data.db"
QUERY_INTERVAL = 117  # seconds

def init_database():
    """Initialize the SQLite database and create table if it doesn't exist."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS charger_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            charger_name TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            port_id TEXT NOT NULL,
            port_status TEXT NOT NULL
        )
    ''')
    # Create an index on charger_name for faster lookups
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_charger_name
        ON charger_data(charger_name)
    ''')
    conn.commit()
    conn.close()
    print(f"Database initialized: {DB_NAME}")

def query_api(charger_name):
    """Query the Chargelab API for a specific charger and return the response."""
    url = f"{API_BASE_URL}?filter_eq[name]={charger_name}"
    try:
        response = requests.get(url, timeout=30)
        return {
            'status_code': response.status_code,
            'data': response.json() if response.status_code == 200 else response.text
        }
    except requests.exceptions.RequestException as e:
        print(f"Error querying API for {charger_name}: {e}")
        return {
            'status_code': 0,
            'data': {'error': str(e)}
        }

def extract_port_data(response_data):
    """Extract port ID and status from API response."""
    port_data = []
    try:
        # Navigate through the API response structure
        if isinstance(response_data, dict) and 'entities' in response_data:
            entities = response_data['entities']
            if isinstance(entities, list):
                for entity in entities:
                    if 'ports' in entity and isinstance(entity['ports'], list):
                        for port in entity['ports']:
                            port_id = port.get('portId', 'unknown')
                            port_status = port.get('status', 'unknown')
                            port_data.append((port_id, port_status))
    except Exception as e:
        print(f"Error extracting port data: {e}")
    
    return port_data if port_data else [('unknown', 'error')]

def get_last_port_state(charger_name, port_id):
    """Get the last stored status for a specific port from the database."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT port_status FROM charger_data
        WHERE charger_name = ? AND port_id = ?
        ORDER BY id DESC LIMIT 1
    ''', (charger_name, port_id))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def store_data(charger_name, timestamp, response_data):
    """Store the port data in the database only if the status has changed for that specific port."""
    port_data = extract_port_data(response_data)
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    stored_any = False
    
    for port_id, port_status in port_data:
        last_status = get_last_port_state(charger_name, port_id)
        
        # If this is the first entry for this port or the status has changed, store it
        if last_status is None or last_status != port_status:
            cursor.execute('''
                INSERT INTO charger_data (charger_name, timestamp, port_id, port_status)
                VALUES (?, ?, ?, ?)
            ''', (charger_name, timestamp, port_id, port_status))
            stored_any = True
    
    conn.commit()
    conn.close()
    return stored_any

def main():
    """Main loop to query API and store data every 3 minutes."""
    print("Starting Chargelab API Monitor...")
    print(f"Monitoring {len(CHARGERS)} chargers:")
    for charger in CHARGERS:
        print(f"  - {charger}")
    print(f"Interval: {QUERY_INTERVAL} seconds (2 minutes)")
    print("Press Ctrl+C to stop\n")
    
    init_database()
    
    try:
        while True:
            timestamp = datetime.now().isoformat()
            
            for charger_name in CHARGERS:
                result = query_api(charger_name)
                was_stored = store_data(charger_name, timestamp, result['data'])
                if was_stored:
                    print(f"[{timestamp}] {charger_name}: Status changed - Stored")
            
            time.sleep(QUERY_INTERVAL)
    
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
