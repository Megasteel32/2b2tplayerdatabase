import requests
import sqlite3
import json

def fetch_data(url):
    print(f"Fetching data from {url}...")
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print(f"Successfully fetched data for {len(data)} players.")
        return data
    else:
        raise Exception(f"Failed to fetch data: HTTP {response.status_code}")

def update_database(data, db_path):
    print(f"Connecting to database: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("Ensuring 'players' table exists...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS players (
        username TEXT PRIMARY KEY,
        id INTEGER,
        uuid TEXT,
        kills INTEGER,
        deaths INTEGER,
        joins INTEGER,
        leaves INTEGER,
        adminlevel INTEGER,
        lastseen TEXT
    )
    ''')
    print("Table check complete.")

    print("Updating player data...")
    players_updated = 0
    players_inserted = 0

    for player in data:
        cursor.execute('SELECT * FROM players WHERE username = ?', (player['username'],))
        existing_player = cursor.fetchone()

        if existing_player:
            cursor.execute('''
            UPDATE players 
            SET id = ?, uuid = ?, kills = ?, deaths = ?, joins = ?, leaves = ?, adminlevel = ?
            WHERE username = ?
            ''', (
                player['id'],
                player['uuid'],
                player['kills'],
                player['deaths'],
                player['joins'],
                player['leaves'],
                player['adminlevel'],
                player['username']
            ))
            players_updated += 1
        else:
            cursor.execute('''
            INSERT INTO players 
            (username, id, uuid, kills, deaths, joins, leaves, adminlevel)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                player['username'],
                player['id'],
                player['uuid'],
                player['kills'],
                player['deaths'],
                player['joins'],
                player['leaves'],
                player['adminlevel']
            ))
            players_inserted += 1

    conn.commit()
    conn.close()

    print(f"Database update complete. {players_updated} players updated, {players_inserted} new players inserted.")

def main():
    url = "https://api.2b2t.dev/stats?username=all"
    db_path = "players.db"

    try:
        print("Starting database update process...")
        data = fetch_data(url)
        update_database(data, db_path)
        print("Database updated successfully.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()