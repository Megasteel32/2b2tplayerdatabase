import sqlite3
import requests
import json
import time
import os
import threading
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from threading import Lock, Thread, Event

from tqdm import tqdm
from datetime import datetime, timedelta

# Configuration
DAYS_BETWEEN_UPDATES = 7  # Adjust this value as needed
MAIN_DB_PATH = 'players.db'
LASTKILL_DB_PATH = 'lastkill.db'
LASTDEATH_DB_PATH = 'lastdeath.db'
FIRSTKILL_DB_PATH = 'firstkill.db'
FIRSTDEATH_DB_PATH = 'firstdeath.db'

def fetch_data(url):
    try:
        response = requests.get(url, timeout=60)
        if response.status_code == 200:
            return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
    return None

def get_optimal_worker_count():
    return os.cpu_count()

def update_main_database():
    conn = sqlite3.connect(MAIN_DB_PATH)
    cursor = conn.cursor()

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
        lastseen TEXT,
        lastupdated TEXT
    )
    ''')

    # Add lastupdated column if it doesn't exist
    cursor.execute("PRAGMA table_info(players)")
    columns = [column[1] for column in cursor.fetchall()]
    if 'lastupdated' not in columns:
        cursor.execute("ALTER TABLE players ADD COLUMN lastupdated TEXT")

    url = "https://api.2b2t.dev/stats?username=all"
    data = fetch_data(url)

    if data:
        for player in data:
            cursor.execute('''
            INSERT OR REPLACE INTO players 
            (username, id, uuid, kills, deaths, joins, leaves, adminlevel)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(username) DO UPDATE SET
            id = excluded.id,
            uuid = excluded.uuid,
            kills = excluded.kills,
            deaths = excluded.deaths,
            joins = excluded.joins,
            leaves = excluded.leaves,
            adminlevel = excluded.adminlevel
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

    conn.commit()
    conn.close()
    print("Main database updated successfully.")

def fetch_last_kill(username):
    url = f"https://api.2b2t.dev/stats?lastkill={username}"
    return fetch_data(url)

def fetch_last_death(username):
    url = f"https://api.2b2t.dev/stats?lastdeath={username}"
    return fetch_data(url)

def fetch_first_kill(username):
    url = f"https://api.2b2t.dev/stats?firstkill={username}"
    return fetch_data(url)

def fetch_first_death(username):
    url = f"https://api.2b2t.dev/stats?firstdeath={username}"
    return fetch_data(url)

def fetch_last_seen(username):
    url = f"https://api.2b2t.dev/seen?username={username}"
    data = fetch_data(url)
    if data and isinstance(data, list) and len(data) > 0:
        return data[0].get('seen')
    return None

def update_user_data(username, db_queue, update_queue):
    try:
        new_last_seen = fetch_last_seen(username)
        if new_last_seen:
            last_kill = fetch_last_kill(username)
            last_death = fetch_last_death(username)
            first_kill = fetch_first_kill(username)
            first_death = fetch_first_death(username)

            db_queue.put(('lastkill', username, last_kill[0] if last_kill else None))
            db_queue.put(('lastdeath', username, last_death[0] if last_death else None))
            db_queue.put(('firstkill', username, first_kill[0] if first_kill else None))
            db_queue.put(('firstdeath', username, first_death[0] if first_death else None))

            update_queue.put((username, new_last_seen))
            return True
        return False
    except Exception as e:
        print(f"Error updating user data for {username}: {e}")
        return False

def db_worker(db_queue, completion_event):
    connections = {
        'lastkill': sqlite3.connect(LASTKILL_DB_PATH),
        'lastdeath': sqlite3.connect(LASTDEATH_DB_PATH),
        'firstkill': sqlite3.connect(FIRSTKILL_DB_PATH),
        'firstdeath': sqlite3.connect(FIRSTDEATH_DB_PATH)
    }
    cursors = {db: conn.cursor() for db, conn in connections.items()}

    for db, cursor in cursors.items():
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {db} (
            username TEXT PRIMARY KEY,
            date TEXT,
            time TEXT,
            message TEXT
        )
        ''')

    while True:
        try:
            item = db_queue.get(timeout=1)
            if item is None:
                break

            db_name, username, data = item
            if data:
                cursors[db_name].execute(f'''
                INSERT OR REPLACE INTO {db_name} (username, date, time, message)
                VALUES (?, ?, ?, ?)
                ''', (username, data.get('date'), data.get('time'), data.get('message')))
            else:
                cursors[db_name].execute(f'''
                INSERT OR REPLACE INTO {db_name} (username, date, time, message)
                VALUES (?, NULL, NULL, NULL)
                ''', (username,))

            connections[db_name].commit()
        except queue.Empty:
            continue
        except Exception as e:
            print(f"Error in db_worker: {e}")

    for conn in connections.values():
        conn.close()
    completion_event.set()

def final_update_worker(update_queue, completion_event):
    conn = sqlite3.connect(MAIN_DB_PATH)
    cursor = conn.cursor()

    while True:
        try:
            item = update_queue.get(timeout=1)
            if item is None:
                break

            username, new_last_seen = item
            current_time = datetime.now().isoformat()
            cursor.execute('''
            UPDATE players 
            SET lastseen = ?, lastupdated = ?
            WHERE username = ?
            ''', (new_last_seen, current_time, username))
            conn.commit()
        except queue.Empty:
            continue
        except Exception as e:
            print(f"Error in final_update_worker: {e}")

    conn.close()
    completion_event.set()

def progress_reporter(progress_queue, total_users):
    start_time = time.time()
    processed = 0
    with tqdm(total=total_users, unit="user") as pbar:
        while processed < total_users:
            try:
                count = progress_queue.get(timeout=1)
                if count is None:
                    break
                processed += count
                pbar.update(count)
                elapsed_time = time.time() - start_time
                items_per_second = processed / elapsed_time
                estimated_time = (total_users - processed) / items_per_second if items_per_second > 0 else 0
                pbar.set_postfix({"Users/s": f"{items_per_second:.2f}", "ETA": f"{estimated_time:.2f}s"})
            except queue.Empty:
                continue

def get_users_to_update():
    conn = sqlite3.connect(MAIN_DB_PATH)
    cursor = conn.cursor()

    update_threshold = datetime.now() - timedelta(days=DAYS_BETWEEN_UPDATES)
    cursor.execute('''
    SELECT username FROM players
    WHERE lastupdated IS NULL OR datetime(lastupdated) < ?
    ''', (update_threshold.isoformat(),))

    users_to_update = [row[0] for row in cursor.fetchall()]
    conn.close()

    return users_to_update

def update_all_data():
    update_main_database()

    users_to_update = get_users_to_update()
    total_users = len(users_to_update)
    print(f"Users to update: {total_users}")

    if total_users == 0:
        print("No users need updating. Exiting.")
        return

    db_queue = Queue()
    update_queue = Queue()
    progress_queue = Queue()

    db_worker_completion = Event()
    final_update_completion = Event()

    # Start the database worker threads
    db_thread = Thread(target=db_worker, args=(db_queue, db_worker_completion))
    db_thread.start()

    # Start the final update worker thread
    final_update_thread = Thread(target=final_update_worker, args=(update_queue, final_update_completion))
    final_update_thread.start()

    # Start the progress reporter thread
    progress_thread = Thread(target=progress_reporter, args=(progress_queue, total_users))
    progress_thread.start()

    # Determine optimal worker count
    max_workers = get_optimal_worker_count()
    print(f"Using {max_workers} worker threads")

    # Process users in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(update_user_data, username, db_queue, update_queue) for username in users_to_update]
        for future in as_completed(futures):
            if future.result():
                progress_queue.put(1)

    # Signal the workers to finish
    db_queue.put(None)
    update_queue.put(None)
    progress_queue.put(None)

    # Wait for all workers to complete
    db_worker_completion.wait()
    final_update_completion.wait()

    db_thread.join()
    final_update_thread.join()
    progress_thread.join()

    print("Database update completed.")

if __name__ == "__main__":
    update_all_data()