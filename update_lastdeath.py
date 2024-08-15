import sqlite3
import requests
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from threading import Lock, Thread
from tqdm import tqdm

def fetch_last_death(username):
    url = f"https://api.2b2t.dev/stats?lastdeath={username}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data:
            return username, data[0]
    return username, None

def update_user(username, db_queue):
    death_data = fetch_last_death(username)
    db_queue.put(death_data)
    return death_data

def db_worker(db_queue, conn, cursor, progress_queue, total_users):
    users_updated = 0
    while True:
        item = db_queue.get()
        if item is None:
            break
        username, data = item
        if data:
            cursor.execute('''
            INSERT OR REPLACE INTO lastdeath (username, date, time, message)
            VALUES (?, ?, ?, ?)
            ''', (username, data['date'], data['time'], data['message']))
        else:
            cursor.execute('''
            INSERT OR REPLACE INTO lastdeath (username, date, time, message)
            VALUES (?, '0', '0', '0')
            ''', (username,))
        conn.commit()
        users_updated += 1
        progress_queue.put(1)
    conn.close()

def progress_reporter(progress_queue, total_users):
    start_time = time.time()
    processed = 0
    with tqdm(total=total_users, unit="user") as pbar:
        while processed < total_users:
            count = progress_queue.get()
            if count is None:
                break
            processed += count
            pbar.update(count)
            elapsed_time = time.time() - start_time
            items_per_second = processed / elapsed_time
            estimated_time = (total_users - processed) / items_per_second if items_per_second > 0 else 0
            pbar.set_postfix({"Users/s": f"{items_per_second:.2f}", "ETA": f"{estimated_time:.2f}s"})

def get_optimal_worker_count():
    return min(32, (os.cpu_count() or 1) + 4)  # Heuristic: CPU count + 4, max 32

def load_usernames(cursor):
    cursor.execute('SELECT username FROM lastdeath')
    return [row[0] for row in cursor.fetchall()]

def update_lastdeath_db():
    conn = sqlite3.connect('lastdeath.db', check_same_thread=False)
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS lastdeath (
        username TEXT PRIMARY KEY,
        date TEXT,
        time TEXT,
        message TEXT
    )
    ''')
    conn.commit()

    # Load all usernames
    usernames = load_usernames(cursor)

    total_users = len(usernames)
    print(f"Total users to update: {total_users}")

    if total_users == 0:
        print("No users in the database. Exiting.")
        return

    db_queue = Queue()
    progress_queue = Queue()

    # Start the database worker thread
    db_thread = Thread(target=db_worker, args=(db_queue, conn, cursor, progress_queue, total_users))
    db_thread.start()

    # Start the progress reporter thread
    progress_thread = Thread(target=progress_reporter, args=(progress_queue, total_users))
    progress_thread.start()

    # Determine optimal worker count
    max_workers = get_optimal_worker_count()
    print(f"Using {max_workers} worker threads")

    # Process users in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(update_user, username, db_queue) for username in usernames]
        for future in as_completed(futures):
            future.result()

    # Signal the database worker and progress reporter to finish
    db_queue.put(None)
    progress_queue.put(None)
    db_thread.join()
    progress_thread.join()

    print("Database update completed.")

if __name__ == "__main__":
    update_lastdeath_db()