import sqlite3
import os
import time

DB_FILE = 'my_wal_database.db'

def setup_database():
    """Connects to the database and ensures WAL mode is enabled."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # Enable WAL mode
        # This PRAGMA is persistent. You usually only need to run it once
        # per database file to convert it to WAL mode.
        cursor.execute("PRAGMA journal_mode=WAL;")

        # Verify the journal mode (optional, but good for checking)
        cursor.execute("PRAGMA journal_mode;")
        current_mode = cursor.fetchone()[0]
        print(f"Database '{DB_FILE}' journal mode: {current_mode.upper()}")

        if current_mode.upper() != 'WAL':
            print("Warning: Failed to set WAL mode. Check permissions or if a transaction is active.")

        # Create a table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        print("Database setup complete.")
        return conn
    except sqlite3.Error as e:
        print(f"Database error during setup: {e}")
        if conn:
            conn.close()
        return None

def write_data(conn, message):
    """Inserts data into the database."""
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO messages (content) VALUES (?)", (message,))
        conn.commit()
        print(f"Written: '{message}'")
    except sqlite3.Error as e:
        print(f"Write error: {e}")

def read_data(conn):
    """Reads data from the database."""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, content, timestamp FROM messages ORDER BY id DESC LIMIT 5")
        rows = cursor.fetchall()
        print("\nLatest messages:")
        if rows:
            for row in rows:
                print(f"  ID: {row[0]}, Content: '{row[1]}', Time: {row[2]}")
        else:
            print("  No messages yet.")
    except sqlite3.Error as e:
        print(f"Read error: {e}")

if __name__ == "__main__":
    # Clean up previous files for a fresh start
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    if os.path.exists(f"{DB_FILE}-wal"):
        os.remove(f"{DB_FILE}-wal")
    if os.path.exists(f"{DB_FILE}-shm"):
        os.remove(f"{DB_FILE}-shm")

    # 1. Enable WAL mode and set up the database
    db_connection = setup_database()

    if db_connection:
        # 2. Perform some writes
        for i in range(5):
            write_data(db_connection, f"Message {i+1}")
            time.sleep(0.1) # Simulate some work

        # 3. Perform some reads
        read_data(db_connection)

        # 4. Demonstrate concurrent access (simulated)
        # In a real multi-process/multi-thread scenario, you'd open
        # separate connections for each thread/process.
        # For simplicity, we'll just open another connection here to show it works
        # against the same WAL-enabled file.
        print("\n--- Simulating another connection accessing the database ---")
        another_connection = sqlite3.connect(DB_FILE)
        another_cursor = another_connection.cursor()

        # This connection will also be in WAL mode because the file is.
        another_cursor.execute("PRAGMA journal_mode;")
        print(f"Another connection's journal mode: {another_cursor.fetchone()[0].upper()}")

        # While we're writing with db_connection, another_connection can read
        print("\nAttempting concurrent read/write:")

        # Start a write in one connection
        write_data(db_connection, "Concurrent write 1")

        # Read from the other connection
        # It might still see the state before "Concurrent write 1" if checkpointing hasn't happened.
        read_data(another_connection)

        write_data(db_connection, "Concurrent write 2")
        read_data(another_connection)

        another_connection.close()
        print("--- End of concurrent simulation ---")

        # After some operations, you'll see the -wal and -shm files alongside your .db file.
        print(f"\nFiles in directory: {os.listdir(os.path.dirname(os.path.abspath(__file__)))}")

        # Close the main connection
        db_connection.close()

        # Manual checkpoint (optional):
        # When all connections to the database close, SQLite automatically attempts a checkpoint.
        # You can also manually trigger one if needed (e.g., to reduce WAL file size).
        print("\nPerforming a final checkpoint (if WAL files still exist):")
        try:
            temp_conn = sqlite3.connect(DB_FILE)
            temp_conn.execute("PRAGMA journal_mode=WAL;") # Re-establish WAL mode for the temp connection if it closed
            temp_conn.execute("PRAGMA wal_checkpoint(FULL);") # Force a full checkpoint
            temp_conn.close()
            print("Checkpoint performed.")
        except sqlite3.Error as e:
            print(f"Error during checkpoint: {e}")

    print("\nScript finished.")
    # You'll notice the -wal and -shm files might disappear after the last connection closes
    # and a checkpoint occurs. If they persist, it means a checkpoint hasn't fully cleared them.
    print(f"Files in directory after closing and potential checkpoint: {os.listdir(os.path.dirname(os.path.abspath(__file__)))}")
