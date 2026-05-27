import sqlite3
import argparse
import time
from typing import Annotated
from pydantic import Field
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError


class SQLiteQueue:
    def __init__(self, db_path="message_queue.db"):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self):
        # Using isolation_level=None enables autocommit mode,
        # allowing us to manage transactions explicitly.
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        # WAL mode dramatically improves concurrent read/write performance
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init_db(self):
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT NOT NULL,
                    timestamp REAL NOT NULL
                )
            """)
            conn.commit()


    def push(self, message: str):
        """Adds a message to the end of the queue."""
        with self._get_connection() as conn:
            conn.execute(
                "INSERT INTO queue (payload, timestamp) VALUES (?, ?)",
                (message, time.time())
            )
            conn.commit()


    def push_many(self, message_list: list[str]):
        """Adds multiple messages to the queue."""
        messages = [(message, time.time()) for message in message_list]
        with self._get_connection() as conn:
            conn.executemany(
                "INSERT INTO queue (payload, timestamp) VALUES (?, ?)",
                messages
            )
            conn.commit()


    def pop(self):
        """
        Removes and returns the oldest message from the queue (FIFO).
        Returns None if the queue is empty.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # We use a transaction to ensure that in a multi-threaded/processed
            # environment, a message is never popped twice.
            cursor.execute("BEGIN EXCLUSIVE TRANSACTION;")

            # Fetch the oldest message
            cursor.execute("SELECT id, payload FROM queue ORDER BY id ASC LIMIT 1;")
            row = cursor.fetchone()

            if row:
                msg_id, payload = row
                # Delete it so no one else grabs it
                cursor.execute("DELETE FROM queue WHERE id = ?;", (msg_id,))
                conn.commit()
                return payload
            else:
                conn.commit()
                return None

        except sqlite3.Error as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def size(self):
        """Returns the current number of items in the queue."""
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM queue;")
            return cursor.fetchone()[0]


# 1. Setup Argument Parsing for the DB path
parser = argparse.ArgumentParser(description="FastMCP Task Server")
parser.add_argument("--db", default="tasks.db", help="Path to the SQLite database file")
parser.add_argument("--transport", default="http", help="Transport mode stdio|http")
parser.add_argument("--host", default="127.0.0.1", help="HTTP Transport mode bind interface (default localhost)")
parser.add_argument("--port", type=int, default=8000, help="HTTP Transport mode listen port (default 8000)")
parser.add_argument("--push", help="(CLI) push a message onto the queue: --push <message_content>")
parser.add_argument("--pop", action=argparse.BooleanOptionalAction, default=False, help="(CLI) pop a message from the queue")

args, unknown = parser.parse_known_args()

# 2. Initialize FastMCP and Database
mcp = FastMCP(name="Generic Item Datastore")
DB_PATH = args.db

msg_queue = SQLiteQueue(DB_PATH)

# 3. Define Tools

@mcp.tool()
def save_item(content: str) -> str:
    """Adds a new item to the datastore."""
    if not content.strip():
        return "Error: Item content cannot be empty."
    
    msg_queue.push(content)

    return f"Item created"


@mcp.tool()
def save_items(
    messages: Annotated[
        list[str], 
        Field(
            description="A list of string items to store in the datastore.",
            min_length=1,
            examples=[["The first item", "A second item"]]
        )
    ]) -> str:
    """
    Pushes multiple string items into the datastore at once.
    """
    # Your internal queue.executemany() logic here...
    msg_queue.push_many(messages)

    return f"Successfully stored {len(messages)} items."


@mcp.tool()
def get_number_of_items() -> str:
    """
    Get the number of items in the datastore.
    """
    num_messages = msg_queue.size()
            
    return f"{num_messages} items."


@mcp.tool()
def get_next_item() -> str:
    """Retrieves the next item from the datastore."""
    item = msg_queue.pop()    
    if item is not None:
        return f"{item}"
    raise ToolError("No items found in the datastore.")


# 4. Entry Point
if __name__ == "__main__":
    if args.push:
        c = args.push
        print(c)
        save_item(c)
    elif args.pop:
        item = get_next_item()
        print(item)
    else:
        mode = args.transport.lower().strip()
        if mode not in ['stdio']:
            mcp.run(transport="streamable-http", host=args.host, port=args.port)
        else:
            mcp.run(transport="stdio")
