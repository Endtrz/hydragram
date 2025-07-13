import sqlite3
import logging
from typing import Optional, Dict, Any
from pyrogram.storage import Storage
from pyrogram import utils

log = logging.getLogger(__name__)

class HydraStorage(Storage):
    """Enhanced SQLite storage with username support for Hydragram"""
    
    async def init(self):
        await super().init()
        await self._migrate_database()

    async def _migrate_database(self):
        """Ensure database schema has all required columns"""
        try:
            # Check if peers table exists
            cursor = await self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='peers'"
            )
            if not await cursor.fetchone():
                await self._create_new_schema()
                return

            # Add missing columns if needed
            await self._add_missing_columns()
        except sqlite3.Error as e:
            log.error(f"Database migration failed: {e}")
            raise

    async def _create_new_schema(self):
        """Create fresh schema with all columns"""
        await self.conn.execute("""
        CREATE TABLE peers (
            id INTEGER PRIMARY KEY,
            access_hash INTEGER,
            type TEXT,
            username TEXT,
            phone_number TEXT,
            last_update_on INTEGER
        )
        """)
        await self.conn.commit()

    async def _add_missing_columns(self):
        """Add missing columns to existing schema"""
        columns_to_add = [
            ("username", "TEXT"),
            ("phone_number", "TEXT")
        ]
        
        for column, ctype in columns_to_add:
            try:
                await self.conn.execute(
                    f"ALTER TABLE peers ADD COLUMN {column} {ctype}"
                )
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e):
                    raise
        await self.conn.commit()

    async def update_peer(
        self,
        id: int,
        access_hash: int,
        type: str,
        username: Optional[str] = None,
        phone_number: Optional[str] = None
    ):
        """Enhanced peer update with all fields"""
        await self.conn.execute(
            """
            INSERT OR REPLACE INTO peers
            (id, access_hash, type, username, phone_number, last_update_on)
            VALUES (?, ?, ?, ?, ?, strftime('%s'))
            """,
            (id, access_hash, type, username, phone_number)
        )
        await self.conn.commit()

    async def get_peer_by_username(self, username: str) -> Dict[str, Any]:
        """Lookup peer by username (case-insensitive)"""
        cursor = await self.conn.execute(
            "SELECT id, access_hash, type FROM peers WHERE LOWER(username) = ?",
            (username.lower(),)
        )
        if row := await cursor.fetchone():
            return {
                "id": row[0],
                "access_hash": row[1],
                "type": row[2]
            }
        raise KeyError(f"Username {username} not found")

    async def get_peer_by_phone_number(self, phone_number: str) -> Dict[str, Any]:
        """Lookup peer by phone number"""
        cursor = await self.conn.execute(
            "SELECT id, access_hash, type FROM peers WHERE phone_number = ?",
            (phone_number,)
        )
        if row := await cursor.fetchone():
            return {
                "id": row[0],
                "access_hash": row[1],
                "type": row[2]
            }
        raise KeyError(f"Phone number {phone_number} not found")
