import re
import logging
import sqlite3
from typing import Union, Optional
from pyrogram import raw, utils
from pyrogram.errors import PeerIdInvalid
from pyrogram import Client as PyroClient

log = logging.getLogger(__name__)

class PeerResolver:
    """Enhanced peer resolution with SQLite support"""
    
    @staticmethod
    async def resolve(
        client: PyroClient,
        peer_id: Union[int, str, None],
        *,
        use_cache: bool = True
    ) -> Union[raw.base.InputPeer, raw.base.InputUser, raw.base.InputChannel]:
        if not client.is_connected:
            raise ConnectionError("Client not connected")

        if peer_id in (None, "self", "me"):
            return raw.types.InputPeerSelf()

        if use_cache:
            try:
                if isinstance(peer_id, str):
                    return await PeerResolver._resolve_cached_username(client, peer_id)
                return await client.storage.get_peer_by_id(peer_id)
            except (KeyError, AttributeError, sqlite3.OperationalError) as e:
                log.debug(f"Cache miss: {e}")

        if isinstance(peer_id, str):
            peer_id = PeerResolver._parse_input_string(peer_id)
            if isinstance(peer_id, str):
                return await PeerResolver._resolve_username_api(client, peer_id)

        return await PeerResolver._resolve_by_id(client, peer_id)

    @staticmethod
    async def _resolve_cached_username(client: PyroClient, username: str):
        """Handle username resolution with SQLite fallback"""
        try:
            return await client.storage.get_peer_by_username(username)
        except AttributeError:
            cursor = await client.storage.conn.execute(
                "SELECT id, access_hash, type FROM peers WHERE username = ?",
                (username.lower(),)
            )
            if row := await cursor.fetchone():
                return utils.get_input_peer(utils.parse_peer_row(row))
            raise KeyError(f"Username {username} not in cache")

    @staticmethod
    def _parse_input_string(peer_id: str) -> Union[int, str]:
        """Parse various input formats"""
        if match := re.match(
            r"(?:https?://)?(?:t\.me/|telegram\.(?:org|me|dog)/)(?:c/)?([\w]+)",
            peer_id.lower()
        ):
            try:
                return utils.get_channel_id(int(match.group(1)))
            except ValueError:
                return match.group(1)
        return re.sub(r"[@+\s]", "", peer_id.lower())

    @staticmethod
    async def _resolve_username_api(client: PyroClient, username: str):
        """Resolve through Telegram API"""
        result = await client.invoke(
            raw.functions.contacts.ResolveUsername(username=username)
        )
        peer = getattr(result, 'peer', None)
        
        if isinstance(peer, raw.types.PeerUser):
            return raw.types.InputPeerUser(
                user_id=peer.user_id,
                access_hash=0
            )
        elif isinstance(peer, raw.types.PeerChannel):
            return raw.types.InputPeerChannel(
                channel_id=utils.get_channel_id(peer.channel_id),
                access_hash=0
            )
        raise PeerIdInvalid("Invalid peer type in API response")

    @staticmethod
    async def _resolve_by_id(client: PyroClient, peer_id: int):
        """Resolve by ID through API"""
        peer_type = utils.get_peer_type(peer_id)
        
        if peer_type == "user":
            users = await client.invoke(
                raw.functions.users.GetUsers(
                    id=[raw.types.InputUser(user_id=peer_id, access_hash=0)]
                )
            )
            return utils.get_input_peer(users[0])
        
        elif peer_type == "chat":
            chats = await client.invoke(
                raw.functions.messages.GetChats(id=[-peer_id])
            )
            return utils.get_input_peer(chats.chats[0])
        
        else:  # channel
            channels = await client.invoke(
                raw.functions.channels.GetChannels(
                    id=[raw.types.InputChannel(
                        channel_id=utils.get_channel_id(peer_id),
                        access_hash=0
                    )]
                )
            )
            return utils.get_input_peer(channels.chats[0])
