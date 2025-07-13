import re
import logging
from typing import Union, Optional
from pyrogram import raw, utils
from pyrogram.errors import PeerIdInvalid
from pyrogram import Client as PyroClient

log = logging.getLogger(__name__)

class PeerResolver:
    """Complete peer resolution system with all fixes"""

    async def resolve(
        self,
        client: PyroClient,
        peer_id: Union[int, str, None],
        *,
        use_cache: bool = True
    ) -> raw.base.InputPeer:
        """
        Resolve any peer identifier to InputPeer
        
        Args:
            client: Pyrogram client instance
            peer_id: Can be user_id, username, phone number, or link
            use_cache: Whether to check storage first (default: True)
            
        Returns:
            InputPeer object
            
        Raises:
            PeerIdInvalid: When peer cannot be resolved
            ConnectionError: When client is disconnected
        """
        # Validate client connection
        if not client.is_connected:
            raise ConnectionError("Client not connected")

        # Handle special cases
        if peer_id in (None, "self", "me"):
            return raw.types.InputPeerSelf()

        # Cache lookup
        if use_cache:
            try:
                if isinstance(peer_id, str):
                    if peer_id.startswith("+"):
                        peer = await client.storage.get_peer_by_phone_number(peer_id)
                    else:
                        peer = await client.storage.get_peer_by_username(peer_id)
                    return utils.get_input_peer(peer)
                return await client.storage.get_peer_by_id(peer_id)
            except Exception as e:
                log.debug(f"Cache lookup failed: {e}")

        # Process string inputs
        if isinstance(peer_id, str):
            peer_id = self._normalize_peer_string(peer_id)
            if isinstance(peer_id, str):
                return await self._resolve_username(client, peer_id)

        # ID-based resolution
        peer_type = utils.get_peer_type(peer_id)
        try:
            if peer_type == "user":
                return await self._resolve_user(client, peer_id)
            elif peer_type == "chat":
                return await self._resolve_chat(client, peer_id)
            else:
                return await self._resolve_channel(client, peer_id)
        except Exception as e:
            log.error(f"Failed to resolve peer {peer_id}: {e}")
            raise PeerIdInvalid(f"Could not resolve peer: {peer_id}")

    def _normalize_peer_string(self, peer_id: str) -> Union[int, str]:
        """
        Normalize various string formats:
        - Links (t.me/username)
        - Usernames (@username)
        - Phone numbers (+123456789)
        """
        # Handle Telegram links
        if match := re.match(
            r"(?:https?://)?(?:t\.me/|telegram\.(?:org|me|dog)/)(?:c/)?([\w]+)",
            peer_id.lower()
        ):
            try:
                return utils.get_channel_id(int(match.group(1)))
            except ValueError:
                return match.group(1)
        
        # Clean other strings
        return re.sub(r"[@+\s]", "", peer_id.lower())

    async def _resolve_username(
        self,
        client: PyroClient,
        username: str
    ) -> raw.base.InputPeer:
        """Resolve through username API"""
        result = await client.invoke(
            raw.functions.contacts.ResolveUsername(username=username)
        )
        return utils.get_input_peer(result.peer)

    async def _resolve_user(
        self,
        client: PyroClient,
        user_id: int
    ) -> raw.base.InputPeer:
        """Resolve user by ID"""
        users = await client.invoke(
            raw.functions.users.GetUsers(
                id=[raw.types.InputUser(user_id=user_id, access_hash=0)]
            )
        )
        return utils.get_input_peer(users[0])

    async def _resolve_chat(
        self,
        client: PyroClient,
        chat_id: int
    ) -> raw.base.InputPeer:
        """Resolve chat by ID"""
        chats = await client.invoke(
            raw.functions.messages.GetChats(id=[-chat_id])
        )
        return utils.get_input_peer(chats.chats[0])

    async def _resolve_channel(
        self,
        client: PyroClient,
        channel_id: int
    ) -> raw.base.InputPeer:
        """Resolve channel by ID"""
        channels = await client.invoke(
            raw.functions.channels.GetChannels(
                id=[raw.types.InputChannel(
                    channel_id=utils.get_channel_id(channel_id),
                    access_hash=0
                )]
            )
        )
        return utils.get_input_peer(channels.chats[0])
