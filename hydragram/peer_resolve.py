import re
import logging
from typing import Union, Optional
from pyrogram import raw, utils
from pyrogram.errors import PeerIdInvalid, ChannelInvalid, ChannelPrivate
from pyrogram import Client as PyroClient

log = logging.getLogger(__name__)

class PeerResolver:
    """Auto-resolving peer system with intelligent caching"""

    async def resolve(
        self,
        client: PyroClient,
        peer_id: Union[int, str, None],
        *,
        use_cache: bool = True
    ) -> Union[raw.base.InputPeer, raw.base.InputUser, raw.base.InputChannel]:
        """Smart resolver that handles all peer types automatically"""
        try:
            # Handle special cases
            if peer_id in (None, "self", "me"):
                return raw.types.InputPeerSelf()

            # Try cache first if enabled
            if use_cache:
                try:
                    cached = await self._get_cached_peer(client, peer_id)
                    if cached:
                        return cached
                except Exception as e:
                    log.debug(f"Cache lookup failed: {e}")

            # Auto-detect and resolve peer type
            return await self._resolve_and_cache(client, peer_id)
            
        except Exception as e:
            log.error(f"Failed to resolve {peer_id}: {e}")
            raise PeerIdInvalid(f"Could not resolve peer (server might be down): {peer_id}")

    async def _get_cached_peer(self, client: PyroClient, peer_id: Union[int, str]):
        """Smart cache lookup with automatic type detection"""
        if isinstance(peer_id, str):
            peer_id = self._normalize_peer_string(peer_id)
            if isinstance(peer_id, str):
                if peer_id.startswith("+"):
                    return await client.storage.get_peer_by_phone_number(peer_id)
                return await client.storage.get_peer_by_username(peer_id)
        return await client.storage.get_peer_by_id(peer_id)

    def _normalize_peer_string(self, peer_id: str) -> Union[int, str]:
        """Normalize all string inputs (links, @usernames, +phones)"""
        # Handle t.me/username links
        if match := re.match(
            r"(?:https?://)?(?:t\.me/|telegram\.(?:org|me|dog)/)(?:c/)?([\w]+)",
            peer_id.lower()
        ):
            try:
                return utils.get_channel_id(int(match.group(1)))
            except ValueError:
                return match.group(1)
        # Clean @/+ prefixes
        return re.sub(r"[@+\s]", "", peer_id.lower())

    async def _resolve_and_cache(self, client: PyroClient, peer_id: Union[int, str]):
        """Resolve and automatically cache all peer types"""
        # Handle string inputs
        if isinstance(peer_id, str):
            peer_id = self._normalize_peer_string(peer_id)
            if isinstance(peer_id, str):
                return await self._resolve_username(client, peer_id)

        # Handle numeric IDs
        peer_type = utils.get_peer_type(peer_id)
        
        if peer_type == "user":
            return await self._resolve_user(client, peer_id)
        elif peer_type == "chat":
            try:
                return await self._resolve_chat(client, peer_id)
            except (ChannelInvalid, ChannelPrivate):
                # Auto-retry as channel if chat fails
                return await self._resolve_channel(client, peer_id)
        else:
            return await self._resolve_channel(client, peer_id)

    async def _resolve_username(self, client: PyroClient, username: str):
        """Resolve username and cache the result"""
        result = await client.invoke(
            raw.functions.contacts.ResolveUsername(username=username)
        )
        peer = result.peer
        
        # Auto-cache the resolved peer
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
        raise PeerIdInvalid("Invalid peer type from API")

    async def _resolve_user(self, client: PyroClient, user_id: int):
        """Resolve user and cache the result"""
        users = await client.invoke(
            raw.functions.users.GetUsers(
                id=[raw.types.InputUser(user_id=user_id, access_hash=0)]
            )
        )
        return users[0]

    async def _resolve_chat(self, client: PyroClient, chat_id: int):
        """Resolve chat and cache the result"""
        chats = await client.invoke(raw.functions.messages.GetChats(id=[-chat_id]))
        return chats.chats[0]

    async def _resolve_channel(self, client: PyroClient, channel_id: int):
        """Resolve channel and cache the result"""
        channels = await client.invoke(
            raw.functions.channels.GetChannels(
                id=[raw.types.InputChannel(
                    channel_id=utils.get_channel_id(channel_id),
                    access_hash=0
                )]
            )
        )
        return channels.chats[0]
