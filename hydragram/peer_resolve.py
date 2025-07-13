import re
import logging
from typing import Union, Optional
from pyrogram import raw, utils
from pyrogram.errors import PeerIdInvalid, ChannelInvalid, ChannelPrivate
from pyrogram import Client as PyroClient

log = logging.getLogger(__name__)

class PeerResolver:
    """Complete peer resolution with all edge cases handled"""

    async def resolve(
        self,
        client: PyroClient,
        peer_id: Union[int, str, None],
        *,
        use_cache: bool = True,
        retry_as_channel: bool = True
    ) -> raw.base.InputPeer:
        """
        Resolve any peer identifier with comprehensive error handling
        
        Args:
            peer_id: Can be user_id, username, phone, link, or negative chat_id
            use_cache: Check storage first (default: True)
            retry_as_channel: Try as channel if chat resolution fails (default: True)
        """
        try:
            # Handle None/"me"/"self"
            if peer_id in (None, "self", "me"):
                return raw.types.InputPeerSelf()

            # Convert string inputs
            if isinstance(peer_id, str):
                peer_id = self._normalize_input(peer_id)

            # Handle negative IDs (chat/channel)
            if isinstance(peer_id, int) and peer_id < 0:
                abs_id = abs(peer_id)
                try:
                    return await self._resolve_chat(client, abs_id)
                except (ChannelInvalid, ChannelPrivate) if retry_as_channel else Exception:
                    return await self._resolve_channel(client, abs_id)

            # Cache lookup
            if use_cache:
                try:
                    if isinstance(peer_id, str):
                        if peer_id.startswith("+"):
                            peer = await client.storage.get_peer_by_phone_number(peer_id)
                        else:
                            peer = await client.storage.get_peer_by_username(peer_id)
                    else:
                        peer = await client.storage.get_peer_by_id(peer_id)
                    return utils.get_input_peer(peer)
                except Exception as e:
                    log.debug(f"Cache miss for {peer_id}: {e}")

            # API resolution
            if isinstance(peer_id, str):
                return await self._resolve_username(client, peer_id)

            peer_type = utils.get_peer_type(peer_id)
            if peer_type == "user":
                return await self._resolve_user(client, peer_id)
            elif peer_type == "chat":
                return await self._resolve_chat(client, peer_id)
            else:
                return await self._resolve_channel(client, peer_id)

        except Exception as e:
            log.error(f"Failed to resolve {peer_id} (type: {type(peer_id)}): {e}")
            raise PeerIdInvalid(f"Could not resolve peer (server might be down or peer inaccessible): {peer_id}")

    def _normalize_input(self, peer_id: str) -> Union[int, str]:
        """Normalize links/usernames/phone numbers"""
        # Handle t.me/username or telegram.me/username
        if match := re.match(r"(?:https?://)?(?:t\.me/|telegram\.(?:org|me|dog)/)(?:c/)?([\w]+)", peer_id.lower()):
            try:
                return utils.get_channel_id(int(match.group(1)))
            except ValueError:
                return match.group(1)
        # Clean @/+ prefixes and whitespace
        return re.sub(r"[@+\s]", "", peer_id.lower())

    async def _resolve_username(self, client: PyroClient, username: str) -> raw.base.InputPeer:
        """Resolve through username API"""
        try:
            r = await client.invoke(raw.functions.contacts.ResolveUsername(username=username))
            return utils.get_input_peer(r.peer)
        except Exception as e:
            log.error(f"Username resolution failed for @{username}: {e}")
            raise

    async def _resolve_user(self, client: PyroClient, user_id: int) -> raw.base.InputPeer:
        """Resolve user by ID"""
        users = await client.invoke(
            raw.functions.users.GetUsers(
                id=[raw.types.InputUser(user_id=user_id, access_hash=0)]
            )
        )
        return utils.get_input_peer(users[0])

    async def _resolve_chat(self, client: PyroClient, chat_id: int) -> raw.base.InputPeer:
        """Resolve chat by ID"""
        chats = await client.invoke(raw.functions.messages.GetChats(id=[-chat_id]))
        return utils.get_input_peer(chats.chats[0])

    async def _resolve_channel(self, client: PyroClient, channel_id: int) -> raw.base.InputPeer:
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
