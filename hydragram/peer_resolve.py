import re
import logging
from typing import Union, Optional
from pyrogram import raw, utils
from pyrogram.errors import PeerIdInvalid, ChannelInvalid, ChannelPrivate
from pyrogram import Client as PyroClient

log = logging.getLogger(__name__)

class PeerResolver:
    """Complete peer resolution with all fixes"""

    async def resolve(
        self,
        client: PyroClient,
        peer_id: Union[int, str, None],
        *,
        use_cache: bool = True
    ) -> raw.base.InputPeer:
        try:
            # Handle special cases
            if peer_id in (None, "self", "me"):
                return raw.types.InputPeerSelf()

            # Handle negative chat IDs
            if isinstance(peer_id, int) and peer_id < 0:
                try:
                    return await self._resolve_chat(client, abs(peer_id))
                except (ChannelInvalid, ChannelPrivate):
                    return await self._resolve_channel(client, abs(peer_id))

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
                    log.debug(f"Cache miss: {e}")

            # Process string inputs
            if isinstance(peer_id, str):
                peer_id = self._normalize_string(peer_id)
                if isinstance(peer_id, str):
                    return await self._resolve_username(client, peer_id)

            # ID-based resolution
            peer_type = utils.get_peer_type(peer_id)
            if peer_type == "user":
                return await self._resolve_user(client, peer_id)
            elif peer_type == "chat":
                return await self._resolve_chat(client, peer_id)
            else:
                return await self._resolve_channel(client, peer_id)

        except Exception as e:
            log.error(f"Peer resolution failed for {peer_id}: {e}")
            raise PeerIdInvalid(f"Could not resolve peer: {peer_id}")

    def _normalize_string(self, peer_id: str) -> Union[int, str]:
        """Normalize usernames/links/phone numbers"""
        if match := re.match(r"(?:https?://)?(?:t\.me/|telegram\.(?:org|me|dog)/)(?:c/)?([\w]+)", peer_id.lower()):
            try:
                return utils.get_channel_id(int(match.group(1)))
            except ValueError:
                return match.group(1)
        return re.sub(r"[@+\s]", "", peer_id.lower())

    async def _resolve_username(self, client: PyroClient, username: str) -> raw.base.InputPeer:
        r = await client.invoke(raw.functions.contacts.ResolveUsername(username=username))
        return utils.get_input_peer(r.peer)

    async def _resolve_user(self, client: PyroClient, user_id: int) -> raw.base.InputPeer:
        users = await client.invoke(raw.functions.users.GetUsers(
            id=[raw.types.InputUser(user_id=user_id, access_hash=0)]
        ))
        return utils.get_input_peer(users[0])

    async def _resolve_chat(self, client: PyroClient, chat_id: int) -> raw.base.InputPeer:
        chats = await client.invoke(raw.functions.messages.GetChats(id=[-chat_id]))
        return utils.get_input_peer(chats.chats[0])

    async def _resolve_channel(self, client: PyroClient, channel_id: int) -> raw.base.InputPeer:
        channels = await client.invoke(raw.functions.channels.GetChannels(
            id=[raw.types.InputChannel(
                channel_id=utils.get_channel_id(channel_id),
                access_hash=0
            )]
        ))
        return utils.get_input_peer(channels.chats[0])
