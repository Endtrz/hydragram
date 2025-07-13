import re
import logging
from typing import Union, Optional
from pyrogram import raw, utils
from pyrogram.errors import PeerIdInvalid, ChannelInvalid, ChannelPrivate
from pyrogram import Client as PyroClient

log = logging.getLogger(__name__)

class PeerResolver:
    """Enhanced peer resolution based on Pyrogram's ResolvePeer"""

    async def resolve(
        self,
        client: PyroClient,
        peer_id: Union[int, str, None],
        *,
        use_cache: bool = True
    ) -> Union[raw.base.InputPeer, raw.base.InputUser, raw.base.InputChannel]:
        """
        Enhanced version of Pyrogram's resolve_peer with:
        - Better error handling
        - Cache control
        - Negative ID support
        """
        if not client.is_connected:
            raise ConnectionError("Client has not been started yet")

        if peer_id is None:
            return None

        if peer_id in ("self", "me"):
            return raw.types.InputPeerSelf()

        try:
            if use_cache:
                return await self._resolve_from_cache(client, peer_id)
        except KeyError:
            pass

        try:
            if isinstance(peer_id, str):
                return await self._resolve_string(client, peer_id)
            else:
                return await self._resolve_id(client, peer_id)
        except Exception as e:
            log.error(f"Failed to resolve peer {peer_id}: {e}")
            raise PeerIdInvalid(f"Could not resolve peer: {peer_id}")

    async def _resolve_from_cache(self, client: PyroClient, peer_id: Union[int, str]):
        """Try resolving from storage cache first"""
        if isinstance(peer_id, str):
            peer_id = self._normalize_string(peer_id)
            if isinstance(peer_id, str):
                try:
                    return await client.storage.get_peer_by_username(peer_id)
                except KeyError:
                    return await client.storage.get_peer_by_phone_number(peer_id)
        return await client.storage.get_peer_by_id(peer_id)

    def _normalize_string(self, peer_id: str) -> Union[int, str]:
        """Normalize usernames/links/phone numbers"""
        if match := re.match(r"^(?:https?://)?(?:www\.)?(?:t(?:elegram)?\.(?:org|me|dog)/(?:c/)?)([\w]+)(?:.+)?$", peer_id.lower()):
            try:
                return utils.get_channel_id(int(match.group(1)))
            except ValueError:
                return match.group(1)
        return re.sub(r"[@+\s]", "", peer_id.lower())

    async def _resolve_string(self, client: PyroClient, peer_id: str):
        """Resolve string inputs (usernames/links/phones)"""
        peer_id = self._normalize_string(peer_id)
        if isinstance(peer_id, str):
            r = await client.invoke(
                raw.functions.contacts.ResolveUsername(
                    username=peer_id
                )
            )
            if isinstance(r.peer, raw.types.PeerUser):
                return raw.types.InputPeerUser(
                    user_id=r.peer.user_id,
                    access_hash=0
                )
            elif isinstance(r.peer, raw.types.PeerChannel):
                return raw.types.InputPeerChannel(
                    channel_id=utils.get_channel_id(r.peer.channel_id),
                    access_hash=0
                )
            raise PeerIdInvalid("Invalid peer type from API")
        return await self._resolve_id(client, peer_id)

    async def _resolve_id(self, client: PyroClient, peer_id: int):
        """Resolve numeric IDs"""
        peer_type = utils.get_peer_type(peer_id)
        
        if peer_type == "user":
            users = await client.invoke(
                raw.functions.users.GetUsers(
                    id=[raw.types.InputUser(user_id=peer_id, access_hash=0)]
                )
            )
            return users[0]
        elif peer_type == "chat":
            try:
                chats = await client.invoke(raw.functions.messages.GetChats(id=[-peer_id]))
                return chats.chats[0]
            except (ChannelInvalid, ChannelPrivate):
                # Try resolving as channel if chat fails
                channels = await client.invoke(
                    raw.functions.channels.GetChannels(
                        id=[raw.types.InputChannel(
                            channel_id=utils.get_channel_id(peer_id),
                            access_hash=0
                        )]
                    )
                )
                return channels.chats[0]
        else:
            channels = await client.invoke(
                raw.functions.channels.GetChannels(
                    id=[raw.types.InputChannel(
                        channel_id=utils.get_channel_id(peer_id),
                        access_hash=0
                    )]
                )
            )
            return channels.chats[0]
