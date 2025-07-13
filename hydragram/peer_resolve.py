import re
import logging
from typing import Union, Optional
from pyrogram import raw, utils
from pyrogram.errors import PeerIdInvalid
from pyrogram import Client as PyroClient

log = logging.getLogger(__name__)

class PeerResolver:
    """
    Complete peer resolution system for Hydragram
    Compatible with latest Pyrogram raw types
    """

    @staticmethod
    async def resolve(
        client: PyroClient,
        peer_id: Union[int, str, None],
        *,
        use_cache: bool = True
    ) -> raw.base.InputPeer:
        """
        Resolve any peer identifier to InputPeer
        
        Args:
            peer_id: Can be user_id, username, phone number, or link
            use_cache: Whether to check storage first (default: True)
            
        Returns:
            InputPeer object
            
        Raises:
            PeerIdInvalid: When peer cannot be resolved
            ConnectionError: When client is disconnected
        """
        if not client.is_connected:
            raise ConnectionError("Client not connected")

        if peer_id in (None, "self", "me"):
            return raw.types.InputPeerSelf()

        if use_cache:
            try:
                cached = await PeerResolver._try_cache(client, peer_id)
                if cached:
                    return cached
            except Exception as e:
                log.debug(f"Cache lookup failed: {e}")

        try:
            return await PeerResolver._resolve_via_api(client, peer_id)
        except Exception as e:
            log.error(f"Failed to resolve peer {peer_id}: {e}")
            raise PeerIdInvalid(f"Could not resolve peer: {peer_id}")

    @staticmethod
    async def _try_cache(
        client: PyroClient,
        peer_id: Union[int, str]
    ) -> Optional[raw.base.InputPeer]:
        if isinstance(peer_id, str):
            if peer_id.startswith("+"):
                peer = await client.storage.get_peer_by_phone_number(peer_id)
            else:
                peer = await client.storage.get_peer_by_username(peer_id)
            return utils.get_input_peer(peer)
        return await client.storage.get_peer_by_id(peer_id)

    @staticmethod
    async def _resolve_via_api(
        client: PyroClient,
        peer_id: Union[int, str]
    ) -> raw.base.InputPeer:
        if isinstance(peer_id, str):
            processed = PeerResolver._process_string_input(peer_id)
            if isinstance(processed, str):
                return await PeerResolver._resolve_username(client, processed)
            peer_id = processed

        peer_type = utils.get_peer_type(peer_id)
        if peer_type == "user":
            return await PeerResolver._resolve_user(client, peer_id)
        elif peer_type == "chat":
            return await PeerResolver._resolve_chat(client, peer_id)
        return await PeerResolver._resolve_channel(client, peer_id)

    @staticmethod
    def _process_string_input(peer_id: str) -> Union[int, str]:
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
    async def _resolve_username(
        client: PyroClient,
        username: str
    ) -> raw.base.InputPeer:
        result = await client.invoke(
            raw.functions.contacts.ResolveUsername(username=username)
        )
        return utils.get_input_peer(result.peer)

    @staticmethod
    async def _resolve_user(
        client: PyroClient,
        user_id: int
    ) -> raw.base.InputPeer:
        users = await client.invoke(
            raw.functions.users.GetUsers(
                id=[raw.types.InputUser(user_id=user_id, access_hash=0)]
            )
        )
        return utils.get_input_peer(users[0])

    @staticmethod
    async def _resolve_chat(
        client: PyroClient,
        chat_id: int
    ) -> raw.base.InputPeer:
        chats = await client.invoke(
            raw.functions.messages.GetChats(id=[-chat_id])
        )
        return utils.get_input_peer(chats.chats[0])

    @staticmethod
    async def _resolve_channel(
        client: PyroClient,
        channel_id: int
    ) -> raw.base.InputPeer:
        channels = await client.invoke(
            raw.functions.channels.GetChannels(
                id=[raw.types.InputChannel(
                    channel_id=utils.get_channel_id(channel_id),
                    access_hash=0
                )]
            )
        )
        return utils.get_input_peer(channels.chats[0])
