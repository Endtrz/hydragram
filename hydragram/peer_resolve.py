import re
import logging
from typing import Union, Optional
from pyrogram import raw, utils
from pyrogram.errors import PeerIdInvalid
from pyrogram import Client as PyroClient
from hydragram import *

log = logging.getLogger(__name__)

class PeerResolver:
    """
    Complete peer resolution system for Hydragram with:
    - Username/phone number support
    - Link parsing
    - Cache management
    - Error handling
    """

    @staticmethod
    async def resolve(
        client: PyroClient,
        peer_id: Union[int, str, None],
        *,
        use_cache: bool = True
    ) -> Union[raw.base.InputPeer, raw.base.InputUser, raw.base.InputChannel]:
        """
        Resolve any peer identifier to InputPeer
        
        Args:
            peer_id: Can be user_id, username, phone number, or link
            use_cache: Whether to check storage first (default: True)
            
        Returns:
            InputPeer/InputUser/InputChannel object
            
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
                cached = await PeerResolver._try_cache(client, peer_id)
                if cached:
                    return cached
            except Exception as e:
                log.debug(f"Cache lookup failed: {e}")

        # API resolution
        try:
            return await PeerResolver._resolve_via_api(client, peer_id)
        except Exception as e:
            log.error(f"Failed to resolve peer {peer_id}: {e}")
            raise PeerIdInvalid(f"Could not resolve peer: {peer_id}")

    @staticmethod
    async def _try_cache(
        client: PyroClient,
        peer_id: Union[int, str]
    ) -> Optional[Union[raw.base.InputPeer, raw.base.InputUser, raw.base.InputChannel]]:
        """Attempt to resolve peer from cache"""
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
    ) -> Union[raw.base.InputPeer, raw.base.InputUser, raw.base.InputChannel]:
        """Resolve peer through Telegram API"""
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
        else:
            return await PeerResolver._resolve_channel(client, peer_id)

    @staticmethod
    def _process_string_input(peer_id: str) -> Union[int, str]:
        """
        Parse various string formats:
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

    @staticmethod
    async def _resolve_username(
        client: PyroClient,
        username: str
    ) -> Union[raw.base.InputPeerUser, raw.base.InputPeerChannel]:
        """Resolve through username API"""
        result = await client.invoke(
            raw.functions.contacts.ResolveUsername(username=username)
        )
        
        if isinstance(result.peer, raw.types.PeerUser):
            return raw.types.InputPeerUser(
                user_id=result.peer.user_id,
                access_hash=0
            )
        elif isinstance(result.peer, raw.types.PeerChannel):
            return raw.types.InputPeerChannel(
                channel_id=utils.get_channel_id(result.peer.channel_id),
                access_hash=0
            )
        raise PeerIdInvalid("Invalid peer type in API response")

    @staticmethod
    async def _resolve_user(
        client: PyroClient,
        user_id: int
    ) -> raw.base.InputPeerUser:
        """Resolve user by ID"""
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
    ) -> raw.base.InputPeerChat:
        """Resolve chat by ID"""
        chats = await client.invoke(
            raw.functions.messages.GetChats(id=[-chat_id])
        )
        return utils.get_input_peer(chats.chats[0])

    @staticmethod
    async def _resolve_channel(
        client: PyroClient,
        channel_id: int
    ) -> raw.base.InputPeerChannel:
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
