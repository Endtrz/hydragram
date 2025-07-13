import re
import logging
from typing import Union, Optional
from pyrogram import raw, utils
from pyrogram.errors import PeerIdInvalid
from pyrogram import Client as PyroClient  # Proper import for type hints

log = logging.getLogger(__name__)

class PeerResolver:
    """Enhanced peer resolution with caching and error handling"""
    
    @staticmethod
    async def resolve(
        client: PyroClient,
        peer_id: Union[int, str, None],
        *,
        use_cache: bool = True
    ) -> Union[raw.base.InputPeer, raw.base.InputUser, raw.base.InputChannel]:
        if not client.is_connected:
            raise ConnectionError("Client not connected")

        # Short-circuit for special cases
        if peer_id in (None, "self", "me"):
            return raw.types.InputPeerSelf()

        # Try cache first
        if use_cache:
            try:
                return await client.storage.get_peer_by_id(peer_id)
            except (KeyError, AttributeError) as e:
                log.debug(f"Cache miss for {peer_id}: {e}")

        # Handle string inputs
        if isinstance(peer_id, str):
            processed_id = PeerResolver._parse_string_input(peer_id)
            if isinstance(processed_id, str):
                return await PeerResolver._resolve_username(client, processed_id)
            peer_id = processed_id

        # ID-based resolution
        peer_type = utils.get_peer_type(peer_id)
        try:
            return await PeerResolver._resolve_by_type(client, peer_id, peer_type)
        except Exception as e:
            log.error(f"Failed to resolve peer {peer_id}: {e}")
            raise PeerIdInvalid(f"Could not resolve peer: {peer_id}")

    @staticmethod
    def _parse_string_input(peer_id: str) -> Union[int, str]:
        """Parse links/usernames/phone numbers"""
        # Handle Telegram links
        if match := re.match(
            r"(?:https?://)?(?:t\.me/|telegram\.(?:org|me|dog)/)(?:c/)?([\w]+)",
            peer_id.lower()
        ):
            try:
                return utils.get_channel_id(int(match.group(1)))
            except ValueError:
                return match.group(1)
        
        # Clean usernames/phone numbers
        return re.sub(r"[@+\s]", "", peer_id.lower())

    @staticmethod
    async def _resolve_username(client: PyroClient, username: str):
        """Resolve through username API"""
        try:
            return await client.storage.get_peer_by_username(username)
        except (KeyError, AttributeError):
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
            raise PeerIdInvalid("Username returned invalid peer type")

    @staticmethod
    async def _resolve_by_type(client: PyroClient, peer_id: int, peer_type: str):
        """Type-specific resolution logic"""
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
