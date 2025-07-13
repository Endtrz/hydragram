from pyrogram import Client as PyroClient
from typing import Optional, Any, Union
from pyrogram import raw
from .peer_resolve import PeerResolver

class Client:
    _instance: Optional['Client'] = None
    app: Optional[PyroClient] = None

    def __init__(self, name: str, **kwargs):
        """
        Initialize Hydragram client with enhanced peer resolution
        
        Args:
            name: Session name/path
            kwargs: Standard Pyrogram client arguments
                (api_id, api_hash, bot_token, etc.)
        """
        # Initialize Pyrogram client
        self._client = PyroClient(name, **kwargs)
        
        # Initialize resolver and attach methods
        self._resolver = PeerResolver()
        self._client.resolve_peer = self._resolver.resolve
        
        # Set global instance
        Client._instance = self
        Client.app = self._client

    def __getattr__(self, name: str) -> Any:
        """Forward all unknown calls to Pyrogram client"""
        return getattr(self._client, name)

    async def resolve_peer(
        self,
        peer_id: Union[int, str, None],
        *,
        use_cache: bool = True
    ) -> raw.base.InputPeer:
        """
        Enhanced peer resolution supporting:
        - User IDs (123456)
        - Usernames (@username)
        - Phone numbers (+123456789)
        - Telegram links (t.me/username)
        - Special cases ("me", "self")
        
        Args:
            peer_id: Identifier to resolve
            use_cache: Whether to check cache first (default: True)
        """
        return await self._resolver.resolve(
            client=self._client,
            peer_id=peer_id,
            use_cache=use_cache
        )

    @classmethod
    def get_client(cls) -> PyroClient:
        """Get the active client instance"""
        if cls._instance is None:
            raise RuntimeError(
                "Client not initialized yet. "
                "First create instance with Client('session_name')"
            )
        return cls._instance._client

    def run(self) -> None:
        """Start the client"""
        self._client.run()

# Global access point
app: Optional[PyroClient] = Client.app
