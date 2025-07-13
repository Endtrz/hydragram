from pyrogram import Client as PyroClient
from typing import Optional, Any, Union
from pyrogram import raw
from .peer_resolve import PeerResolver

class Client:
    _instance: Optional['Client'] = None
    app: Optional[PyroClient] = None

    def __init__(self, name: str, **kwargs):
        """
        Initialize Hydragram client with proper peer resolution binding
        """
        self._client = PyroClient(name, **kwargs)
        self._resolver = PeerResolver()
        
        # Properly bind the resolver method
        self._client.resolve_peer = self._wrap_resolve_peer
        
        Client._instance = self
        Client.app = self._client

    def _wrap_resolve_peer(self, peer_id, *, use_cache=True):
        """Wrapper to maintain proper method binding"""
        return self._resolver.resolve(self._client, peer_id, use_cache=use_cache)

    def __getattr__(self, name: str) -> Any:
        """Forward unknown attributes to Pyrogram client"""
        return getattr(self._client, name)

    async def resolve_peer(
        self,
        peer_id: Union[int, str, None],
        *,
        use_cache: bool = True
    ) -> raw.base.InputPeer:
        """Public resolve_peer method"""
        return await self._wrap_resolve_peer(peer_id, use_cache=use_cache)

    @classmethod
    def get_client(cls) -> PyroClient:
        """Get active client instance"""
        if cls._instance is None:
            raise RuntimeError("Client not initialized. Use Client() first.")
        return cls._instance._client

    def run(self) -> None:
        """Start the client"""
        self._client.run()

# Global access
app: Optional[PyroClient] = Client.app
