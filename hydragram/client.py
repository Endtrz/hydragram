from pyrogram import Client as PyroClient
from typing import Optional, Any, Union
from pyrogram import raw
from .peer_resolve import PeerResolver

class Client:
    _instance: Optional['Client'] = None
    app: Optional[PyroClient] = None

    def __init__(self, name: str, **kwargs):
        self._client = PyroClient(name, **kwargs)
        self._resolver = PeerResolver()
        
        # Bind resolver with proper method signature
        self._client.resolve_peer = self._resolver.resolve
        
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
        """One-line resolution for all peer types"""
        return await self._resolver.resolve(self._client, peer_id, use_cache=use_cache)

    @classmethod
    def get_client(cls) -> PyroClient:
        """Get active client instance"""
        if cls._instance is None:
            raise RuntimeError("Client not initialized yet")
        return cls._instance._client

    def run(self) -> None:
        """Start the client"""
        self._client.run()

# Global access
app: Optional[PyroClient] = Client.app
