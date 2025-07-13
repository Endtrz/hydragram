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
        
        # Bind the resolver method properly
        self._client.resolve_peer = self._resolver.resolve
        
        Client._instance = self
        Client.app = self._client

    def __getattr__(self, name: str) -> Any:
        """Forward unknown attributes to Pyrogram client"""
        return getattr(self._client, name)

    async def resolve_peer(
        self,
        peer_id: Union[int, str, None],
        *,
        use_cache: bool = True
    ) -> Union[raw.base.InputPeer, raw.base.InputUser, raw.base.InputChannel]:
        """Public resolve_peer method"""
        return await self._resolver.resolve(self._client, peer_id, use_cache=use_cache)

    @classmethod
    def get_client(cls) -> PyroClient:
        """Get the active client instance"""
        if cls._instance is None:
            raise RuntimeError("Client not initialized. Use Client() first.")
        return cls._instance._client

    def run(self) -> None:
        """Start the client"""
        self._client.run()

# Global access
app: Optional[PyroClient] = Client.app
