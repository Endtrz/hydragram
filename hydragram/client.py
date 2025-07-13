from pyrogram import Client as PyroClient
from typing import Optional, Any, Union
from pyrogram import raw
from .storage import HydraStorage
from .peer_resolve import PeerResolver

class Client:
    _instance: Optional['Client'] = None
    app: Optional[PyroClient] = None

    def __init__(self, name: str, **kwargs):
        """
        Initialize Hydragram client with enhanced storage.
        
        Args:
            name: Session name
            kwargs: Additional Pyrogram client arguments
        """
        # Force our custom storage
        kwargs['storage'] = HydraStorage
        self._client = PyroClient(name, **kwargs)
        self._resolver = PeerResolver()
        
        # Bind resolver methods
        self._client.resolve_peer = self._resolver.resolve
        
        Client._instance = self
        Client.app = self._client

    def __getattr__(self, name: str) -> Any:
        """Forward all unknown attributes to Pyrogram client"""
        return getattr(self._client, name)

    async def resolve_peer(
        self,
        peer_id: Union[int, str, None],
        *,
        use_cache: bool = True
    ) -> Union[raw.base.InputPeer, raw.base.InputUser, raw.base.InputChannel]:
        """
        Enhanced peer resolution with:
        - Username/phone number support
        - Cache control
        - Link parsing
        """
        return await self._resolver.resolve(
            self._client,
            peer_id,
            use_cache=use_cache
        )

    @classmethod
    def get_client(cls) -> PyroClient:
        """Get the active client instance"""
        if cls._instance is None:
            raise RuntimeError(
                "Client not initialized. "
                "Create instance with Client('session_name') first."
            )
        return cls._instance._client

    def run(self) -> None:
        """Start the client"""
        self._client.run()

# Global access
app: Optional[PyroClient] = Client.app
