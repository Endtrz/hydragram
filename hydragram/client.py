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
        
        # Proper method binding with all parameters
        self._client.resolve_peer = lambda pid, **kw: self._resolver.resolve(
            self._client, pid, **{**{'use_cache': True, 'retry_as_channel': True}, **kw}
        )
        
        Client._instance = self
        Client.app = self._client

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)

    async def resolve_peer(
        self,
        peer_id: Union[int, str, None],
        *,
        use_cache: bool = True,
        retry_as_channel: bool = True
    ) -> raw.base.InputPeer:
        """Enhanced resolve_peer with retry options"""
        return await self._resolver.resolve(
            self._client,
            peer_id,
            use_cache=use_cache,
            retry_as_channel=retry_as_channel
        )

    @classmethod
    def get_client(cls) -> PyroClient:
        if cls._instance is None:
            raise RuntimeError("Client not initialized. Use Client() first.")
        return cls._instance._client

    def run(self) -> None:
        self._client.run()

app: Optional[PyroClient] = Client.app
