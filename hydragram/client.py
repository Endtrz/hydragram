from pyrogram import Client as PyroClient
from typing import Optional, Any
from .resolve_peer import ResolvePeer

class Client(PyroClient):
    _instance: Optional["Client"] = None
    app: Optional[PyroClient] = None

    def __init__(self, name: str, **kwargs):
        super().__init__(name, **kwargs)
        Client._instance = self
        Client.app = self

    def __getattr__(self, name: str) -> Any:
        """Forward unknown attributes to Pyrogram.Client"""
        return getattr(super(), name)

    async def resolve_peer(self, id):
        obj = ResolvePeer(self)
        return await obj.resolve_peer(id)

    @classmethod
    def get_client(cls) -> PyroClient:
        if cls._instance is None:
            raise RuntimeError(
                "Client instance not created yet! Please create the Client first."
            )
        return cls._instance

    def run(self) -> None:
        """Start the Pyrogram client (blocking)"""
        super().run()


# Expose the app variable globally
app: Optional[PyroClient] = Client.app
