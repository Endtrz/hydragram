from pyrogram import Client as PyroClient
from typing import Optional, Any
from functools import partial

class Client:
    _instance: Optional['Client'] = None
    app: Optional[PyroClient] = None

    def __init__(self, name: str, **kwargs):
        self._client = PyroClient(name, **kwargs)
        Client._instance = self
        Client.app = self._client

    def __getattr__(self, name: str) -> Any:
        """Forward all unknown attributes to the underlying Pyrogram client"""
        return getattr(self._client, name)

    @classmethod
    def get_client(cls) -> PyroClient:
        if cls._instance is None:
            raise RuntimeError("Client instance not created yet! Please create Client instance first.")
        return cls._instance._client

    def run(self) -> None:
        self._client.run()

# Expose the app variable
app: Optional[PyroClient] = Client.app
