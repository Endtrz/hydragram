from pyrogram import Client as PyroClient
from typing import Optional

class Client:
    _instance: Optional['Client'] = None
    app: Optional[PyroClient] = None  # Global Pyrogram Client instance

    def __init__(self, name: str, **kwargs):
        self._client = PyroClient(name, **kwargs)
        Client._instance = self
        Client.app = self._client  # Set global app to this instance

    @classmethod
    def get_client(cls) -> PyroClient:
        if cls._instance is None:
            raise RuntimeError("Client instance not created yet! Please create Client instance first.")
        return cls._instance._client

    def run(self) -> None:
        self._client.run()

    def add_handler(self, *args, **kwargs) -> None:
        """Forward add_handler to the underlying Pyrogram client"""
        self._client.add_handler(*args, **kwargs)

    def on_message(self, filters=None, group: int = 0):
        """Decorator to register message handlers"""
        from pyrogram.handlers import MessageHandler
        
        def decorator(func):
            self._client.add_handler(MessageHandler(func, filters), group)
            return func
        return decorator

# Explicitly expose the app variable
app: Optional[PyroClient] = Client.app
