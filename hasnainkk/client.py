from pyrogram import Client as PyroClient

class Client:
    _instance = None
    app = None  # Global Pyrogram Client instance

    def __init__(self, name, **kwargs):
        self._client = PyroClient(name, **kwargs)
        Client._instance = self
        Client.app = self._client  # Set global app to this instance

    @classmethod
    def get_client(cls):
        if cls._instance is None:
            raise RuntimeError("Client instance not created yet! Please create Client instance first.")
        return cls._instance._client

    def run(self):
        self._client.run()

# Explicitly expose the app variable
app = Client.app
