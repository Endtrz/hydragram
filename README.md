# hasnainkk

An enhanced Pyrogram-like filter and handler system built on Kurigram.

## Installation

```bash
pip install hydragram==2.0

```
### How to use it

```python
from hydragram.client import Client
from hydragram.handler import handler
from hydragram.filters import command, group

client = Client("mybot", api_id=12345, api_hash="xyz", bot_token="TOKEN")

@handler("start")
async def start(client, message):
    await message.reply_text("Hello!")

client.run()
