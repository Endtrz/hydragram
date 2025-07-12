# hasnainkk

An enhanced Pyrogram-like filter and handler system built on Kurigram.

## Installation

```bash
pip install hasnainkk

```
### How to use it

```python
from hasnainkk.client import Client
from hasnainkk.handler import handler
from hasnainkk.filters import command, group

client = Client("mybot", api_id=12345, api_hash="xyz", bot_token="TOKEN")

@handler("start")
async def start(client, message):
    await message.reply_text("Hello!")

client.run()
