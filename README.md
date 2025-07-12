### hydragram

Hydragram is an enhanced filter and handler system inspired by Pyrogram, built on top of Kurigram.

It helps you avoid common Pyrogram errors—especially those related to using `group={}` in command handlers—by providing a simple and stable way to register commands and filters.

With Hydragram, you no longer need to manually manage the `group` parameter or worry about filter conflicts. It handles all that smoothly for you, so your commands work reliably every time.

Simplify your Telegram bot development and enjoy clean, easy-to-use decorators and filters designed for real-world use.


## Installation

```bash
pip install hydragram==2.0

```
### How to use it

```python
from hydragram.client import Client
from hydragram.handler import handler
from hydragram.filters import command, group
from Germany import filters 

client = Client("mybot", api_id=12345, api_hash="xyz", bot_token="TOKEN")

@handler("start")
async def start(client, message):
    await message.reply_text("Hello!")

# <======== YOU CAN ALSO USE IT IN ANOTHER WAY =====>
@client.on_message(filters.command("start"))
async def start(client, message):
    await message.reply_text("Hello this is second way!")

client.run()
```


### Example How to use them 
```python
from hydragram import Client
from hydragram.filters import setup 
from hydragram.handler import handler
from hydragram.filters import group, private
from pyrogram.types import Message

app = Client(
    "TestBot",
    api_id=You_Api_Id, # generate it from my.telegram.org
    api_hash="Your_Api_hash", # generate it from my.telegram.org
    bot_token="Your_Bot_Token" # Get from @BotFather
)

# Initialize with your settings
setup(
    OWNER_ID=6346273488,  # Replace Your user ID and generate from @Raiden_Robot
    DEV_USERS=[8171988347, 5907205317],  # Your dev team Id and generate Their I'd from @Raiden_Robot
    PREFIX_HANDLER=["/", "!" "Raiden ", "raiden "],  # Command prefixes
    BOT_USERNAME="@Raiden_Robot"  # Your bot's username
)

# Regular command (anyone can use)
@handler("start", extra=private)
async def start_handler(_, m: Message):
    await m.reply("Hello! I'm alive ✅")

# Group command
@handler("alive", extra=group)
async def alive(_, m: Message):
    await m.reply("Bot is working fine in group!")

# Owner-only command
@handler("owner", owner_cmd=True)
async def owner_command(_, m: Message):
    await m.reply("👑 This is an owner-only command!")

# Developer-only command
@handler("dev", dev_cmd=True)
async def dev_command(_, m: Message):
    await m.reply("💻 This is a developer-only command!")

# Group owner-only command
@handler("gowner", gc_owner=True)
async def group_owner_command(_, m: Message):
    await m.reply("🏘️ This command can only be used by group owners!")

# Group admin-only command (includes owner)
@handler("gadmin", gc_admin=True)
async def group_admin_command(_, m: Message):
    await m.reply("🛡️ This command can only be used by group admins!")

# Combined permission example
@handler("power", dev_cmd=True, gc_admin=True)
async def power_command(_, m: Message):
    await m.reply("⚡ This command requires either dev rights or group admin!")

if __name__ == "__main__":
    app.run()

```
### 🤝 Contributing

Contributions are welcome! If you have suggestions, improvements, or bug fixes,
feel free to fork the repository and submit a pull request.


### Created By [hasnainkk-07](https://github.com/hasnainkk-07)
