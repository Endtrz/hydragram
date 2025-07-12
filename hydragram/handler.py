from functools import wraps
from typing import Union, List, Optional
from pyrogram import filters as pyro_filters
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
from pyrogram.types import Message, CallbackQuery

def handler(
    commands: Optional[Union[str, List[str]]] = None,
    *,
    group: int = 0,
    dev_cmd: bool = False,
    owner_cmd: bool = False,
    gc_owner: bool = False,
    gc_admin: bool = False,
    case_sensitive: bool = False,
    filters=None,
    handler_type: str = "message"
):
    def decorator(func):
        @wraps(func)
        async def wrapper(client, update):
            return await func(client, update)

        try:
            # Try getting Hydragram client first
            from .client import Client as HydraClient
            pyro_client = HydraClient.get_client()
        except RuntimeError:
            # Fall back to Pyrogram client if available
            from pyrogram import Client as PyroClient
            if isinstance(wrapper.__self__, PyroClient):
                pyro_client = wrapper.__self__
            else:
                raise

        # Build filters
        if commands is not None and handler_type == "message":
            from .filters import command as hydra_command
            cmd_list = [commands] if isinstance(commands, str) else commands
            flt = hydra_command(
                cmd_list,
                dev_cmd=dev_cmd,
                owner_cmd=owner_cmd,
                gc_owner=gc_owner,
                gc_admin=gc_admin,
                case_sensitive=case_sensitive
            )
            if filters:
                flt = flt & filters
        else:
            flt = filters if filters else pyro_filters.all

        # Add appropriate handler
        if handler_type == "message":
            pyro_client.add_handler(MessageHandler(wrapper, flt), group)
        elif handler_type == "callback_query":
            pyro_client.add_handler(CallbackQueryHandler(wrapper, flt), group)
        # Add more handler types as needed

        return wrapper
    return decorator
