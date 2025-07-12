from functools import wraps
from typing import Union, List, Optional
from pyrogram import filters as pyro_filters
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
from pyrogram.types import Message, CallbackQuery


def handler(
    commands: Optional[Union[str, List[str]]] = None,
    *,
    group: int = 9999999,
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

        # Store the client reference when the handler is registered
        wrapper._client_ref = None
        
        def register_handler(client):
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
                client.add_handler(MessageHandler(wrapper, flt), group)
            elif handler_type == "callback_query":
                client.add_handler(CallbackQueryHandler(wrapper, flt), group)
            
            wrapper._client_ref = client

        # Try to register immediately if client is available
        try:
            from .client import Client as HydraClient
            pyro_client = HydraClient.get_client()
            register_handler(pyro_client)
        except RuntimeError:
            # If Hydragram client not available, defer registration
            # This will work with Pyrogram's decorator system
            pass

        return wrapper
    return decorator
        
