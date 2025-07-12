from functools import wraps
from pyrogram.handlers import MessageHandler
from typing import Union, List
from .filters import command

def handler(
    commands: Union[str, List[str]],
    *,
    group: int = 0,
    dev_cmd: bool = False,
    owner_cmd: bool = False,
    gc_owner: bool = False,
    gc_admin: bool = False,
    case_sensitive: bool = False,
    extra=None
):
    def decorator(func):
        # Convert single string to list if needed
        cmd_list = [commands] if isinstance(commands, str) else commands
        
        base_filter = command(
            cmd_list,
            dev_cmd=dev_cmd,
            owner_cmd=owner_cmd,
            gc_owner=gc_owner,
            gc_admin=gc_admin,
            case_sensitive=case_sensitive
        )
        
        if extra:
            final_filter = base_filter & extra
        else:
            final_filter = base_filter

        from .client import Client as HydraClient
        pyro_client = HydraClient.get_client()
        pyro_client.add_handler(MessageHandler(func, final_filter), group)
        return func
    return decorator
