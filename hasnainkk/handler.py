from functools import wraps
from hasnainkk.client import app
from hasnainkk.filters import command

def handler(
    commands,
    *,
    group=99996666,
    dev_cmd=False,
    owner_cmd=False,
    gc_owner=False,
    gc_admin=False,
    case_sensitive=False,
    extra=None
):
    def decorator(func):
        base_filter = command(
            commands,
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

        app.on_message(final_filter, group=group)(func)
        return func
    return decorator
