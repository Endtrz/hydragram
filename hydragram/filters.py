from re import compile as compile_re
from re import escape
from shlex import split
from typing import List, Union, Optional, Set

from pyrogram.enums import ChatType, ChatMemberStatus
from pyrogram.errors import RPCError, UserNotParticipant
from pyrogram.filters import create
from pyrogram.types import Message


class Config:
    # Default values
    _OWNER_ID: int = 6346273488
    _DEV_USERS: List[int] = [6346273488, 5907205317, 5881613383, 1284920298, 1805959544, 8171988347]
    _PREFIX_HANDLER: List[str] = ["/", "!", "."]
    _BOT_USERNAME: str = "Raiden_Robot"
    
    # Internal variable
    _DEV_LEVEL: Set[int] = set(_DEV_USERS + [_OWNER_ID])
    
    @classmethod
    def set_owner_id(cls, owner_id: int):
        cls._OWNER_ID = owner_id
        cls._update_dev_level()
    
    @classmethod
    def set_dev_users(cls, dev_users: List[int]):
        cls._DEV_USERS = dev_users
        cls._update_dev_level()
    
    @classmethod
    def set_prefix_handler(cls, prefixes: List[str]):
        cls._PREFIX_HANDLER = prefixes
    
    @classmethod
    def set_bot_username(cls, username: str):
        cls._BOT_USERNAME = username
    
    @classmethod
    def _update_dev_level(cls):
        cls._DEV_LEVEL = set(cls._DEV_USERS + [cls._OWNER_ID])


def command(
    commands: Union[str, List[str]],
    case_sensitive: bool = False,
    owner_cmd: bool = False,
    dev_cmd: bool = False,
    gc_owner: bool = False,
    gc_admin: bool = False,
):
    async def func(flt, client, m: Message):
        if not m:
            return False

        if m.edit_date or not m.from_user:
            return False

        if m.chat and m.chat.type == ChatType.CHANNEL:
            return False

        if m.from_user.is_bot or m.forward_from_chat or m.forward_from:
            return False

        if owner_cmd and m.from_user.id != Config._OWNER_ID:
            return False

        if dev_cmd and m.from_user.id not in Config._DEV_LEVEL:
            return False

        # Group owner check
        if gc_owner:
            try:
                member = await client.get_chat_member(m.chat.id, m.from_user.id)
                if member.status != ChatMemberStatus.OWNER:
                    return False
            except Exception:
                return False

        # Group admin check (includes owner)
        if gc_admin:
            try:
                member = await client.get_chat_member(m.chat.id, m.from_user.id)
                if member.status not in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                    return False
            except Exception:
                return False

        text: str = m.text or m.caption
        if not text:
            return False

        regex = r"^[{prefix}](\w+)(@{bot_name})?(?: |$)(.*)".format(
            prefix="|".join(escape(x) for x in Config._PREFIX_HANDLER),
            bot_name=Config._BOT_USERNAME,
        )
        matches = compile_re(regex).search(text)
        if matches:
            m.command = [matches.group(1)]
            if matches.group(1) not in flt.commands:
                return False

            if matches.group(3) == "":
                return True

            try:
                for arg in split(matches.group(3)):
                    m.command.append(arg)
            except ValueError:
                pass

            return True

        return False

    commands = commands if isinstance(commands, list) else [commands]
    commands = {c if case_sensitive else c.lower() for c in commands}

    return create(
        func,
        "NormalCommandFilter",
        commands=commands,
        case_sensitive=case_sensitive,
    )


def setup(
    OWNER_ID: Optional[int] = None,
    DEV_USERS: Optional[List[int]] = None,
    PREFIX_HANDLER: Optional[List[str]] = None,
    BOT_USERNAME: Optional[str] = None
):
    """Initialize the package with user-specific settings
    
    Args:
        OWNER_ID: Your user ID as owner (optional)
        DEV_USERS: List of developer user IDs (optional)
        PREFIX_HANDLER: List of command prefixes (optional)
        BOT_USERNAME: Your bot's username (optional)
    """
    if OWNER_ID is not None:
        Config.set_owner_id(OWNER_ID)
    if DEV_USERS is not None:
        Config.set_dev_users(DEV_USERS)
    if PREFIX_HANDLER is not None:
        Config.set_prefix_handler(PREFIX_HANDLER)
    if BOT_USERNAME is not None:
        Config.set_bot_username(BOT_USERNAME)



from pyrogram.filters import (
    all as all_,
    me,
    bot,
    regex,
    incoming,
    outgoing,
    text,
    reply,
    forwarded,
    caption,
    audio,
    document,
    photo,
    sticker,
    animation,
    game,
    video,
    media_group,
    voice,
    video_note,
    contact,
    location,
    venue,
    web_page,
    poll,
    dice,
    media_spoiler,
    private,
    group,
    channel,
    new_chat_members,
    left_chat_member,
    new_chat_title,
    new_chat_photo,
    delete_chat_photo,
    group_chat_created,
    supergroup_chat_created,
    channel_chat_created,
    migrate_to_chat_id,
    migrate_from_chat_id,
    pinned_message,
    game_high_score,
    reply_keyboard,
    inline_keyboard,
    mentioned,
    via_bot,
    video_chat_started,
    video_chat_ended,
    video_chat_members_invited,
    service,
    media,
    scheduled,
    from_scheduled,
    linked_channel,
)



__all__ = [
    "command",
    "all_",
    "me",
    "bot",
    "regex",
    "incoming",
    "outgoing",
    "text",
    "reply",
    "forwarded",
    "caption",
    "audio",
    "document",
    "photo",
    "sticker",
    "animation",
    "game",
    "video",
    "media_group",
    "voice",
    "video_note",
    "contact",
    "location",
    "venue",
    "web_page",
    "poll",
    "dice",
    "media_spoiler",
    "private",
    "group",
    "channel",
    "new_chat_members",
    "left_chat_member",
    "new_chat_title",
    "new_chat_photo",
    "delete_chat_photo",
    "group_chat_created",
    "supergroup_chat_created",
    "channel_chat_created",
    "migrate_to_chat_id",
    "migrate_from_chat_id",
    "pinned_message",
    "game_high_score",
    "reply_keyboard",
    "inline_keyboard",
    "mentioned",
    "via_bot",
    "video_chat_started",
    "video_chat_ended",
    "video_chat_members_invited",
    "service",
    "media",
    "scheduled",
    "from_scheduled",
    "linked_channel",
    ]
