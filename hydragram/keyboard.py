from dataclasses import dataclass
from typing import List, Optional, Dict, Any

@dataclass
class KeyboardButton:
    """This object represents one button of the reply keyboard"""
    text: str
    request_contact: bool = False
    request_location: bool = False
    request_poll: Optional[Dict[str, str]] = None

@dataclass
class ReplyKeyboardMarkup:
    """This object represents a custom keyboard with reply options"""
    keyboard: List[List[KeyboardButton]]
    resize_keyboard: bool = True
    one_time_keyboard: bool = False
    selective: bool = False

@dataclass
class InlineKeyboardButton:
    """This object represents one button of an inline keyboard"""
    text: str
    url: Optional[str] = None
    callback_data: Optional[str] = None
    web_app: Optional[Dict[str, str]] = None
    login_url: Optional[Dict[str, str]] = None
    switch_inline_query: Optional[str] = None
    switch_inline_query_current_chat: Optional[str] = None
    callback_game: Optional[Dict[str, Any]] = None
    pay: bool = False

@dataclass
class InlineKeyboardMarkup:
    """This object represents an inline keyboard"""
    inline_keyboard: List[List[InlineKeyboardButton]]

@dataclass
class ReplyKeyboardRemove:
    """Upon receiving a message with this object, Telegram clients will remove the current custom keyboard"""
    remove_keyboard: bool = True
    selective: bool = False

@dataclass
class ForceReply:
    """Upon receiving a message with this object, Telegram clients will display a reply interface"""
    force_reply: bool = True
    input_field_placeholder: Optional[str] = None
    selective: bool = False
