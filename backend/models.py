from pydantic import BaseModel, Field, EmailStr
from typing import List, Optional, Dict, Any
from datetime import datetime
import uuid


def _id():
    return str(uuid.uuid4())


def _now():
    return datetime.utcnow()


class UserPublic(BaseModel):
    id: str
    username: str
    name: str
    email: str
    avatar: str
    instagramConnected: bool = False
    instagramHandle: Optional[str] = None


class SignupIn(BaseModel):
    username: str
    email: EmailStr
    password: str


class LoginIn(BaseModel):
    username: str
    password: str


class AuthOut(BaseModel):
    token: str
    user: UserPublic


class AutomationIn(BaseModel):
    name: str
    trigger: str = 'Manual'
    status: str = 'draft'
    nodes: Optional[List[Dict[str, Any]]] = None
    edges: Optional[List[Dict[str, Any]]] = None


class AutomationPatch(BaseModel):
    name: Optional[str] = None
    trigger: Optional[str] = None
    status: Optional[str] = None
    nodes: Optional[List[Dict[str, Any]]] = None
    edges: Optional[List[Dict[str, Any]]] = None


class Automation(BaseModel):
    id: str = Field(default_factory=_id)
    user_id: str
    name: str
    trigger: str = 'Manual'
    status: str = 'draft'
    sent: int = 0
    clicks: int = 0
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    updated: datetime = Field(default_factory=_now)
    created: datetime = Field(default_factory=_now)


class ContactIn(BaseModel):
    name: str
    username: str
    avatar: Optional[str] = None
    tags: List[str] = []
    subscribed: bool = True


class ContactPatch(BaseModel):
    name: Optional[str] = None
    tags: Optional[List[str]] = None
    subscribed: Optional[bool] = None


class Contact(BaseModel):
    id: str = Field(default_factory=_id)
    user_id: str
    name: str
    username: str
    avatar: str
    tags: List[str] = []
    subscribed: bool = True
    lastActive: datetime = Field(default_factory=_now)
    created: datetime = Field(default_factory=_now)


class BroadcastIn(BaseModel):
    name: str
    message: str
    audience_size: Optional[int] = None


class BroadcastPatch(BaseModel):
    status: Optional[str] = None
    name: Optional[str] = None
    message: Optional[str] = None


class Broadcast(BaseModel):
    id: str = Field(default_factory=_id)
    user_id: str
    name: str
    message: str = ''
    status: str = 'draft'
    audience: int = 0
    openRate: str = '-'
    clickRate: str = '-'
    date: str = '-'
    created: datetime = Field(default_factory=_now)


class MessageIn(BaseModel):
    text: str


class MessageModel(BaseModel):
    id: str = Field(default_factory=_id)
    from_: str = Field(alias='from')  # 'me' or 'contact'
    text: str
    time: str = ''

    model_config = {'populate_by_name': True}


class Conversation(BaseModel):
    id: str = Field(default_factory=_id)
    user_id: str
    contact: Dict[str, Any]
    messages: List[Dict[str, Any]] = []
    lastMessage: str = ''
    time: str = 'now'
    unread: int = 0
    created: datetime = Field(default_factory=_now)
