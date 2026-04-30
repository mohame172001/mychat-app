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
    instagramProfilePictureUrl: Optional[str] = None
    instagramConnectionValid: bool = False
    instagramAccountType: Optional[str] = None
    activeInstagramAccountId: Optional[str] = None
    activeInstagramIgUserId: Optional[str] = None


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
    match: Optional[str] = None
    keyword: Optional[str] = None
    mode: Optional[str] = None
    comment_reply: Optional[str] = None
    comment_reply_2: Optional[str] = None
    comment_reply_3: Optional[str] = None
    dm_text: Optional[str] = None
    media_id: Optional[str] = None
    latest: Optional[bool] = None
    media_preview: Optional[Dict[str, Any]] = None
    instagramAccountId: Optional[str] = None
    igUserId: Optional[str] = None
    instagramUsername: Optional[str] = None
    follow_request_enabled: Optional[bool] = None
    follow_request_message: Optional[str] = None
    follow_request_button_text: Optional[str] = None
    follow_confirmation_keywords: Optional[List[str]] = None
    follow_gate_expires_after_minutes: Optional[int] = None
    follow_gate_fallback_message: Optional[str] = None
    verify_actual_follow: Optional[bool] = None
    follow_not_detected_message: Optional[str] = None
    follow_verification_failed_message: Optional[str] = None
    follow_retry_button_text: Optional[str] = None
    follow_cooldown_message: Optional[str] = None
    max_follow_verification_attempts: Optional[int] = None
    processExistingComments: bool = False
    process_existing_unreplied_comments: Optional[bool] = None
    processExistingUnrepliedComments: Optional[bool] = None


class AutomationPatch(BaseModel):
    name: Optional[str] = None
    trigger: Optional[str] = None
    status: Optional[str] = None
    nodes: Optional[List[Dict[str, Any]]] = None
    edges: Optional[List[Dict[str, Any]]] = None
    match: Optional[str] = None
    keyword: Optional[str] = None
    mode: Optional[str] = None
    comment_reply: Optional[str] = None
    comment_reply_2: Optional[str] = None
    comment_reply_3: Optional[str] = None
    dm_text: Optional[str] = None
    media_id: Optional[str] = None
    latest: Optional[bool] = None
    media_preview: Optional[Dict[str, Any]] = None
    instagramAccountId: Optional[str] = None
    igUserId: Optional[str] = None
    instagramUsername: Optional[str] = None
    keywords: Optional[List[str]] = None
    post_scope: Optional[str] = None
    reply_under_post: Optional[bool] = None
    opening_dm_enabled: Optional[bool] = None
    opening_dm_text: Optional[str] = None
    opening_dm_button_text: Optional[str] = None
    link_dm_text: Optional[str] = None
    link_button_text: Optional[str] = None
    link_url: Optional[str] = None
    conversionTrackingEnabled: Optional[bool] = None
    follow_request_enabled: Optional[bool] = None
    follow_request_message: Optional[str] = None
    follow_request_button_text: Optional[str] = None
    follow_confirmation_keywords: Optional[List[str]] = None
    follow_gate_expires_after_minutes: Optional[int] = None
    follow_gate_fallback_message: Optional[str] = None
    followGateEnabled: Optional[bool] = None
    followGateMessage: Optional[str] = None
    followGateButtonText: Optional[str] = None
    followGateConfirmationKeywords: Optional[List[str]] = None
    followGateExpiresAfterMinutes: Optional[int] = None
    followGateFallbackMessage: Optional[str] = None
    verify_actual_follow: Optional[bool] = None
    verifyActualFollow: Optional[bool] = None
    follow_not_detected_message: Optional[str] = None
    followNotDetectedMessage: Optional[str] = None
    follow_verification_failed_message: Optional[str] = None
    followVerificationFailedMessage: Optional[str] = None
    follow_retry_button_text: Optional[str] = None
    followRetryButtonText: Optional[str] = None
    follow_cooldown_message: Optional[str] = None
    followCooldownMessage: Optional[str] = None
    max_follow_verification_attempts: Optional[int] = None
    maxFollowVerificationAttempts: Optional[int] = None
    process_existing_unreplied_comments: Optional[bool] = None
    processExistingUnrepliedComments: Optional[bool] = None
    email_request_enabled: Optional[bool] = None
    follow_up_enabled: Optional[bool] = None
    follow_up_text: Optional[str] = None
    processExistingComments: Optional[bool] = None


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
    match: Optional[str] = None
    keyword: Optional[str] = None
    mode: Optional[str] = None
    comment_reply: Optional[str] = None
    comment_reply_2: Optional[str] = None
    comment_reply_3: Optional[str] = None
    dm_text: Optional[str] = None
    media_id: Optional[str] = None
    latest: Optional[bool] = None
    media_preview: Optional[Dict[str, Any]] = None
    instagramAccountId: Optional[str] = None
    igUserId: Optional[str] = None
    instagramUsername: Optional[str] = None
    follow_request_enabled: Optional[bool] = None
    follow_request_message: Optional[str] = None
    follow_request_button_text: Optional[str] = None
    follow_confirmation_keywords: Optional[List[str]] = None
    follow_gate_expires_after_minutes: Optional[int] = None
    follow_gate_fallback_message: Optional[str] = None
    verify_actual_follow: Optional[bool] = None
    follow_not_detected_message: Optional[str] = None
    follow_verification_failed_message: Optional[str] = None
    follow_retry_button_text: Optional[str] = None
    follow_cooldown_message: Optional[str] = None
    max_follow_verification_attempts: Optional[int] = None
    processExistingComments: bool = False
    process_existing_unreplied_comments: bool = False
    activationStartedAt: Optional[datetime] = None
    createdAt: datetime = Field(default_factory=_now)
    updatedAt: datetime = Field(default_factory=_now)
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


class DmRuleIn(BaseModel):
    name: str
    keyword: str
    matchMode: str = 'contains'  # exact | contains | starts_with
    replyText: str
    isActive: bool = True


class DmRulePatch(BaseModel):
    name: Optional[str] = None
    keyword: Optional[str] = None
    matchMode: Optional[str] = None
    replyText: Optional[str] = None
    isActive: Optional[bool] = None


class DmTestIn(BaseModel):
    text: str


class Conversation(BaseModel):
    id: str = Field(default_factory=_id)
    user_id: str
    contact: Dict[str, Any]
    messages: List[Dict[str, Any]] = []
    lastMessage: str = ''
    time: str = 'now'
    unread: int = 0
    created: datetime = Field(default_factory=_now)
