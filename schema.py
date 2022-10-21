import datetime
from typing import Optional
from pydantic import BaseModel, Field


class UserGet(BaseModel):
    id: int
    age: int
    country: str
    city: str
    exp_group: int
    gender: int
    source: str
    os: str

    class Config:
        orm_mode = True


class PostGet(BaseModel):
    id: int
    text: str
    topic: str

    class Config:
        orm_mode = True


class FeedGet(BaseModel):
    action: str
    post_id: int
    time: datetime.datetime
    user_id: int

    user: Optional["UserGet"] = None
    post: Optional["PostGet"] = None

    class Config:
        orm_mode = True
