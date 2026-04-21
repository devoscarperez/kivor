from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=3)
    password: str = Field(..., min_length=3)

class LoginUsernameRequest(BaseModel):
    username: str = Field(..., min_length=3)

class LogoutSessionRequest(BaseModel):
    session_id: str = Field(..., min_length=1)
