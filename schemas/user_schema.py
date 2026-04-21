from pydantic import BaseModel, Field


class CreateUserRequest(BaseModel):
    user_nickname: str = Field(..., min_length=3)
    user_name: str = Field(..., min_length=3)
    user_password: str = Field(..., min_length=3)
    user_firstname: str = Field(..., min_length=2)
    user_lastname: str = Field(..., min_length=2)
    user_group_id: int
