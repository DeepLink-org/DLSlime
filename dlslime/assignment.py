from pydantic import BaseModel


class Assignment(BaseModel):
    mr_key: str
    target_offset: int
    source_offset: int
    length: int
