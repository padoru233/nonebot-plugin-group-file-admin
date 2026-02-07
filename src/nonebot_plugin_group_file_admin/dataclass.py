from pydantic import BaseModel

class file_data(BaseModel):
    group_id: int
    file_id: str
    file_name: str
    busid: int
    size: int


class folders_data(BaseModel):
    group_id: int
    folder_id: str
    folder_name: str


class data(BaseModel):
    files: list[file_data]
    folders: list[folders_data]