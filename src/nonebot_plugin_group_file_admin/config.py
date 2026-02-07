from pydantic import BaseModel, field_validator
from typing import Optional
from nonebot import get_plugin_config
import json
import ast

class Config(BaseModel):
    fa_del_model: int = 1
    fa_expand_name: Optional[list] = None
    fa_white_group_list: Optional[list[int]] = None
    fa_white_folder_list: Optional[list[str]] = None
    fa_backup_interval: int = 86400 # Default 24 hours

    @field_validator("fa_expand_name", "fa_white_group_list", "fa_white_folder_list", mode="before")
    @classmethod
    def parse_list(cls, v):
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                try:
                    return ast.literal_eval(v)
                except (ValueError, SyntaxError):
                    pass
        return v

plugin_config = get_plugin_config(Config)