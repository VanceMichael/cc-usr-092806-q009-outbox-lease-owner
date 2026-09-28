"""实体标识与请求键。"""

from __future__ import annotations

import re
import uuid

from .errors import ValidationError

SAFE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


def new_id(prefix: str) -> str:
    if not SAFE.fullmatch(prefix):
        raise ValidationError("标识前缀不合法")
    return f"{prefix}:{uuid.uuid4().hex}"


def require_safe(value: str, label: str = "标识") -> str:
    if not isinstance(value, str) or not SAFE.fullmatch(value.strip()):
        raise ValidationError(f"{label}不合法")
    return value.strip()
