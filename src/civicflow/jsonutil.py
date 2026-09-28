"""确定性 JSON 与摘要工具。"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any

from .errors import ValidationError


def _default(value: Any) -> str:
    if isinstance(value, Decimal):
        return format(value, "f")
    raise TypeError(type(value).__name__)


def canonical_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_default)
    except (TypeError, ValueError) as exc:
        raise ValidationError("内容不能转换为稳定 JSON") from exc


def digest_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def parse_object(raw: str) -> dict:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValidationError("JSON 格式错误") from exc
    if not isinstance(value, dict):
        raise ValidationError("JSON 顶层必须是对象")
    return value
