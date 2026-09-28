"""无需联网依赖的最小构建后端。"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import os
import zipfile
from pathlib import Path


def _project_root() -> Path:
    return Path.cwd()


def build_wheel(wheel_directory: str, config_settings=None, metadata_directory=None) -> str:
    root = _project_root()
    output = Path(wheel_directory)
    output.mkdir(parents=True, exist_ok=True)
    filename = "civicflow-1.0.0-py3-none-any.whl"
    destination = output / filename
    records = []
    with zipfile.ZipFile(destination, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted((root / "src" / "civicflow").rglob("*.py")):
            relative = Path("civicflow") / path.relative_to(root / "src" / "civicflow")
            data = path.read_bytes()
            archive.writestr(relative.as_posix(), data)
            digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
            records.append((relative.as_posix(), f"sha256={digest}", str(len(data))))
        dist = Path("civicflow-1.0.0.dist-info")
        metadata = b"Metadata-Version: 2.1\nName: civicflow\nVersion: 1.0.0\n"
        wheel = b"Wheel-Version: 1.0\nGenerator: civicflow\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
        for name, data in ((dist / "METADATA", metadata), (dist / "WHEEL", wheel)):
            archive.writestr(name.as_posix(), data)
            digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
            records.append((name.as_posix(), f"sha256={digest}", str(len(data))))
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerows(records + [((dist / "RECORD").as_posix(), "", "")])
        archive.writestr((dist / "RECORD").as_posix(), buffer.getvalue())
    return filename


def build_editable(wheel_directory: str, config_settings=None, metadata_directory=None) -> str:
    return build_wheel(wheel_directory, config_settings, metadata_directory)


def build_sdist(sdist_directory: str, config_settings=None) -> str:
    raise RuntimeError("本项目使用 wheel 构建")
