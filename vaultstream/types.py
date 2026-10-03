"""VaultStream 同步清单库的公开数据类型。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class FileState:
    """文件的完整状态。内容哈希由上游保证有效。"""

    name: str
    content_hash: str


@dataclass(frozen=True)
class ChangeRecord:
    """一条净变化记录。

    deleted 为 True 表示文件在终点时刻不存在；否则 name/content_hash
    为终点时刻的完整状态。seq 为该文件截至终点 H 的最后一次提交序号，
    记录按它升序排列。
    """

    file_id: str
    seq: int
    deleted: bool
    name: Optional[str] = None
    content_hash: Optional[str] = None


@dataclass(frozen=True)
class ChangesPage:
    """一页变化结果。

    非末页：cursor 非空、checkpoint 为 None。
    末页：cursor 为 None、checkpoint 为本会话封定的终点 H。
    """

    records: tuple[ChangeRecord, ...]
    cursor: Optional[str]
    checkpoint: Optional[int]
