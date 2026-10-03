"""VaultStream 同步清单库的异常类型。"""


class VaultStreamError(Exception):
    """库内所有异常的基类。"""


class CheckpointTooOldError(VaultStreamError):
    """请求开启会话的 checkpoint 低于当前允许下限。"""


class InvalidCheckpointError(VaultStreamError):
    """checkpoint 非法（负数或超过当前最大提交序号）。"""


class InvalidCursorError(VaultStreamError):
    """游标格式非法或对应会话不存在。"""


class CursorExpiredError(VaultStreamError):
    """游标对应会话已超过有效期。"""


class CompactFloorError(VaultStreamError):
    """compact 的下限非法（回退或超过当前最大提交序号）。"""
