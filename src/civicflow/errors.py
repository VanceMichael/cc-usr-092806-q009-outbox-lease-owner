"""领域异常。"""

class CivicFlowError(Exception):
    """平台异常基类。"""

class ValidationError(CivicFlowError):
    """输入不满足领域约束。"""

class ConflictError(CivicFlowError):
    """版本、幂等键或状态发生冲突。"""

class NotFoundError(CivicFlowError):
    """目标记录不存在。"""

class PermissionDenied(CivicFlowError):
    """当前主体没有所需权限。"""

class InvariantViolation(CivicFlowError):
    """持久化状态破坏业务守恒。"""
