from __future__ import annotations

from dataclasses import dataclass

import bcrypt


@dataclass(slots=True, frozen=True)
class PasswordInput:
    """密码输入对象，表示一个密码值的包装。

    支持两种密码来源：
    - ``plain``: 明文密码，构造时会自动进行 bcrypt 哈希处理
    - ``hashed``: 已哈希的密码，直接使用传入的哈希值

    Attributes:
        source: 密码来源，取值 "plain" 或 "hashed"。
        encoded: 编码后的密码值（明文密码经过 bcrypt 哈希后的结果，
                 或已哈希密码的原始哈希值）。
    """
    source: str
    encoded: str

    def validate(self) -> None:
        """验证密码输入是否有效。

        检查 source 字段是否为 "plain" 或 "hashed"，
        且 encoded 字段不能为空字符串。

        Raises:
            ValueError: 如果 source 无效或 encoded 为空字符串。
        """
        if self.source not in {"plain", "hashed"}:
            raise ValueError(f"invalid password source {self.source!r}")
        if self.encoded == "":
            raise ValueError("password is required")

    def wire_value(self) -> str:
        """获取用于网络传输的密码值（即经过编码后的字符串）。

        在调用此方法前会自动执行 validate() 验证。

        Returns:
            编码后的密码字符串。

        Raises:
            ValueError: 如果密码输入无效。
        """
        self.validate()
        return self.encoded

    def is_hashed(self) -> bool:
        """判断密码是否已编码（非空）。

        Returns:
            如果 encoded 不为空字符串则返回 True。
        """
        return self.encoded != ""

    def is_zero(self) -> bool:
        """判断密码是否为空值（source 和 encoded 均为空）。

        Returns:
            如果 source 和 encoded 均为空字符串则返回 True。
        """
        return self.source == "" and self.encoded == ""


def hash_password(plain: str) -> str:
    """使用 bcrypt 算法对明文密码进行哈希处理。

    Args:
        plain: 明文密码字符串。

    Returns:
        bcrypt 哈希后的密码字符串（包含盐值）。

    Raises:
        ValueError: 如果 plain 为空字符串。
    """
    if plain == "":
        raise ValueError("password is required")
    hashed = bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt())
    return hashed.decode("utf-8")


def plain_password(plain: str) -> PasswordInput:
    """从明文密码创建 PasswordInput 对象。

    自动对明文密码进行 bcrypt 哈希处理，
    并将 source 标记为 "plain"。

    Args:
        plain: 明文密码字符串。

    Returns:
        包含哈希后密码的 PasswordInput 实例。
    """
    return PasswordInput(source="plain", encoded=hash_password(plain))


def hashed_password(value: str) -> PasswordInput:
    """从已哈希的密码值创建 PasswordInput 对象。

    直接使用传入的哈希值，不再进行哈希处理，
    并将 source 标记为 "hashed"。

    Args:
        value: 已哈希的密码字符串（bcrypt 格式）。

    Returns:
        PasswordInput 实例，source 为 "hashed"。
    """
    return PasswordInput(source="hashed", encoded=value)
