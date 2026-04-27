from __future__ import annotations

from dataclasses import dataclass

import bcrypt


@dataclass(slots=True, frozen=True)
class PasswordInput:
    source: str
    encoded: str

    def validate(self) -> None:
        if self.source not in {"plain", "hashed"}:
            raise ValueError(f"invalid password source {self.source!r}")
        if self.encoded == "":
            raise ValueError("password is required")

    def wire_value(self) -> str:
        self.validate()
        return self.encoded

    def is_hashed(self) -> bool:
        return self.encoded != ""

    def is_zero(self) -> bool:
        return self.source == "" and self.encoded == ""


def hash_password(plain: str) -> str:
    if plain == "":
        raise ValueError("password is required")
    hashed = bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt())
    return hashed.decode("utf-8")


def plain_password(plain: str) -> PasswordInput:
    return PasswordInput(source="plain", encoded=hash_password(plain))


def hashed_password(value: str) -> PasswordInput:
    return PasswordInput(source="hashed", encoded=value)
