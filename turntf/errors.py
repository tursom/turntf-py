class TurntfError(Exception):
    pass


class ClosedError(TurntfError):
    def __init__(self) -> None:
        super().__init__("turntf client is closed")


class NotConnectedError(TurntfError):
    def __init__(self) -> None:
        super().__init__("turntf client is not connected")


class DisconnectedError(TurntfError):
    def __init__(self) -> None:
        super().__init__("turntf websocket disconnected")


class ServerError(TurntfError):
    def __init__(self, code: str, message: str, request_id: int = 0) -> None:
        self.code = code
        self.server_message = message
        self.request_id = request_id
        if request_id == 0:
            super().__init__(f"turntf server error: {code} ({message})")
        else:
            super().__init__(f"turntf server error: {code} ({message}), request_id={request_id}")

    def unauthorized(self) -> bool:
        return self.code == "unauthorized"


class ProtocolError(TurntfError):
    def __init__(self, message: str) -> None:
        self.protocol_message = message
        super().__init__(f"turntf protocol error: {message}")


class ConnectionError(TurntfError):
    def __init__(self, op: str, cause: BaseException) -> None:
        self.op = op
        self.cause = cause
        super().__init__(f"turntf connection error during {op}: {cause}")
