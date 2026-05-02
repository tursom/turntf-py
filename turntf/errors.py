class TurntfError(Exception):
    """所有 turntf 异常的基类。

    所有由 SDK 抛出的自定义异常都继承自此类，
    方便上层代码统一捕获和处理 turntf 相关的错误。
    """
    pass


class ClosedError(TurntfError):
    """客户端已被关闭时抛出的异常。

    当对已关闭的 AsyncClient 执行操作时（如调用 connect、send_message 等），
    会抛出此异常。客户端关闭后需要重新创建实例才能继续使用。
    """

    def __init__(self) -> None:
        super().__init__("turntf client is closed")


class NotConnectedError(TurntfError):
    """客户端未连接到服务器时抛出的异常。

    当客户端尚未成功建立 WebSocket 连接，但尝试执行
    需要连接的操作时抛出此异常。
    """

    def __init__(self) -> None:
        super().__init__("turntf client is not connected")


class DisconnectedError(TurntfError):
    """WebSocket 连接已断开时抛出的异常。

    当客户端原本已连接，但由于网络问题或服务器主动关闭
    导致连接中断时抛出此异常。如果配置了自动重连，客户端
    会尝试重新建立连接。
    """

    def __init__(self) -> None:
        super().__init__("turntf websocket disconnected")


class ServerError(TurntfError):
    """服务器返回错误时抛出的异常。

    当服务器在处理客户端请求时返回错误响应时抛出。
    包含服务器返回的错误码、错误信息和对应的请求 ID。

    Attributes:
        code: 服务器定义的错误码，如 "unauthorized"。
        server_message: 服务器返回的详细错误描述。
        request_id: 导致错误的请求 ID，0 表示非请求相关的错误。
    """

    def __init__(self, code: str, message: str, request_id: int = 0) -> None:
        self.code = code
        self.server_message = message
        self.request_id = request_id
        if request_id == 0:
            super().__init__(f"turntf server error: {code} ({message})")
        else:
            super().__init__(f"turntf server error: {code} ({message}), request_id={request_id}")

    def unauthorized(self) -> bool:
        """判断错误是否为未授权错误。

        Returns:
            如果错误码为 "unauthorized" 返回 True，否则返回 False。
        """
        return self.code == "unauthorized"


class ProtocolError(TurntfError):
    """协议错误时抛出的异常。

    当客户端收到无法解析或不符合协议预期的服务器响应时抛出。
    可能的原因包括：无效的 protobuf 帧、响应中缺少必要字段、
    不支持的服务器消息类型等。

    Attributes:
        protocol_message: 描述协议错误详情的消息文本。
    """

    def __init__(self, message: str) -> None:
        self.protocol_message = message
        super().__init__(f"turntf protocol error: {message}")


class ConnectionError(TurntfError):
    """网络连接错误时抛出的异常。

    当底层网络通信（如 WebSocket 连接、HTTP 请求）发生异常时抛出。
    通常由网络不稳定、服务器不可达或连接超时等原因引起。

    Attributes:
        op: 发生错误时的操作描述，如 "dial"、"write"、"read" 或 HTTP 方法路径。
        cause: 导致此异常的原始异常对象。
    """

    def __init__(self, op: str, cause: BaseException) -> None:
        self.op = op
        self.cause = cause
        super().__init__(f"turntf connection error during {op}: {cause}")
