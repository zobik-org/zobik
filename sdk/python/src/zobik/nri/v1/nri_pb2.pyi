from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class LogicMessage(_message.Message):
    __slots__ = ("hello", "received")
    HELLO_FIELD_NUMBER: _ClassVar[int]
    RECEIVED_FIELD_NUMBER: _ClassVar[int]
    hello: Hello
    received: Received
    def __init__(self, hello: _Optional[_Union[Hello, _Mapping]] = ..., received: _Optional[_Union[Received, _Mapping]] = ...) -> None: ...

class Hello(_message.Message):
    __slots__ = ("minor",)
    MINOR_FIELD_NUMBER: _ClassVar[int]
    minor: int
    def __init__(self, minor: _Optional[int] = ...) -> None: ...

class Received(_message.Message):
    __slots__ = ("delivery_id",)
    DELIVERY_ID_FIELD_NUMBER: _ClassVar[int]
    delivery_id: str
    def __init__(self, delivery_id: _Optional[str] = ...) -> None: ...

class Accepted(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SidecarMessage(_message.Message):
    __slots__ = ("accepted", "assignment", "result", "notice", "inbound")
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    ASSIGNMENT_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    NOTICE_FIELD_NUMBER: _ClassVar[int]
    INBOUND_FIELD_NUMBER: _ClassVar[int]
    accepted: Accepted
    assignment: Delivery
    result: Delivery
    notice: Delivery
    inbound: Inbound
    def __init__(self, accepted: _Optional[_Union[Accepted, _Mapping]] = ..., assignment: _Optional[_Union[Delivery, _Mapping]] = ..., result: _Optional[_Union[Delivery, _Mapping]] = ..., notice: _Optional[_Union[Delivery, _Mapping]] = ..., inbound: _Optional[_Union[Inbound, _Mapping]] = ...) -> None: ...

class Inbound(_message.Message):
    __slots__ = ("delivery_id", "slot", "operation", "fields", "expects_reply", "bodies")
    DELIVERY_ID_FIELD_NUMBER: _ClassVar[int]
    SLOT_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    EXPECTS_REPLY_FIELD_NUMBER: _ClassVar[int]
    BODIES_FIELD_NUMBER: _ClassVar[int]
    delivery_id: str
    slot: str
    operation: str
    fields: bytes
    expects_reply: bool
    bodies: _containers.RepeatedCompositeFieldContainer[InboundBody]
    def __init__(self, delivery_id: _Optional[str] = ..., slot: _Optional[str] = ..., operation: _Optional[str] = ..., fields: _Optional[bytes] = ..., expects_reply: _Optional[bool] = ..., bodies: _Optional[_Iterable[_Union[InboundBody, _Mapping]]] = ...) -> None: ...

class InboundBody(_message.Message):
    __slots__ = ("path", "name", "media_type", "context")
    PATH_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    MEDIA_TYPE_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    path: str
    name: str
    media_type: str
    context: bool
    def __init__(self, path: _Optional[str] = ..., name: _Optional[str] = ..., media_type: _Optional[str] = ..., context: _Optional[bool] = ...) -> None: ...

class Delivery(_message.Message):
    __slots__ = ("delivery_id", "event", "announcement")
    DELIVERY_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_FIELD_NUMBER: _ClassVar[int]
    ANNOUNCEMENT_FIELD_NUMBER: _ClassVar[int]
    delivery_id: str
    event: bytes
    announcement: bytes
    def __init__(self, delivery_id: _Optional[str] = ..., event: _Optional[bytes] = ..., announcement: _Optional[bytes] = ...) -> None: ...

class PublishRequest(_message.Message):
    __slots__ = ("task_id", "announce", "complete", "fail", "ask")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    ANNOUNCE_FIELD_NUMBER: _ClassVar[int]
    COMPLETE_FIELD_NUMBER: _ClassVar[int]
    FAIL_FIELD_NUMBER: _ClassVar[int]
    ASK_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    announce: AnnounceTask
    complete: CompleteTask
    fail: FailTask
    ask: AskRequester
    def __init__(self, task_id: _Optional[str] = ..., announce: _Optional[_Union[AnnounceTask, _Mapping]] = ..., complete: _Optional[_Union[CompleteTask, _Mapping]] = ..., fail: _Optional[_Union[FailTask, _Mapping]] = ..., ask: _Optional[_Union[AskRequester, _Mapping]] = ...) -> None: ...

class AnnounceTask(_message.Message):
    __slots__ = ("topic", "data", "previous_attempt")
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    topic: str
    data: bytes
    previous_attempt: str
    def __init__(self, topic: _Optional[str] = ..., data: _Optional[bytes] = ..., previous_attempt: _Optional[str] = ...) -> None: ...

class CompleteTask(_message.Message):
    __slots__ = ("data", "delivery_id")
    DATA_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_ID_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    delivery_id: str
    def __init__(self, data: _Optional[bytes] = ..., delivery_id: _Optional[str] = ...) -> None: ...

class FailTask(_message.Message):
    __slots__ = ("failure_reason", "data", "failure_code")
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    FAILURE_CODE_FIELD_NUMBER: _ClassVar[int]
    failure_reason: str
    data: bytes
    failure_code: str
    def __init__(self, failure_reason: _Optional[str] = ..., data: _Optional[bytes] = ..., failure_code: _Optional[str] = ...) -> None: ...

class AskRequester(_message.Message):
    __slots__ = ("data", "previous_attempt")
    DATA_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    previous_attempt: str
    def __init__(self, data: _Optional[bytes] = ..., previous_attempt: _Optional[str] = ...) -> None: ...

class PublishAck(_message.Message):
    __slots__ = ("task_id",)
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    def __init__(self, task_id: _Optional[str] = ...) -> None: ...

class ContextRef(_message.Message):
    __slots__ = ("trace_id", "task_id", "name")
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    trace_id: str
    task_id: str
    name: str
    def __init__(self, trace_id: _Optional[str] = ..., task_id: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class ContextRead(_message.Message):
    __slots__ = ("task_id", "ref")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    REF_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    ref: ContextRef
    def __init__(self, task_id: _Optional[str] = ..., ref: _Optional[_Union[ContextRef, _Mapping]] = ...) -> None: ...

class ContextWrite(_message.Message):
    __slots__ = ("header", "chunk")
    HEADER_FIELD_NUMBER: _ClassVar[int]
    CHUNK_FIELD_NUMBER: _ClassVar[int]
    header: ContextWriteHeader
    chunk: bytes
    def __init__(self, header: _Optional[_Union[ContextWriteHeader, _Mapping]] = ..., chunk: _Optional[bytes] = ...) -> None: ...

class ContextWriteHeader(_message.Message):
    __slots__ = ("task_id", "name")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    name: str
    def __init__(self, task_id: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class ContextList(_message.Message):
    __slots__ = ("task_id", "section")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    SECTION_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    section: str
    def __init__(self, task_id: _Optional[str] = ..., section: _Optional[str] = ...) -> None: ...

class ContextListing(_message.Message):
    __slots__ = ("refs",)
    REFS_FIELD_NUMBER: _ClassVar[int]
    refs: _containers.RepeatedCompositeFieldContainer[ContextRef]
    def __init__(self, refs: _Optional[_Iterable[_Union[ContextRef, _Mapping]]] = ...) -> None: ...

class Chunk(_message.Message):
    __slots__ = ("data",)
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    def __init__(self, data: _Optional[bytes] = ...) -> None: ...

class EgressRequest(_message.Message):
    __slots__ = ("task_id", "slot", "operation", "fields", "body")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    SLOT_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    slot: str
    operation: str
    fields: bytes
    body: str
    def __init__(self, task_id: _Optional[str] = ..., slot: _Optional[str] = ..., operation: _Optional[str] = ..., fields: _Optional[bytes] = ..., body: _Optional[str] = ...) -> None: ...

class EgressResponse(_message.Message):
    __slots__ = ("fields", "body")
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    fields: bytes
    body: str
    def __init__(self, fields: _Optional[bytes] = ..., body: _Optional[str] = ...) -> None: ...

class OpenTrace(_message.Message):
    __slots__ = ("header", "chunk")
    HEADER_FIELD_NUMBER: _ClassVar[int]
    CHUNK_FIELD_NUMBER: _ClassVar[int]
    header: OpenHeader
    chunk: bytes
    def __init__(self, header: _Optional[_Union[OpenHeader, _Mapping]] = ..., chunk: _Optional[bytes] = ...) -> None: ...

class OpenHeader(_message.Message):
    __slots__ = ("delivery_id", "data", "workspace")
    DELIVERY_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    delivery_id: str
    data: bytes
    workspace: str
    def __init__(self, delivery_id: _Optional[str] = ..., data: _Optional[bytes] = ..., workspace: _Optional[str] = ...) -> None: ...

class OpenAck(_message.Message):
    __slots__ = ("trace_id", "entry_task_id")
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    ENTRY_TASK_ID_FIELD_NUMBER: _ClassVar[int]
    trace_id: str
    entry_task_id: str
    def __init__(self, trace_id: _Optional[str] = ..., entry_task_id: _Optional[str] = ...) -> None: ...

class PurgeRequest(_message.Message):
    __slots__ = ("task_id",)
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    def __init__(self, task_id: _Optional[str] = ...) -> None: ...

class PurgeAck(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class IngressReply(_message.Message):
    __slots__ = ("delivery_id", "fields")
    DELIVERY_ID_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    delivery_id: str
    fields: bytes
    def __init__(self, delivery_id: _Optional[str] = ..., fields: _Optional[bytes] = ...) -> None: ...

class ReplyAck(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
