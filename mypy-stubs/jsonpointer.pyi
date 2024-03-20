from typing import (
    Any,
    Iterable,
    Iterator,
    Optional,
    Sequence,
    Tuple,
    Union,
)

__version__: str
__website__: str

def set_pointer(doc: Any, pointer: str, value: Any, inplace: bool = True) -> Any: ...
def resolve_pointer(doc: Any, pointer: str, default: Any = ...) -> Any: ...
def pairwise(iterable: Iterable[Any]) -> Iterator[Tuple[Any, Any]]: ...

class JsonPointerException(Exception): ...

class EndOfList:
    list_: Any
    def __init__(self, list_: Any) -> None: ...

class JsonPointer:
    parts: Sequence[str]
    def __init__(self, pointer: str) -> None: ...
    def to_last(self, doc: Any) -> Tuple[Any, Optional[Union[str, int]]]: ...
    def resolve(self, doc: Any, default: Any = ...) -> Any: ...
    def set(self, doc: Any, value: Any, inplace: bool = True) -> Any: ...
    @classmethod
    def get_part(cls, doc: Any, part: str) -> Union[str, int]: ...
    def get_parts(self) -> Sequence[str]: ...
    def walk(self, doc: Any, part: str) -> Any: ...
    def contains(self, ptr: JsonPointer) -> bool: ...
    def __contains__(self, item: JsonPointer) -> bool: ...
    def join(self, suffix: Union[JsonPointer, str, Sequence[str]]) -> JsonPointer: ...
    def __truediv__(
        self, suffix: Union[JsonPointer, str, Sequence[str]]
    ) -> JsonPointer: ...
    @property
    def path(self) -> str: ...
    def __eq__(self, other: Any) -> bool: ...
    def __hash__(self) -> int: ...
    @classmethod
    def from_parts(cls, parts: Sequence[str]) -> JsonPointer: ...

def escape(s: str) -> str: ...
def unescape(s: str) -> str: ...
