from ..utils import is_url as is_url, iso_now as iso_now
from .file_or_dir import FileOrDir as FileOrDir

class File(FileOrDir):
    def write(self, base_path: str) -> None: ...
