"""Validated, community-only image storage.

Community media uses the existing public ``/uploads`` mount, but has its own
randomized subdirectory and upload code.  There is no endpoint that accepts a
path or URL from a client.
"""

from contextlib import contextmanager, suppress
import errno
import os
from pathlib import Path
import re
import stat
import uuid

from app.broadcast_media import validate_media


DEFAULT_COMMUNITY_UPLOAD_ROOT = Path("/data/uploads/community")
_MEDIA_KEY = re.compile(r"[0-9a-f]{32}\.(?:jpg|png)\Z")


class CommunityMediaStorage:
    def __init__(self, root: str | Path = DEFAULT_COMMUNITY_UPLOAD_ROOT):
        candidate = Path(root)
        if candidate.is_symlink():
            raise ValueError("Community upload root must not be a symlink")
        self.root = candidate.resolve()

    @contextmanager
    def _directory(self, *, create: bool = False):
        if create:
            self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            descriptor = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        except OSError as error:
            if error.errno in {errno.ELOOP, errno.ENOTDIR}:
                raise ValueError("Invalid community upload directory") from error
            raise
        try:
            if create:
                os.fchmod(descriptor, 0o700)
            yield descriptor
        finally:
            os.close(descriptor)

    def save(self, media: dict) -> str:
        data = media.get("bytes")
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise ValueError("Invalid community media")
        validated = validate_media("photo", media.get("filename") or "photo", bytes(data))
        extension = ".jpg" if validated["mime_type"] == "image/jpeg" else ".png"
        key = uuid.uuid4().hex + extension
        temporary_key = key + ".tmp"
        with self._directory(create=True) as directory:
            created = False
            published = False
            try:
                descriptor = os.open(
                    temporary_key,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=directory,
                )
                created = True
                with os.fdopen(descriptor, "wb") as stream:
                    os.fchmod(stream.fileno(), 0o600)
                    stream.write(validated["bytes"])
                    stream.flush()
                    os.fsync(stream.fileno())
                os.link(temporary_key, key, src_dir_fd=directory, dst_dir_fd=directory,
                        follow_symlinks=False)
                published = True
                os.unlink(temporary_key, dir_fd=directory)
                os.fsync(directory)
            except Exception:
                if published:
                    with suppress(FileNotFoundError):
                        os.unlink(key, dir_fd=directory)
                raise
            finally:
                if created:
                    with suppress(FileNotFoundError):
                        os.unlink(temporary_key, dir_fd=directory)
        return f"/uploads/community/{key}"

    def delete(self, url: str) -> bool:
        prefix = "/uploads/community/"
        if not isinstance(url, str) or not url.startswith(prefix):
            return False
        key = url[len(prefix):]
        if _MEDIA_KEY.fullmatch(key) is None:
            return False
        try:
            with self._directory() as directory:
                info = os.stat(key, dir_fd=directory, follow_symlinks=False)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    return False
                os.unlink(key, dir_fd=directory)
                os.fsync(directory)
        except FileNotFoundError:
            return False
        return True

    def read(self, url: str) -> bytes | None:
        """Read our own regular asset only; never fetch client-provided URLs."""
        prefix = "/uploads/community/"
        if not isinstance(url, str) or not url.startswith(prefix):
            raise ValueError("Invalid community image reference")
        key = url[len(prefix):]
        if _MEDIA_KEY.fullmatch(key) is None:
            raise ValueError("Invalid community image reference")
        try:
            with self._directory() as directory:
                descriptor = os.open(key, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
                with os.fdopen(descriptor, "rb") as stream:
                    info = os.fstat(stream.fileno())
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                        raise ValueError("Invalid community image file")
                    from app.broadcast_media import PHOTO_LIMIT
                    data = stream.read(PHOTO_LIMIT + 1)
                    return validate_media("photo", key, data)["bytes"]
        except FileNotFoundError:
            return None
