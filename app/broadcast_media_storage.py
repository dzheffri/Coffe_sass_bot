"""Private broadcast media on the existing /data Railway volume.

No public mount, DB writes or delivery integration. Import/initialization performs
no filesystem writes. Pass a private temporary root for local tests.
"""

from contextlib import contextmanager, suppress
import errno
import os
from pathlib import Path
import re
import stat
import uuid

from fastapi import HTTPException

from app.broadcast_media import PHOTO_LIMIT, VIDEO_LIMIT, validate_media


PRIVATE_MEDIA_ROOT = Path("/data/broadcast_media")
PUBLIC_UPLOADS_ROOT = Path("/data/uploads")
_KEY_PATTERN = re.compile(r"[0-9a-f]{32}\.(jpg|png|mp4|mov)")


class BroadcastMediaStorage:
    def __init__(self, root: str | Path = PRIVATE_MEDIA_ROOT):
        candidate = Path(root)
        if candidate.is_symlink():
            raise ValueError("Media root must not be a symlink")
        self.root = candidate.resolve()
        if self.root.is_relative_to(PUBLIC_UPLOADS_ROOT.resolve()):
            raise ValueError("Broadcast media must not use public uploads")

    @staticmethod
    def _kind_and_limit(media_key: str) -> tuple[str, int]:
        if not isinstance(media_key, str) or not _KEY_PATTERN.fullmatch(media_key):
            raise ValueError("Invalid media key")
        return ("photo", PHOTO_LIMIT) if media_key.endswith((".jpg", ".png")) else ("video", VIDEO_LIMIT)

    @contextmanager
    def _directory(self, *, create=False):
        if create:
            self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            descriptor = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        except OSError as error:
            if error.errno in {errno.ELOOP, errno.ENOTDIR}:
                raise ValueError("Invalid media directory") from error
            raise
        try:
            if create:
                os.fchmod(descriptor, 0o700)
            yield descriptor
        finally:
            os.close(descriptor)

    def save(self, media: dict) -> dict:
        """Accept the existing claimed native preview's media dictionary.

        Returned media_type/media_key can later be stored on broadcasts. Neither
        the supplied MIME/size nor the original filename is trusted as a path.
        """
        data = media.get("bytes")
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise HTTPException(422, detail={"code": "INVALID_MEDIA"})
        validated = validate_media(media.get("kind"), media.get("filename") or "attachment", bytes(data))
        extension = Path(validated["filename"]).suffix
        media_key = uuid.uuid4().hex + extension
        temporary_key = media_key + ".tmp"
        temporary_created = False
        published = False
        with self._directory(create=True) as directory:
            try:
                descriptor = os.open(temporary_key, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                     0o600, dir_fd=directory)
                temporary_created = True
                with os.fdopen(descriptor, "wb") as stream:
                    os.fchmod(stream.fileno(), 0o600)
                    stream.write(validated["bytes"])
                    stream.flush()
                    os.fsync(stream.fileno())
                # link publishes the complete file atomically and cannot replace
                # another asset, including in the unlikely event of a UUID clash.
                os.link(temporary_key, media_key, src_dir_fd=directory, dst_dir_fd=directory,
                        follow_symlinks=False)
                published = True
                os.unlink(temporary_key, dir_fd=directory)
                os.fsync(directory)
            except Exception:
                if published:
                    with suppress(FileNotFoundError):
                        os.unlink(media_key, dir_fd=directory)
                raise
            finally:
                if temporary_created:
                    with suppress(FileNotFoundError):
                        os.unlink(temporary_key, dir_fd=directory)
        return {"media_type": validated["kind"], "media_key": media_key,
                "mime_type": validated["mime_type"], "size_bytes": validated["size_bytes"]}

    def get(self, media_key: str) -> dict | None:
        kind, limit = self._kind_and_limit(media_key)
        try:
            with self._directory() as directory:
                try:
                    descriptor = os.open(media_key, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                                         dir_fd=directory)
                except OSError as error:
                    if error.errno == errno.ELOOP:
                        raise ValueError("Invalid media file") from error
                    raise
                try:
                    info = os.fstat(descriptor)
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                        raise ValueError("Invalid media file")
                    if info.st_size > limit:
                        raise HTTPException(413, detail={"code": "MEDIA_TOO_LARGE"})
                except Exception:
                    os.close(descriptor)
                    raise
                with os.fdopen(descriptor, "rb") as stream:
                    data = stream.read(limit + 1)
        except FileNotFoundError:
            return None
        media = validate_media(kind, media_key, data)
        if media["filename"] != media_key:
            raise ValueError("Stored media format does not match its key")
        return media

    def delete(self, media_key: str) -> bool:
        self._kind_and_limit(media_key)
        try:
            with self._directory() as directory:
                info = os.stat(media_key, dir_fd=directory, follow_symlinks=False)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise ValueError("Invalid media file")
                os.unlink(media_key, dir_fd=directory)
                os.fsync(directory)
        except FileNotFoundError:
            return False
        return True
