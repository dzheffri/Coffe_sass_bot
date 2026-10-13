"""Private media storage tests using local fixtures and temporary paths only.

No backend configuration, database, delivery providers or production media are
loaded. Public-root checks use a temporary analogue of /data/uploads.
"""

import errno
from inspect import signature
from io import BytesIO
from pathlib import Path
import stat
import uuid

from fastapi import HTTPException
from PIL import Image
import pytest

from app import broadcast_media_storage as storage_module
from app.broadcast_media import PHOTO_LIMIT, VIDEO_LIMIT, validate_media
from app.broadcast_media_storage import BroadcastMediaStorage


DECLARED_PUBLIC_ROOT = storage_module.PUBLIC_UPLOADS_ROOT
FORMATS = [
    ("jpg", "photo", "image/jpeg"),
    ("png", "photo", "image/png"),
    ("mp4", "video", "video/mp4"),
    ("mov", "video", "video/quicktime"),
]


@pytest.fixture(autouse=True)
def isolate_public_root_checks(tmp_path, monkeypatch):
    # Constructor path validation must never inspect the real /data tree in these tests.
    monkeypatch.setattr(storage_module, "PUBLIC_UPLOADS_ROOT", tmp_path / "public_uploads")


@pytest.fixture
def storage(tmp_path):
    return BroadcastMediaStorage(root=tmp_path / "private_media")


def photo_bytes(format="JPEG", color="brown"):
    stream = BytesIO()
    Image.new("RGB", (32, 32), color).save(stream, format=format)
    return stream.getvalue()


def iso_box(kind, payload):
    return (8 + len(payload)).to_bytes(4, "big") + kind + payload


def video_bytes(brand=b"isom", *, total_size=None):
    prefix = iso_box(b"ftyp", brand + b"\0" * 4 + brand) + iso_box(b"moov", b"")
    payload = b"test" if total_size is None else b"\0" * (total_size - len(prefix) - 8)
    return prefix + iso_box(b"mdat", payload)


def bytes_for(extension):
    if extension == "jpg":
        return photo_bytes()
    if extension == "png":
        return photo_bytes("PNG")
    return video_bytes(b"qt  " if extension == "mov" else b"isom")


def media(kind="photo", data=None, filename="original.jpg", **metadata):
    return {"kind": kind, "filename": filename,
            "bytes": photo_bytes() if data is None else data, **metadata}


def assert_uuid4_key(key, extension):
    assert key.endswith("." + extension)
    hex_part = key[:-(len(extension) + 1)]
    assert len(hex_part) == 32 and hex_part == hex_part.lower()
    parsed = uuid.UUID(hex=hex_part)
    assert parsed.hex == hex_part and parsed.version == 4 and parsed.variant == uuid.RFC_4122


@pytest.mark.parametrize("extension,kind,mime", FORMATS, ids=["jpeg", "png", "mp4", "mov"])
def test_save_get_delete_roundtrip_uses_validated_bytes_and_mime(storage, extension, kind, mime):
    data = bytes_for(extension)
    saved = storage.save(media(kind, data, filename="incorrect-extension.exe",
                               mime_type="application/x-untrusted", size_bytes=1))
    key = saved["media_key"]
    assert_uuid4_key(key, extension)
    assert saved == {"media_type": kind, "media_key": key,
                     "mime_type": mime, "size_bytes": len(data)}
    assert storage.get(key) == {"kind": kind, "filename": key, "mime_type": mime,
                                "size_bytes": len(data), "bytes": data}
    assert [path.name for path in storage.root.iterdir()] == [key]
    assert storage.delete(key) is True
    assert storage.get(key) is None
    assert storage.delete(key) is False
    assert list(storage.root.iterdir()) == []


@pytest.mark.parametrize("case,kind,status,code", [
    ("gif", "photo", 415, "UNSUPPORTED_MEDIA_TYPE"),
    ("corrupt-jpeg", "photo", 422, "INVALID_MEDIA"),
    ("corrupt-png", "photo", 422, "INVALID_MEDIA"),
    ("bad-dimensions", "photo", 422, "INVALID_MEDIA"),
    ("unknown-video", "video", 415, "UNSUPPORTED_MEDIA_TYPE"),
    ("truncated-video", "video", 422, "INVALID_MEDIA"),
    ("unsupported-brand", "video", 415, "UNSUPPORTED_MEDIA_TYPE"),
    ("unsupported-kind", "animation", 422, "INVALID_MEDIA"),
    ("empty", "photo", 422, "INVALID_MEDIA"),
])
def test_invalid_media_fails_without_publishing_files(storage, case, kind, status, code):
    data = {
        "gif": b"GIF89a",
        "corrupt-jpeg": b"\xff\xd8\xffinvalid",
        "corrupt-png": b"\x89PNG\r\n\x1a\ninvalid",
        "bad-dimensions": photo_bytes(),
        "unknown-video": b"not a video",
        "truncated-video": video_bytes()[:-2],
        "unsupported-brand": video_bytes(b"zzzz"),
        "unsupported-kind": photo_bytes(),
        "empty": b"",
    }[case]
    if case == "bad-dimensions":
        stream = BytesIO()
        Image.new("RGB", (10001, 1), "brown").save(stream, format="PNG")
        data = stream.getvalue()
    with pytest.raises(HTTPException) as error:
        storage.save(media(kind, data, mime_type="image/jpeg"))
    assert error.value.status_code == status and error.value.detail["code"] == code
    assert not storage.root.exists() or list(storage.root.iterdir()) == []


def bounded_valid_bytes(kind, size):
    if kind == "video":
        return video_bytes(total_size=size)
    jpeg = photo_bytes()
    return jpeg + b"\0" * (size - len(jpeg))


@pytest.mark.parametrize("kind,limit", [("photo", PHOTO_LIMIT), ("video", VIDEO_LIMIT)])
def test_exact_binary_limit_accepts_valid_media_and_preserves_original(storage, kind, limit):
    data = bounded_valid_bytes(kind, limit)
    assert len(data) == limit
    saved = storage.save(media(kind, data))
    assert saved["size_bytes"] == limit
    assert storage.get(saved["media_key"])["bytes"] == data


@pytest.mark.parametrize("kind,limit", [("photo", PHOTO_LIMIT), ("video", VIDEO_LIMIT)])
def test_binary_limit_plus_one_rejects_before_any_asset_is_written(storage, kind, limit):
    data = bounded_valid_bytes(kind, limit + 1)
    with pytest.raises(HTTPException) as error:
        storage.save(media(kind, data))
    assert error.value.status_code == 413 and error.value.detail["code"] == "MEDIA_TOO_LARGE"
    assert not storage.root.exists() or list(storage.root.iterdir()) == []


@pytest.mark.parametrize("buffer_type", [bytearray, memoryview], ids=["bytearray", "postgres-memoryview"])
def test_preview_binary_buffers_are_converted_to_bytes(storage, buffer_type):
    data = photo_bytes()
    saved = storage.save(media(data=buffer_type(data)))
    restored = storage.get(saved["media_key"])
    assert restored["bytes"] == data and type(restored["bytes"]) is bytes


def test_existing_claimed_native_preview_dictionary_can_be_saved_directly(storage):
    data = video_bytes()
    validated = validate_media("video", "native-preview.mov", data)
    # Exact media shape returned by admin_tools.claim_broadcast; unlike
    # multipart validation it carries no size_bytes field.
    preview = {key: validated[key] for key in ("kind", "filename", "mime_type", "bytes")}
    before = dict(preview)
    saved = storage.save(preview)
    assert preview == before
    assert saved["media_type"] == "video" and saved["mime_type"] == "video/mp4"
    assert storage.get(saved["media_key"])["bytes"] == data


@pytest.mark.parametrize("filename_kind", ["absolute", "parent", "windows", "control-characters", "empty"])
def test_hostile_original_filenames_never_choose_disk_paths(storage, tmp_path, filename_kind):
    outside = tmp_path / "must-not-overwrite.jpg"
    outside.write_bytes(b"unrelated file")
    filename = {
        "absolute": str(outside),
        "parent": "../must-not-overwrite.jpg",
        "windows": r"C:\private\must-not-overwrite.jpg",
        "control-characters": "../\0\n\rmust-not-overwrite.jpg",
        "empty": "",
    }[filename_kind]
    saved = storage.save(media(filename=filename))
    assert_uuid4_key(saved["media_key"], "jpg")
    assert outside.read_bytes() == b"unrelated file"
    assert [path.name for path in storage.root.iterdir()] == [saved["media_key"]]


@pytest.mark.parametrize("operation", ["get", "delete"])
@pytest.mark.parametrize("invalid_key", [
    "", "../outside.jpg", "nested/" + "a" * 32 + ".jpg",
    "..\\outside.jpg", "%2e%2e%2foutside.jpg", "not-a-uuid.jpg",
    "a" * 32 + ".gif", "a" * 32 + ".jpg\0", "a" * 32 + ".jpg.tmp",
])
def test_invalid_and_traversal_keys_are_rejected(storage, tmp_path, operation, invalid_key):
    outside = tmp_path / "outside.jpg"
    outside.write_bytes(b"unrelated file")
    with pytest.raises(ValueError):
        getattr(storage, operation)(invalid_key)
    with pytest.raises(ValueError):
        getattr(storage, operation)(str(outside))
    assert outside.read_bytes() == b"unrelated file"
    assert not storage.root.exists()


def test_missing_valid_key_is_none_for_get_and_false_for_delete(storage):
    missing = uuid.uuid4().hex + ".jpg"
    assert storage.get(missing) is None
    assert storage.delete(missing) is False


def test_equal_media_creates_independent_uuid4_assets(storage):
    data = photo_bytes()
    first, second = storage.save(media(data=data)), storage.save(media(data=data))
    assert first["media_key"] != second["media_key"]
    for saved in (first, second):
        assert_uuid4_key(saved["media_key"], "jpg")
        assert storage.get(saved["media_key"])["bytes"] == data
    assert storage.delete(first["media_key"]) is True
    assert storage.get(second["media_key"])["bytes"] == data


def test_media_persists_across_storage_instances(storage):
    data = photo_bytes("PNG")
    saved = storage.save(media(data=data))
    reopened = BroadcastMediaStorage(root=storage.root)
    assert reopened.get(saved["media_key"]) == {
        "kind": "photo", "filename": saved["media_key"], "mime_type": "image/png",
        "size_bytes": len(data), "bytes": data,
    }
    assert reopened.delete(saved["media_key"]) is True
    assert storage.get(saved["media_key"]) is None


def test_private_directory_and_files_have_owner_only_permissions(storage):
    saved = storage.save(media())
    assert stat.S_IMODE(storage.root.stat().st_mode) == 0o700
    assert stat.S_IMODE((storage.root / saved["media_key"]).stat().st_mode) == 0o600


@pytest.mark.parametrize("operation", ["get", "delete"])
def test_symlink_file_is_rejected_without_touching_its_target(storage, tmp_path, operation):
    storage.root.mkdir(mode=0o700)
    outside = tmp_path / "outside.jpg"
    data = photo_bytes()
    outside.write_bytes(data)
    before_mode = stat.S_IMODE(outside.stat().st_mode)
    key = uuid.uuid4().hex + ".jpg"
    link = storage.root / key
    link.symlink_to(outside)
    with pytest.raises(ValueError):
        getattr(storage, operation)(key)
    assert link.is_symlink()
    assert outside.read_bytes() == data
    assert stat.S_IMODE(outside.stat().st_mode) == before_mode


@pytest.mark.parametrize("operation", ["get", "delete"])
def test_directory_at_asset_key_is_rejected_and_preserved(storage, operation):
    key = uuid.uuid4().hex + ".jpg"
    directory = storage.root / key
    directory.mkdir(parents=True)
    marker = directory / "keep.txt"
    marker.write_text("not a media file")
    with pytest.raises(ValueError):
        getattr(storage, operation)(key)
    assert marker.read_text() == "not a media file"


def test_constructor_is_lazy_and_default_is_private(tmp_path, monkeypatch):
    assert Path(signature(BroadcastMediaStorage).parameters["root"].default) == Path("/data/broadcast_media")
    assert DECLARED_PUBLIC_ROOT == Path("/data/uploads")
    root = tmp_path / "not_created" / "private_media"

    def forbidden_creation(*args, **kwargs):
        raise AssertionError("Constructor must not create directories or files")

    with monkeypatch.context() as guard:
        guard.setattr(Path, "mkdir", forbidden_creation)
        guard.setattr(storage_module.os, "open", forbidden_creation)
        guard.setattr("builtins.open", forbidden_creation)
        BroadcastMediaStorage(root=root)
    assert not root.exists() and not root.parent.exists()


@pytest.mark.parametrize("suffix", ["", "nested", "nested/deeper", "../public_uploads/nested"])
def test_public_uploads_root_and_nested_paths_are_rejected_without_creation(tmp_path, suffix):
    public_root = tmp_path / "public_uploads"
    with pytest.raises(ValueError):
        BroadcastMediaStorage(root=public_root / suffix)
    assert not public_root.exists()


@pytest.mark.parametrize("failure_stage", ["file-sync", "publish", "directory-sync"])
def test_failed_save_cleans_temporary_files_and_preserves_existing_assets(storage, monkeypatch, failure_stage):
    original = photo_bytes()
    existing = storage.save(media(data=original))
    real_fsync = storage_module.os.fsync
    calls = 0

    def failing_sync(descriptor):
        nonlocal calls
        calls += 1
        if calls == (1 if failure_stage == "file-sync" else 2):
            raise OSError(errno.EIO, "simulated disk synchronization failure")
        return real_fsync(descriptor)

    def failing_publish(*args, **kwargs):
        raise OSError(errno.EIO, "simulated publication failure")

    if failure_stage == "publish":
        monkeypatch.setattr(storage_module.os, "link", failing_publish)
    else:
        monkeypatch.setattr(storage_module.os, "fsync", failing_sync)
    with pytest.raises(OSError):
        storage.save(media(data=photo_bytes(color="blue")))
    assert sorted(path.name for path in storage.root.iterdir()) == [existing["media_key"]]
    assert storage.get(existing["media_key"])["bytes"] == original


def test_uuid_collision_cannot_overwrite_existing_asset_and_cleans_temporary_file(storage, monkeypatch):
    fixed_uuid = uuid.UUID("456789ab-cdef-4123-8123-456789abcdef")
    monkeypatch.setattr(storage_module.uuid, "uuid4", lambda: fixed_uuid)
    original = photo_bytes()
    existing = storage.save(media(data=original))
    with pytest.raises(FileExistsError):
        storage.save(media(data=photo_bytes(color="blue")))
    assert [path.name for path in storage.root.iterdir()] == [existing["media_key"]]
    assert storage.get(existing["media_key"])["bytes"] == original
