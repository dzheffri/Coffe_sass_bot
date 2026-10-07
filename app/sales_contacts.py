"""Conservative contact normalization for duplicate hints, never delivery."""

import ipaddress
import re
from urllib.parse import unquote, urlsplit


def normalize_phone(value: str | None) -> str | None:
    """Recognize Ukrainian numbers and explicit international E.164 numbers.

    Ambiguous local numbers, extensions and non-phone text stay unnormalized;
    callers retain the original value independently.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    compact = re.sub(r"[\s()\-]", "", value.strip())
    if compact.startswith("00380"):
        compact = "+" + compact[2:]
    if re.fullmatch(r"0[1-9]\d{8}", compact):
        return "+38" + compact
    if re.fullmatch(r"380[1-9]\d{8}", compact):
        return "+" + compact
    if re.fullmatch(r"\+380[1-9]\d{8}", compact):
        return compact
    if compact.startswith("+380"):
        return None
    if re.fullmatch(r"\+[1-9]\d{7,14}", compact):
        return compact
    return None


def normalize_website(value: str | None) -> str | None:
    """Return a lower-case, IDNA host without www, path, port or query.

    Only HTTP(S) URLs and bare public-looking domains are accepted. Credentials,
    malformed ports, IP addresses and local/ambiguous hosts are not guessed.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    if any(character.isspace() for character in raw) or "\\" in raw:
        return None
    candidate = raw if "://" in raw else "https://" + raw
    try:
        parsed = urlsplit(candidate)
        if parsed.scheme.lower() not in {"http", "https"}:
            return None
        if parsed.username is not None or parsed.password is not None:
            return None
        # Accessing port also validates invalid or out-of-range values.
        parsed.port
        host = parsed.hostname
        if not host:
            return None
        host = host.rstrip(".").encode("idna").decode("ascii").lower()
    except (ValueError, UnicodeError):
        return None
    if host.startswith("www."):
        host = host[4:]
    try:
        ipaddress.ip_address(host)
        return None
    except ValueError:
        pass
    labels = host.split(".")
    if len(host) > 253 or len(labels) < 2:
        return None
    if any(not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
           for label in labels):
        return None
    if len(labels[-1]) < 2 or labels[-1].isdigit():
        return None
    return host


def normalize_instagram(value: str | None) -> str | None:
    """Accept a username, @username or an Instagram profile URL only."""
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    if "://" in raw or raw.lower().startswith(("instagram.com/", "www.instagram.com/")):
        try:
            parsed = urlsplit(raw if "://" in raw else "https://" + raw)
            if (parsed.scheme.lower() not in {"http", "https"}
                    or parsed.hostname not in {"instagram.com", "www.instagram.com"}
                    or parsed.username is not None or parsed.password is not None
                    or parsed.port is not None):
                return None
            segments = [unquote(part) for part in parsed.path.split("/") if part]
            if len(segments) != 1:
                return None
            raw = segments[0]
        except ValueError:
            return None
    raw = raw.removeprefix("@").lower()
    if raw in {"p", "reel", "reels", "stories", "explore", "accounts", "direct"}:
        return None
    if not re.fullmatch(r"[a-z0-9_](?:[a-z0-9_.]{0,28}[a-z0-9_])?", raw):
        return None
    if ".." in raw:
        return None
    return raw
