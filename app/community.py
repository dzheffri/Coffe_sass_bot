"""Database operations for the independent community feed."""

import json
from psycopg.errors import UniqueViolation


class CommunityError(Exception):
    def __init__(self, status_code: int, code: str, message: str | None = None):
        self.status_code = status_code
        self.code = code
        self.message = message or code
        super().__init__(self.message)


def _json_options(value) -> list[str]:
    if isinstance(value, str):
        value = json.loads(value)
    return list(value or [])


def _series_summary(row: dict) -> dict:
    return {
        "id": row["id"],
        "title": row["title"],
        "series_part": row["series_part"],
        "series_total": row["series_total"],
    }


class CommunityRepository:
    def __init__(self, connection):
        self.connection = connection

    def _post_row(self, post_id: int, *, published_only: bool = False, lock: bool = False):
        query = "SELECT * FROM community_posts WHERE id=%s AND deleted_at IS NULL"
        if published_only:
            query += " AND published_at IS NOT NULL AND publish_to_app=TRUE"
        if lock:
            query += " FOR UPDATE"
        row = self.connection.execute(query, (post_id,)).fetchone()
        if row is None:
            raise CommunityError(404, "COMMUNITY_POST_NOT_FOUND")
        return row

    def _serialized(self, row: dict, user_id: int, *, include_series: bool = False,
                    admin: bool = False) -> dict:
        from app.community_telegram import telegram_admin_status

        post_id = row["id"]
        options = _json_options(row["options"])
        is_poll = row["type"] == "poll"
        option_rows = []
        vote_option_id = None
        total_votes = 0
        if is_poll:
            vote_counts = self.connection.execute(
                """SELECT option_id, COUNT(*) AS votes
                   FROM community_poll_votes WHERE post_id=%s
                   GROUP BY option_id""", (post_id,)
            ).fetchall()
            counts = {int(item["option_id"]): int(item["votes"]) for item in vote_counts}
            option_rows = [{"id": index, "text": text, "votes": counts.get(index, 0)}
                           for index, text in enumerate(options, 1)]
            total_votes = sum(counts.values())
            own_vote = self.connection.execute(
                "SELECT option_id FROM community_poll_votes WHERE post_id=%s AND user_id=%s",
                (post_id, user_id),
            ).fetchone()
            vote_option_id = own_vote["option_id"] if own_vote else None

        reaction = self.connection.execute(
            """SELECT COALESCE(COUNT(*) FILTER (WHERE interested), 0) AS reaction_count,
                      COALESCE(BOOL_OR(interested) FILTER (WHERE user_id=%s), FALSE) AS interested
               FROM community_reactions WHERE post_id=%s""", (user_id, post_id),
        ).fetchone()
        series_posts = []
        if include_series and row["type"] == "series" and row["series_title"]:
            siblings = self.connection.execute(
                """SELECT id,title,series_part,series_total
                   FROM community_posts
                   WHERE type='series' AND LOWER(series_title)=LOWER(%s) AND published_at IS NOT NULL
                     AND deleted_at IS NULL AND publish_to_app=TRUE
                   ORDER BY series_part ASC, id ASC""", (row["series_title"],),
            ).fetchall()
            series_posts = [_series_summary(item) for item in siblings]
        return {
            "id": post_id,
            "type": row["type"],
            "title": row["title"],
            "text": row["text"],
            "image_url": row["image_url"],
            "created_at": row["created_at"],
            "published_at": row["published_at"],
            "is_published": row["published_at"] is not None,
            **({"publish_to_app": bool(row["publish_to_app"]),
                "publish_to_telegram": bool(row["publish_to_telegram"])} if admin else {}),
            "series_title": row["series_title"],
            "series_part": row["series_part"],
            "series_total": row["series_total"],
            "question": row["question"],
            "options": option_rows,
            "vote_option_id": vote_option_id,
            "total_votes": total_votes,
            "interested": bool(reaction["interested"]),
            "reaction_count": int(reaction["reaction_count"]),
            "series_posts": series_posts,
            **({"notification_attempted": bool(row["notification_attempted"])}
               if admin and "notification_attempted" in row else {}),
            **(telegram_admin_status(self.connection, row) if admin else {}),
        }

    def list_posts(self, user_id: int, *, limit: int, offset: int,
                   published_only: bool, admin: bool = False) -> dict:
        where = " WHERE deleted_at IS NULL"
        if published_only:
            where += " AND published_at IS NOT NULL AND publish_to_app=TRUE"
        total = self.connection.execute(
            "SELECT COUNT(*) AS total FROM community_posts" + where
        ).fetchone()["total"]
        order = "published_at DESC, id DESC" if published_only else "created_at DESC, id DESC"
        rows = self.connection.execute(
            f"SELECT * FROM community_posts{where} ORDER BY {order} LIMIT %s OFFSET %s",
            (limit, offset),
        ).fetchall()
        return {
            "items": [self._serialized(row, user_id, admin=admin) for row in rows],
            "total": int(total), "limit": limit, "offset": offset,
        }

    def get_post(self, post_id: int, user_id: int, *, published_only: bool,
                 include_series: bool = False, admin: bool = False) -> dict:
        row = self._post_row(post_id, published_only=published_only)
        return self._serialized(row, user_id, include_series=include_series, admin=admin)

    def create_post(self, data: dict, actor_user_id: int) -> dict:
        try:
            row = self.connection.execute(
                """INSERT INTO community_posts
                   (type,title,text,question,options,series_title,series_part,series_total,created_by_user_id,
                    publish_to_app,publish_to_telegram)
                   VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s) RETURNING *""",
                (data["type"], data["title"], data["text"], data.get("question"),
                 json.dumps(data.get("options", []), ensure_ascii=False), data.get("series_title"),
                 data.get("series_part"), data.get("series_total"), actor_user_id,
                 data.get("publish_to_app", True), data.get("publish_to_telegram", True)),
            ).fetchone()
        except UniqueViolation as exc:
            raise CommunityError(409, "SERIES_PART_ALREADY_EXISTS") from exc
        return self._serialized(row, actor_user_id, admin=True)

    def update_post(self, post_id: int, data: dict, actor_user_id: int) -> dict:
        row = self._post_row(post_id, lock=True)
        if row["published_at"] is not None:
            raise CommunityError(409, "COMMUNITY_POST_ALREADY_PUBLISHED")
        try:
            row = self.connection.execute(
                """UPDATE community_posts
                   SET type=%s,title=%s,text=%s,question=%s,options=%s::jsonb,
                       series_title=%s,series_part=%s,series_total=%s,
                       publish_to_app=%s,publish_to_telegram=%s
                   WHERE id=%s RETURNING *""",
                (data["type"], data["title"], data["text"], data.get("question"),
                 json.dumps(data.get("options", []), ensure_ascii=False), data.get("series_title"),
                 data.get("series_part"), data.get("series_total"),
                 data.get("publish_to_app", row["publish_to_app"]),
                 data.get("publish_to_telegram", row["publish_to_telegram"]), post_id),
            ).fetchone()
        except UniqueViolation as exc:
            raise CommunityError(409, "SERIES_PART_ALREADY_EXISTS") from exc
        return self._serialized(row, actor_user_id, admin=True)

    def attach_image(self, post_id: int, image_url: str, actor_user_id: int) -> dict:
        row = self._post_row(post_id, lock=True)
        if row["published_at"] is not None:
            raise CommunityError(409, "COMMUNITY_POST_ALREADY_PUBLISHED")
        row = self.connection.execute(
            "UPDATE community_posts SET image_url=%s WHERE id=%s RETURNING *",
            (image_url, post_id),
        ).fetchone()
        return self._serialized(row, actor_user_id, admin=True)

    def claim_publication(self, post_id: int, actor_user_id: int, notify: bool) -> tuple[dict, bool]:
        row = self._post_row(post_id, lock=True)
        if not row["publish_to_app"] and not row["publish_to_telegram"]:
            raise CommunityError(422, "COMMUNITY_DESTINATION_REQUIRED",
                                 "Оберіть хоча б одне місце публікації.")
        should_notify = row["published_at"] is None and bool(notify) and row["publish_to_app"]
        if row["published_at"] is None:
            row = self.connection.execute(
                """UPDATE community_posts
                   SET published_at=NOW(), notification_attempted=%s,telegram_status=%s
                   WHERE id=%s RETURNING *""",
                (should_notify, "pending" if row["publish_to_telegram"] else None, post_id),
            ).fetchone()
            if bool(notify) and row["publish_to_telegram"]:
                from app.community_telegram import queue_telegram_only_recipients
                queue_telegram_only_recipients(self.connection, post_id)
        return self._serialized(row, actor_user_id, admin=True), should_notify

    def delete_post(self, post_id: int) -> None:
        # Retain content, votes, reactions and media for safe recovery. Repeated
        # deletion keeps the original timestamp and never republishes the post.
        row = self.connection.execute(
            """UPDATE community_posts SET deleted_at=COALESCE(deleted_at, NOW())
               WHERE id=%s RETURNING id""", (post_id,),
        ).fetchone()
        if row is None:
            raise CommunityError(404, "COMMUNITY_POST_NOT_FOUND")

    def react(self, post_id: int, user_id: int, interested: bool) -> dict:
        self._post_row(post_id, published_only=True, lock=True)
        self.connection.execute(
            """INSERT INTO community_reactions(post_id,user_id,interested)
               VALUES(%s,%s,%s)
               ON CONFLICT(post_id,user_id) DO UPDATE SET interested=EXCLUDED.interested,
                   updated_at=NOW()""", (post_id, user_id, bool(interested)),
        )
        return self.get_post(post_id, user_id, published_only=True)

    def vote(self, post_id: int, user_id: int, option_id: int) -> dict:
        row = self._post_row(post_id, published_only=True, lock=True)
        if row["type"] != "poll":
            raise CommunityError(422, "NOT_A_POLL")
        options = _json_options(row["options"])
        if option_id < 1 or option_id > len(options):
            raise CommunityError(422, "INVALID_POLL_OPTION")
        try:
            self.connection.execute(
                "INSERT INTO community_poll_votes(post_id,user_id,option_id) VALUES(%s,%s,%s)",
                (post_id, user_id, option_id),
            )
        except UniqueViolation as exc:
            raise CommunityError(409, "POLL_ALREADY_VOTED") from exc
        return self.get_post(post_id, user_id, published_only=True)

    def enabled_push_devices(self) -> list[dict]:
        # Check catalog visibility first.  A missing optional push table must
        # never raise UndefinedTable inside the publication transaction: that
        # would mark the transaction aborted and roll back the publication.
        table = self.connection.execute(
            "SELECT to_regclass('app_push_devices') AS table_name"
        ).fetchone()
        if not table or table["table_name"] is None:
            return []
        # Registration already normalizes tokens, but normalizing legacy rows
        # here makes publication deduplication safe without exposing tokens.
        rows = self.connection.execute(
            """SELECT DISTINCT ON (LOWER(BTRIM(device_token)))
                      LOWER(BTRIM(device_token)) AS device_token, environment
               FROM app_push_devices
               WHERE notifications_enabled=TRUE AND platform='ios'
                 AND BTRIM(device_token) <> ''
               ORDER BY LOWER(BTRIM(device_token)), id"""
        ).fetchall()
        return list(rows)
