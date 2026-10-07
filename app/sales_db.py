"""Transactional Sales CRUD with an explicitly supplied connection factory.

This module never imports app.db/config, creates schema, or sends messages.
The existing application represents connected shops by coffee_shops.id and
filters their relations by is_active. Accordingly, creating/transferring a
CONNECTED lead requires an active linked shop. A later shop deletion may leave
that historical lead unlinked through the migration's ON DELETE SET NULL FK.
"""

from collections.abc import Callable
from enum import Enum

from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb
from pydantic import ValidationError

from app.sales_contacts import normalize_instagram, normalize_phone, normalize_website
from app.sales_models import (
    FollowupBucket, SalesEventCreate, SalesLeadCreate, SalesLeadUpdate, SalesStatus,
)


class SalesError(Exception):
    def __init__(self, code: str, status_code: int, message: str, extra: dict | None = None):
        self.code = code
        self.status_code = status_code
        self.message = message
        self.extra = extra or {}
        super().__init__(message)


_CONTACT_FIELDS = {
    "phone": ("phone_normalized", normalize_phone),
    "website": ("website_normalized", normalize_website),
    "instagram": ("instagram_normalized", normalize_instagram),
}
_ACTIVE_FOLLOWUP = "next_followup_at IS NOT NULL AND status NOT IN ('CONNECTED','REJECTED','DO_NOT_CONTACT')"
_KYIV_DATE = "(next_followup_at AT TIME ZONE 'Europe/Kyiv')::date"
_TODAY_KYIV = "(statement_timestamp() AT TIME ZONE 'Europe/Kyiv')::date"
_FOLLOWUP_PREDICATES = {
    "overdue": f"{_KYIV_DATE} < {_TODAY_KYIV}",
    "today": f"{_KYIV_DATE} = {_TODAY_KYIV}",
    "upcoming": f"{_KYIV_DATE} > {_TODAY_KYIV}",
}


def _values(model, *, exclude_unset=False):
    return {
        key: value.value if isinstance(value, Enum) else value
        for key, value in model.model_dump(exclude_unset=exclude_unset).items()
    }


def _validated(model, data, code):
    try:
        return model.model_validate(data)
    except ValidationError as exc:
        # Pydantic contexts may hold ValueError objects, which are not JSON data.
        errors = [{"loc": list(error["loc"]), "type": error["type"], "msg": error["msg"]}
                  for error in exc.errors()]
        raise SalesError(code, 422, "Перевірте поля форми.", {"errors": errors}) from exc


def _identity(value):
    return " ".join((value or "").split()).casefold()


class SalesRepository:
    """Use psycopg dict-row connections, supplied by application wiring/tests."""

    def __init__(self, connection_factory: Callable):
        self.connection_factory = connection_factory

    @staticmethod
    def _pagination(limit, offset):
        if (isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100
                or isinstance(offset, bool) or not isinstance(offset, int) or offset < 0):
            raise SalesError("INVALID_PAGINATION", 422, "Некоректні параметри сторінки.")

    @staticmethod
    def _bucket(value):
        if value is None:
            return None
        try:
            return FollowupBucket(value).value
        except ValueError as exc:
            raise SalesError("INVALID_FOLLOWUP", 422, "Оберіть overdue, today або upcoming.") from exc

    @staticmethod
    def _lead(cur, lead_id, *, lock=False):
        lock_sql = " FOR SHARE" if lock == "share" else " FOR UPDATE" if lock else ""
        cur.execute("SELECT * FROM sales_leads WHERE id=%s" + lock_sql, (lead_id,))
        row = cur.fetchone()
        if row is None:
            raise SalesError("LEAD_NOT_FOUND", 404, "Лід не знайдено.")
        return dict(row)

    @staticmethod
    def _events(cur, lead_id):
        cur.execute(
            "SELECT * FROM sales_lead_events WHERE lead_id=%s ORDER BY created_at DESC,id DESC",
            (lead_id,),
        )
        return [dict(row) for row in cur.fetchall()]

    @staticmethod
    def _event(cur, lead_id, actor_user_id, event_type, *, from_status=None,
               to_status=None, note=None, metadata=None):
        cur.execute(
            """INSERT INTO sales_lead_events
               (lead_id,event_type,actor_user_id,from_status,to_status,note,metadata)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (lead_id, event_type, actor_user_id, from_status, to_status, note,
             Jsonb(metadata) if metadata is not None else None),
        )

    @staticmethod
    def _normalize(data):
        for field, (normalized_field, normalizer) in _CONTACT_FIELDS.items():
            if field in data:
                data[normalized_field] = normalizer(data[field])

    @staticmethod
    def _validate_link(cur, status, shop_id, *, required):
        if status == "CONNECTED" and required and shop_id is None:
            raise SalesError("CONNECTED_SHOP_REQUIRED", 422, "Оберіть підключену кавʼярню.")
        if shop_id is not None:
            # A share lock keeps the shop active/existing through this transaction.
            cur.execute("SELECT id FROM coffee_shops WHERE id=%s AND is_active IS TRUE FOR SHARE", (shop_id,))
            if cur.fetchone() is None:
                raise SalesError("CONNECTED_SHOP_INVALID", 422, "Підключена кавʼярня не існує або неактивна.")

    def _locked_update_lead(self, cur, lead_id, data):
        """Acquire shop before lead, matching shop deletion's FK lock order.

        The second read is authoritative. If a concurrent lead update changed
        which shop needs validation, ask for a retry before writing anything.
        """
        candidate = self._lead(cur, lead_id)
        candidate_status = data.get("status", candidate["status"])
        candidate_shop = data.get("connected_shop_id", candidate["connected_shop_id"])
        validated = "connected_shop_id" in data or (
            candidate_status == "CONNECTED" and candidate["status"] != "CONNECTED"
        )
        if validated:
            self._validate_link(cur, candidate_status, candidate_shop, required=True)
        current = self._lead(cur, lead_id, lock=True)
        target_status = data.get("status", current["status"])
        target_shop = data.get("connected_shop_id", current["connected_shop_id"])
        needs_validation = "connected_shop_id" in data or (
            target_status == "CONNECTED" and current["status"] != "CONNECTED"
        )
        if needs_validation and (
            not validated or candidate_shop != target_shop
            or (candidate_status == "CONNECTED") != (target_status == "CONNECTED")
        ):
            raise SalesError("LEAD_CHANGED_RETRY", 409, "Лід змінився. Оновіть сторінку й повторіть дію.")
        return current

    def _duplicate_warnings(self, cur, lead):
        warnings = []
        for field, (normalized_field, _) in _CONTACT_FIELDS.items():
            value = lead.get(normalized_field)
            if value is None:
                continue
            # Column names originate solely from this module's allowlist.
            cur.execute(
                f"SELECT id,name FROM sales_leads WHERE {normalized_field}=%s AND id<>%s ORDER BY id",
                (value, lead["id"]),
            )
            for row in cur.fetchall():
                warnings.append({"code": "POSSIBLE_DUPLICATE", "field": field, "value": value,
                                 "lead_id": row["id"], "lead_name": row["name"]})

        # The real shop schema has Instagram and identity fields, but does not
        # have phone/site/place_id. Never query or fabricate those absent fields.
        instagram = lead.get("instagram_normalized")
        identity = tuple(_identity(lead.get(field)) for field in ("name", "city", "address"))
        # The core PostgreSQL coffee_shops table does not require an Instagram
        # column (the web panel keeps profile fields separately). Preserve the
        # optional match when that column exists, but never make a Sales update
        # fail with UndefinedColumn on installations that do not have it.
        cur.execute(
            """SELECT EXISTS(
                       SELECT 1 FROM information_schema.columns
                       WHERE table_schema=current_schema()
                         AND table_name='coffee_shops'
                         AND column_name='instagram'
                   ) AS has_instagram"""
        )
        has_shop_instagram = bool(cur.fetchone()["has_instagram"])
        if all(identity) or (instagram is not None and has_shop_instagram):
            columns = "id,name,city,address" + (",instagram" if has_shop_instagram else "")
            cur.execute(f"SELECT {columns} FROM coffee_shops WHERE is_active IS TRUE ORDER BY id")
            for row in cur.fetchall():
                if instagram is not None and has_shop_instagram and normalize_instagram(row["instagram"]) == instagram:
                    field, value = "instagram", instagram
                elif all(identity) and identity == tuple(_identity(row[field]) for field in ("name", "city", "address")):
                    field, value = "identity", " | ".join(identity)
                else:
                    continue
                warnings.append({"code": "CONNECTED_SHOP_MATCH", "field": field, "value": value,
                                 "shop_id": row["id"], "shop_name": row["name"]})
        return warnings

    @staticmethod
    def _duplicate_place_error(cur, place_id):
        cur.execute("SELECT id,name FROM sales_leads WHERE place_id=%s", (place_id,))
        row = cur.fetchone()
        extra = {"place_id": place_id}
        if row:
            extra.update({"lead_id": row["id"], "lead_name": row["name"]})
        return SalesError("DUPLICATE_PLACE_ID", 409, "Лід із таким place_id вже існує.", extra)

    def list_leads(self, q=None, status=None, city=None, followup=None, limit=20, offset=0):
        self._pagination(limit, offset)
        predicates, parameters = [], []
        if q:
            pattern = "%" + q.strip().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
            columns = ("name", "city", "address", "phone", "email", "instagram", "telegram", "website")
            predicates.append("(" + " OR ".join(f"{column} ILIKE %s" for column in columns) + ")")
            parameters.extend([pattern] * len(columns))
        if status is not None:
            try:
                status = SalesStatus(status).value
            except ValueError as exc:
                raise SalesError("INVALID_STATUS", 422, "Некоректний статус ліда.") from exc
            predicates.append("status=%s")
            parameters.append(status)
        if city:
            predicates.append("lower(btrim(city))=lower(%s)")
            parameters.append(city.strip())
        bucket = self._bucket(followup)
        if bucket is not None:
            predicates.extend((_ACTIVE_FOLLOWUP, _FOLLOWUP_PREDICATES[bucket]))
        where = " WHERE " + " AND ".join(predicates) if predicates else ""
        with self.connection_factory() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS total FROM sales_leads" + where, parameters)
                total = cur.fetchone()["total"]
                cur.execute("SELECT * FROM sales_leads" + where + " ORDER BY updated_at DESC,id DESC LIMIT %s OFFSET %s",
                            [*parameters, limit, offset])
                return {"items": [dict(row) for row in cur.fetchall()], "total": total, "limit": limit, "offset": offset}

    def get_lead(self, lead_id):
        with self.connection_factory() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    # Every timeline writer first locks the lead. A short share
                    # lock keeps the returned lead and its history consistent.
                    lead = self._lead(cur, lead_id, lock="share")
                    return {"lead": lead, "events": self._events(cur, lead_id)}

    def create_lead(self, data: dict, actor_user_id: int):
        data = _values(_validated(SalesLeadCreate, data, "INVALID_LEAD"))
        self._normalize(data)
        with self.connection_factory() as conn:
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        self._validate_link(cur, data["status"], data["connected_shop_id"], required=True)
                        if data["place_id"] is not None:
                            cur.execute("SELECT id FROM sales_leads WHERE place_id=%s", (data["place_id"],))
                            if cur.fetchone() is not None:
                                raise self._duplicate_place_error(cur, data["place_id"])
                        columns = [*data, "created_by_user_id", "updated_by_user_id"]
                        values = [*data.values(), actor_user_id, actor_user_id]
                        contact_sql = ",last_contact_at" if data["status"] == "CONTACTED" else ""
                        contact_value = ",statement_timestamp()" if contact_sql else ""
                        cur.execute(
                            f"INSERT INTO sales_leads ({','.join(columns)}{contact_sql}) "
                            f"VALUES ({','.join(['%s'] * len(columns))}{contact_value}) RETURNING *", values,
                        )
                        lead = dict(cur.fetchone())
                        self._event(cur, lead["id"], actor_user_id, "CREATED",
                                    to_status=lead["status"], note=lead["notes"])
                        if lead["status"] == "CONTACTED":
                            self._event(cur, lead["id"], actor_user_id, "CONTACTED", to_status=lead["status"])
                        if lead["next_followup_at"] is not None:
                            self._event(cur, lead["id"], actor_user_id, "FOLLOWUP_SET",
                                        metadata={"next_followup_at": lead["next_followup_at"].isoformat()})
                        if lead["connected_shop_id"] is not None:
                            self._event(cur, lead["id"], actor_user_id, "CONNECTED_TO_SHOP",
                                        metadata={"connected_shop_id": lead["connected_shop_id"]})
                        warnings = self._duplicate_warnings(cur, lead)
                        if warnings:
                            self._event(cur, lead["id"], actor_user_id, "DUPLICATE_WARNING", metadata={"warnings": warnings})
                        return {"lead": lead, "events": self._events(cur, lead["id"]), "duplicate_warnings": warnings}
            except UniqueViolation as exc:
                if exc.diag.constraint_name != "sales_leads_place_id_unique_idx":
                    raise
                # The explicit transaction has rolled back before this lookup.
                with conn.cursor() as cur:
                    raise self._duplicate_place_error(cur, data["place_id"]) from exc

    def update_lead(self, lead_id, data: dict, actor_user_id: int):
        data = _values(_validated(SalesLeadUpdate, data, "INVALID_LEAD"), exclude_unset=True)
        self._normalize(data)
        with self.connection_factory() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    previous = self._locked_update_lead(cur, lead_id, data)
                    target_status = data.get("status", previous["status"])
                    if previous["status"] == "DO_NOT_CONTACT" and target_status == "CONTACTED":
                        raise SalesError("CONTACT_FORBIDDEN", 409, "Контакт заборонено.")
                    changed = {key: value for key, value in data.items() if value != previous[key]}
                    if not changed:
                        return {"lead": previous, "events": self._events(cur, lead_id), "duplicate_warnings": []}
                    is_contacted = "status" in changed and target_status == "CONTACTED"
                    assignments = [f"{key}=%s" for key in changed]
                    assignments.extend(("updated_at=statement_timestamp()", "updated_by_user_id=%s"))
                    if is_contacted:
                        assignments.append("last_contact_at=statement_timestamp()")
                    cur.execute("UPDATE sales_leads SET " + ",".join(assignments) + " WHERE id=%s RETURNING *",
                                [*changed.values(), actor_user_id, lead_id])
                    lead = dict(cur.fetchone())
                    if "status" in changed:
                        self._event(cur, lead_id, actor_user_id, "STATUS_CHANGED",
                                    from_status=previous["status"], to_status=lead["status"])
                    if is_contacted:
                        self._event(cur, lead_id, actor_user_id, "CONTACTED",
                                    from_status=previous["status"], to_status=lead["status"])
                    if "next_followup_at" in changed:
                        followup = lead["next_followup_at"]
                        self._event(cur, lead_id, actor_user_id, "FOLLOWUP_SET",
                                    metadata={"next_followup_at": followup.isoformat() if followup else None})
                    if "connected_shop_id" in changed:
                        self._event(cur, lead_id, actor_user_id, "CONNECTED_TO_SHOP",
                                    metadata={"connected_shop_id": lead["connected_shop_id"],
                                              "previous_shop_id": previous["connected_shop_id"]})
                    ordinary_fields = sorted(set(changed) - {"status", "next_followup_at", "connected_shop_id"}
                                             - {item[0] for item in _CONTACT_FIELDS.values()})
                    if ordinary_fields:
                        self._event(cur, lead_id, actor_user_id, "UPDATED",
                                    note=lead["notes"] if "notes" in changed else None,
                                    metadata={"changed_fields": ordinary_fields})
                    warnings = self._duplicate_warnings(cur, lead) if set(changed) & {
                        "phone", "website", "instagram", "name", "city", "address"
                    } else []
                    if warnings:
                        self._event(cur, lead_id, actor_user_id, "DUPLICATE_WARNING", metadata={"warnings": warnings})
                    return {"lead": lead, "events": self._events(cur, lead_id), "duplicate_warnings": warnings}

    def add_event(self, lead_id, data: dict, actor_user_id: int):
        data = _values(_validated(SalesEventCreate, data, "INVALID_EVENT"))
        if data["event_type"] == "NOTE_ADDED" and not data["note"]:
            raise SalesError("NOTE_REQUIRED", 422, "Додайте текст нотатки.")
        with self.connection_factory() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    previous = self._lead(cur, lead_id, lock=True)
                    contacted = data["event_type"] == "CONTACTED"
                    if contacted and previous["status"] == "DO_NOT_CONTACT":
                        raise SalesError("CONTACT_FORBIDDEN", 409, "Контакт заборонено.")
                    target_status = "CONTACTED" if contacted and previous["status"] in {"NEW", "NOT_CONTACTED"} else previous["status"]
                    cur.execute(
                        "UPDATE sales_leads SET updated_at=statement_timestamp(),updated_by_user_id=%s,status=%s"
                        + (",last_contact_at=statement_timestamp()" if contacted else "")
                        + " WHERE id=%s RETURNING *", (actor_user_id, target_status, lead_id),
                    )
                    lead = dict(cur.fetchone())
                    if target_status != previous["status"]:
                        self._event(cur, lead_id, actor_user_id, "STATUS_CHANGED",
                                    from_status=previous["status"], to_status=target_status)
                    self._event(cur, lead_id, actor_user_id, data["event_type"],
                                from_status=previous["status"] if contacted else None,
                                to_status=target_status if contacted else None,
                                note=data["note"], metadata=data["metadata"])
                    return {"lead": lead, "events": self._events(cur, lead_id), "duplicate_warnings": []}

    def followups(self, bucket=None, limit=20, offset=0):
        self._pagination(limit, offset)
        bucket = self._bucket(bucket)
        where = " WHERE " + _ACTIVE_FOLLOWUP
        if bucket is not None:
            where += " AND " + _FOLLOWUP_PREDICATES[bucket]
        with self.connection_factory() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS total FROM sales_leads" + where)
                total = cur.fetchone()["total"]
                cur.execute("SELECT * FROM sales_leads" + where + " ORDER BY next_followup_at,id LIMIT %s OFFSET %s",
                            (limit, offset))
                return {"items": [dict(row) for row in cur.fetchall()], "total": total, "limit": limit, "offset": offset}

    def stats(self):
        statuses = [status.value for status in SalesStatus]
        columns = ["COUNT(*) AS total"]
        columns.extend(f"COUNT(*) FILTER (WHERE status='{status}') AS {status.lower()}" for status in statuses)
        columns.extend((
            f"COUNT(*) FILTER (WHERE {_ACTIVE_FOLLOWUP} AND {_FOLLOWUP_PREDICATES['today']}) AS followups_today",
            f"COUNT(*) FILTER (WHERE {_ACTIVE_FOLLOWUP} AND {_FOLLOWUP_PREDICATES['overdue']}) AS followups_overdue",
        ))
        with self.connection_factory() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT " + ",".join(columns) + " FROM sales_leads")
                return dict(cur.fetchone())
