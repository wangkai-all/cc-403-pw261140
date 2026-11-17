"""Rule parsing, validation and hashing.

The stored rules JSON is the *contract*: reports are only comparable when
their canonical rules hash matches.  Hashes are computed over a canonical
serialisation (sorted keys, no whitespace) so cosmetic edits to the database
file or to the editor payload never change the identity of a rule set.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .errors import ValidationError
from .models import FunnelRules, Step

MAX_STEPS = 24
MAX_WINDOW_MS = 90 * 24 * 3600 * 1000


def canonical_rules_payload(rules: FunnelRules) -> dict:
    return {
        "funnel_id": rules.funnel_id,
        "version": rules.version,
        "conversion_window_ms": rules.conversion_window_ms,
        "steps": [
            {
                "name": s.name,
                "event_name": s.event_name,
                "min_wait_ms": s.min_wait_ms,
                "max_wait_ms": s.max_wait_ms,
                "same_session": bool(s.same_session),
                "same_session_scope": s.same_session_scope,
                "count_repeats": bool(s.count_repeats),
                "description": s.description,
            }
            for s in rules.steps
        ],
    }


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def rules_hash(rules: FunnelRules) -> str:
    payload = canonical_rules_payload(rules)
    payload.pop("version", None)  # same rules == same analytics identity
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def chain_hash(previous: str, rules_json: str) -> str:
    h = hashlib.sha256()
    h.update(previous.encode("utf-8"))
    h.update(b"\n")
    h.update(rules_json.encode("utf-8"))
    return h.hexdigest()


def _as_int(value: Any, field: str, *, minimum: int | None = None) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{field} must be an integer number of milliseconds")
    n = int(value)
    if minimum is not None and n < minimum:
        raise ValidationError(f"{field} must be >= {minimum}")
    return n


def parse_rules(payload: dict, funnel_id: str, version: int) -> FunnelRules:
    """Validate an editor payload and turn it into a :class:`FunnelRules`."""

    if not isinstance(payload, dict):
        raise ValidationError("rules must be a JSON object")
    window = _as_int(payload.get("conversion_window_ms"), "conversion_window_ms", minimum=0)
    if window is None:
        raise ValidationError("conversion_window_ms is required")
    if window > MAX_WINDOW_MS:
        raise ValidationError(f"conversion_window_ms must be <= {MAX_WINDOW_MS}")

    raw_steps = payload.get("steps")
    if not isinstance(raw_steps, list) or not raw_steps:
        raise ValidationError("steps must be a non-empty array")
    if len(raw_steps) > MAX_STEPS:
        raise ValidationError(f"a funnel may have at most {MAX_STEPS} steps")

    steps: list[Step] = []
    seen_names: set[str] = set()
    for index, raw in enumerate(raw_steps):
        if not isinstance(raw, dict):
            raise ValidationError(f"step {index + 1} must be an object")
        label = str(raw.get("name") or "").strip()
        if not label:
            raise ValidationError(f"step {index + 1} needs a name")
        # Duplicate labels are allowed on purpose: a repeated step is
        # expressed as two adjacent steps naming the same event.  A later
        # version may rename either one freely; identity is positional.
        seen_names.add(label)

        event_name = raw.get("event_name")
        if event_name is not None:
            event_name = str(event_name).strip()
            if not event_name:
                raise ValidationError(f"step '{label}' has an empty event_name")
        if index == 0 and event_name is None:
            raise ValidationError("the first step must name an entry event")
        if index > 0 and event_name is None:
            raise ValidationError(
                f"step '{label}' needs an event_name; only the first step may be 'any event'"
            )

        min_wait = _as_int(raw.get("min_wait_ms"), f"step '{label}' min_wait_ms", minimum=0)
        max_wait = _as_int(raw.get("max_wait_ms"), f"step '{label}' max_wait_ms", minimum=0)
        if min_wait is not None and max_wait is not None and min_wait > max_wait:
            raise ValidationError(f"step '{label}': min_wait_ms exceeds max_wait_ms")
        if max_wait is not None and max_wait > MAX_WINDOW_MS:
            raise ValidationError(f"step '{label}': max_wait_ms too large")

        scope = str(raw.get("same_session_scope") or "session").strip()
        if scope not in ("session", "user"):
            raise ValidationError(f"step '{label}': unknown same_session_scope '{scope}'")

        steps.append(
            Step(
                name=label,
                event_name=event_name,
                min_wait_ms=min_wait,
                max_wait_ms=max_wait,
                same_session=bool(raw.get("same_session", False)),
                same_session_scope=scope,
                count_repeats=bool(raw.get("count_repeats", False)),
                description=str(raw.get("description") or ""),
            )
        )

    rules = FunnelRules(
        funnel_id=funnel_id,
        version=version,
        conversion_window_ms=window,
        steps=steps,
        note=str(payload.get("note") or ""),
    )
    rules.rules_hash = rules_hash(rules)
    return rules


def rules_from_stored(stored: dict, funnel_id: str, version: int) -> FunnelRules:
    """Re-hydrate rules from the exact stored version payload."""

    body = dict(stored)
    body.pop("version", None)
    rules = parse_rules(body, funnel_id, version)
    rules.created_at_ms = int(stored.get("created_at_ms") or 0)
    return rules


def preview_diff(old: FunnelRules | None, new: FunnelRules) -> list[str]:
    """Human-readable differences the editor shows next to a preview."""

    out: list[str] = []
    if old is None:
        out.append("first version of this funnel")
        return out
    if old.conversion_window_ms != new.conversion_window_ms:
        out.append(
            f"conversion window {old.conversion_window_ms} ms -> {new.conversion_window_ms} ms"
        )
    old_steps = [
        (s.name, s.event_name, s.min_wait_ms or 0, s.max_wait_ms,
         s.same_session, s.same_session_scope, s.count_repeats)
        for s in old.steps
    ]
    new_steps = [
        (s.name, s.event_name, s.min_wait_ms or 0, s.max_wait_ms,
         s.same_session, s.same_session_scope, s.count_repeats)
        for s in new.steps
    ]
    for i, (o, n) in enumerate(zip(old_steps, new_steps), start=1):
        if o == n:
            continue
        if o[0] != n[0]:
            out.append(f"step {i}: renamed '{o[0]}' -> '{n[0]}'")
        elif o[1] != n[1]:
            out.append(f"step {i}: event '{o[1]}' -> '{n[1]}'")
        elif o[2] != n[2] or o[3] != n[3]:
            out.append(
                f"step {i} wait [{o[2]}, {o[3]}] -> [{n[2]}, {n[3]}] ms"
            )
        else:
            flags = []
            if o[4] != n[4] or o[5] != n[5]:
                flags.append(f"same_session {'on' if n[4] else 'off'}")
            if o[6] != n[6]:
                flags.append(f"count_repeats {'on' if n[6] else 'off'}")
            out.append(f"step {i}: " + (", ".join(flags) or "rule flags changed"))
    if len(new_steps) > len(old_steps):
        out.append(f"{len(new_steps) - len(old_steps)} step(s) appended")
    elif len(old_steps) > len(new_steps):
        out.append(f"{len(old_steps) - len(new_steps)} step(s) removed")
    if old.rules_hash != new.rules_hash:
        out.append(f"rules hash {old.rules_hash[:12]} -> {new.rules_hash[:12]}")
    return out
