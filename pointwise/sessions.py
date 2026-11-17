"""Deterministic session derivation.

Sessions are never trusted directly from event payloads.  They are re-derived
from the full, corrected stream every time, because an identity correction or
a late event can join two windows that were previously separate.

Derivation rules, applied **per user** over that user's events ordered by
``(ts_ms, seq, event_id)`` — another user's event interleaved in between must
not paper over a gap or end a session:

1. The user's first event opens a session.
2. A ``session_start`` event opens a new session.
3. Any event more than ``session_gap_ms`` after that user's predecessor opens
   a new session.
4. The first event after a ``session_end`` opens a new session.
5. ``identity_merge`` events carry ``props.merge_session`` naming another
   session (a derived id *or* a collector-provided ``session_id``) **of the
   same user**; the two windows are unioned.  This is what re-joins a
   pre-login anonymous session to a later identified one, and what a late
   event uses to back-fill history.  Funnel state is keyed by ``user_id``, so
   a merge that named another user's session could not join their funnels;
   it is ignored rather than silently producing a session id shared by two
   different people.
6. A merged session keeps its earliest constituent id.  ``to_ms`` is the start
   of the next distinct derived session, or ``None`` while open.

Derived ids, in preference order: the payload ``session_id`` of the session's
``session_start`` event; the payload ``session_id`` of its first event; else
``auto:<event_id>``.  Session ranges are half-open ``[from_ms, to_ms)``; a
merged session can cover a range containing other sessions' boundaries, so
:meth:`SessionMap.for_ts` returns the most recently started matching session.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field


@dataclass
class DerivedSession:
    session_id: str
    from_ms: int
    to_ms: int | None
    event_seqs: list[int] = field(default_factory=list)
    first_pos: int = 0
    last_pos: int = 0
    ends_with_session_end: bool = False
    merge_aliases: list[str] = field(default_factory=list)


@dataclass
class SessionMap:
    sessions: list[DerivedSession]
    by_seq: dict[int, str]

    def for_ts(self, ts: int) -> str | None:
        found = None
        for session in self.sessions:
            if ts < session.from_ms:
                continue
            if session.to_ms is None or ts < session.to_ms:
                found = session.session_id
        return found

    def session(self, session_id: str) -> DerivedSession | None:
        for s in self.sessions:
            if s.session_id == session_id:
                return s
        return None


def _get(row, key, default=None):
    try:
        v = row[key]
        return default if v is None else v
    except (KeyError, IndexError):
        return getattr(row, key, default)


def _loads(raw) -> dict:
    try:
        value = json.loads(raw or "{}")
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def derive_sessions(rows: list, session_gap_ms: int) -> SessionMap:
    if not rows:
        return SessionMap([], {})

    groups: list[list[int]] = []
    open_group: dict[str, int] = {}
    last_pos: dict[str, int] = {}
    for pos, row in enumerate(rows):
        kind = _get(row, "kind") or "event"
        ts = int(_get(row, "ts_ms"))
        user = str(_get(row, "user_id"))
        prev_index = last_pos.get(user)
        starts_new = prev_index is None or kind == "session_start"
        if prev_index is not None:
            prev = rows[prev_index]
            prev_kind = _get(prev, "kind") or "event"
            gap = ts - int(_get(prev, "ts_ms"))
            if prev_kind == "session_end" or gap > session_gap_ms:
                starts_new = True
        if starts_new:
            groups.append([pos])
            open_group[user] = len(groups) - 1
        else:
            groups[open_group[user]].append(pos)
        last_pos[user] = pos

    # Per-group ids and payload aliases.
    group_ids: list[str] = []
    group_users: list[str] = []
    aliases: list[set[str]] = []
    ended: set[int] = set()
    # Derived ids only need to be unique within one user (same_session compares
    # sessions of that user's own attempts), so two people may both have "s1".
    used_ids: dict[tuple[str, str], int] = {}
    for index, positions in enumerate(groups):
        boundary = rows[positions[0]]
        payload_sid = (_get(boundary, "session_id") or "").strip()
        explicit_start = next(
            (rows[p] for p in positions if (_get(rows[p], "kind") == "session_start")),
            None,
        )
        start_sid = (_get(explicit_start, "session_id") or "").strip() if explicit_start else ""
        gid = start_sid or payload_sid or f"auto:{_get(boundary, 'event_id')}"
        user_key = str(_get(boundary, "user_id"))
        if (user_key, gid) in used_ids:
            # The collector reused an id across a boundary we had to split
            # (e.g. a late session_end).  Derived ids must stay unique per
            # user, or two windows would be indistinguishable to same_session.
            used_ids[(user_key, gid)] += 1
            gid = f"{gid}#{used_ids[(user_key, gid)]}"
        else:
            used_ids[(user_key, gid)] = 1
        group_ids.append(gid)
        group_users.append(str(_get(boundary, "user_id")))
        alias = {str(_get(rows[p], "session_id")) for p in positions if _get(rows[p], "session_id")}
        alias.add(gid)
        aliases.append(alias)
        if (_get(rows[positions[-1]], "kind") or "event") == "session_end":
            ended.add(index)

    # Union-find over group indices.
    parent = list(range(len(groups)))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        if rb < ra:  # earlier group is the surviving root
            ra, rb = rb, ra
        parent[rb] = ra

    # Aliases are namespaced by user: two people can legitimately send the
    # same payload session id, and a merge must not unite them.
    alias_to_group: dict[tuple[str, str], int] = {}
    for index, names in enumerate(aliases):
        for name in names:
            alias_to_group.setdefault((group_users[index], name), index)

    for index, positions in enumerate(groups):
        for pos in positions:
            row = rows[pos]
            is_merge = (_get(row, "kind") == "identity_merge") or (
                _get(row, "name") == "identity_merge"
            )
            if not is_merge:
                continue
            target = _loads(_get(row, "properties")).get("merge_session")
            if not isinstance(target, str) or not target:
                continue
            # The merge event itself may land in a window of its own; tie it to
            # the session named by its own payload so the chain
            # (anonymous -> merge moment -> identified) becomes one session.
            user = group_users[index]
            own_alias = (_get(row, "session_id") or "").strip()
            if own_alias and (user, own_alias) in alias_to_group:
                union(index, alias_to_group[(user, own_alias)])
            other = alias_to_group.get((user, target))
            if other is None:
                # Referencing a session of this user we have not seen: remember
                # the alias so a later event carrying that id joins up.
                alias_to_group[(user, target)] = index
                aliases[index].add(target)
                continue
            union(index, other)

    members: dict[int, list[int]] = {}
    for index in range(len(groups)):
        members.setdefault(find(index), []).append(index)

    sessions: list[DerivedSession] = []
    by_seq: dict[int, str] = {}
    for root, indices in members.items():
        indices.sort()
        first_pos = groups[indices[0]][0]
        last_pos = groups[indices[-1]][-1]
        sid = group_ids[indices[0]]
        if len({group_ids[i] for i in indices}) > 1:
            # Preserve the deterministic "earliest wins" rule when two
            # different payload ids were merged.
            sid = group_ids[indices[0]]
        seqs: list[int] = []
        merged_aliases: list[str] = []
        for i in indices:
            merged_aliases.extend(sorted(aliases[i]))
            for p in groups[i]:
                seq = int(_get(rows[p], "seq"))
                seqs.append(seq)
                by_seq[seq] = sid
        sessions.append(
            DerivedSession(
                session_id=sid,
                from_ms=int(_get(rows[first_pos], "ts_ms")),
                to_ms=None,
                event_seqs=seqs,
                first_pos=first_pos,
                last_pos=last_pos,
                ends_with_session_end=indices[-1] in ended,
                merge_aliases=merged_aliases,
            )
        )

    sessions.sort(key=lambda s: (s.from_ms, s.first_pos))
    for i in range(len(sessions) - 1):
        sessions[i].to_ms = sessions[i + 1].from_ms
    return SessionMap(sessions, by_seq)
