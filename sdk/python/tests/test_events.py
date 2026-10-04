import json
from pathlib import Path

import pytest

from relayplane import Event, SequenceTracker

EXAMPLES = Path(__file__).resolve().parents[3] / "docs" / "events" / "examples"


def ev(instance: str, seq: int) -> Event:
    return Event(event_id=f"e{instance}{seq}", event_type="message.received", sequence=seq, schema_version=1, provider="p", tenant_id="t",
                 instance_id=instance, timestamp="2026-10-03T00:00:00Z", payload={})


def test_every_published_example_parses():
    files = sorted(EXAMPLES.glob("*.json"))
    assert len(files) >= 6
    for f in files:
        e = Event.from_dict(json.loads(f.read_text(encoding="utf-8")))
        assert e.sequence >= 1 and e.schema_version == 1 and e.event_type and e.instance_id


def test_an_unknown_schema_version_is_refused():
    d = json.loads((EXAMPLES / "message.received.json").read_text(encoding="utf-8"))
    d["schema_version"] = 2
    with pytest.raises(ValueError):
        Event.from_dict(d)


def test_tracker_reorders_what_a_retry_reordered():
    t = SequenceTracker()
    assert [e.sequence for e in t.push(ev("i", 1))] == [1]
    assert t.push(ev("i", 3)) == []  # 2 is late
    assert t.missing("i") == [2]
    assert [e.sequence for e in t.push(ev("i", 2))] == [2, 3]
    assert t.missing("i") == []


def test_tracker_drops_redeliveries():
    t = SequenceTracker()
    t.push(ev("i", 1))
    assert t.push(ev("i", 1)) == []  # already processed
    t.push(ev("i", 3))
    assert t.push(ev("i", 3)) == []  # already waiting


def test_tracker_instances_are_independent():
    t = SequenceTracker()
    assert len(t.push(ev("a", 1))) == 1
    assert len(t.push(ev("b", 1))) == 1
    assert t.push(ev("a", 3)) == [] and len(t.push(ev("b", 2))) == 1


def test_tracker_gives_up_on_a_gap_when_asked():
    t = SequenceTracker()
    t.push(ev("i", 1))
    t.push(ev("i", 4))
    t.push(ev("i", 5))
    assert t.missing("i") == [2, 3]
    assert [e.sequence for e in t.skip_gap("i")] == [4, 5]
    assert t.push(ev("i", 2)) == []  # too late: it is behind us now


def test_tracker_state_survives_a_restart():
    t = SequenceTracker()
    t.push(ev("i", 1))
    t.push(ev("i", 2))
    t2 = SequenceTracker(t.state())
    assert t2.push(ev("i", 2)) == []  # redelivered after the restart: already done
    assert [e.sequence for e in t2.push(ev("i", 3))] == [3]


def test_media_of_a_message_is_typed():
    ready = Event.from_dict(json.loads((EXAMPLES / "message.received.media.json").read_text(encoding="utf-8")))
    assert ready.media and ready.media.ready and ready.media.kind == "audio" and ready.media.seconds == 7 and ready.media.size == 4719
    refused = Event.from_dict(json.loads((EXAMPLES / "message.received.media.rejected.json").read_text(encoding="utf-8")))
    assert refused.media and not refused.media.ready and refused.media.reason == "too_large"
    plain = Event.from_dict(json.loads((EXAMPLES / "message.received.json").read_text(encoding="utf-8")))
    assert plain.media is None


# the recipe documented in docs/EVENTS.md ("uma confirmação que não pode ser desfeita por baixo"): it must really work
def verdict(event: Event, prompt_id: str, answers: dict) -> "str | None":
    p = event.payload
    if event.event_type == "message.received":
        if p.get("reply_to_provider_message_id") == prompt_id and p["type"] == "text":
            answers[p["provider_message_id"]] = p["text"]
            return "answered"
        if p["type"] == "secretEncrypted":
            answers.clear()
            return "ask_again"
    if event.event_type == "message.deleted" and p["provider_message_id"] in answers:
        del answers[p["provider_message_id"]]
        return "ask_again"
    return None


def _ev(seq, typ, payload):
    return Event(event_id=f"e{seq}", event_type=typ, sequence=seq, schema_version=1, provider="p", tenant_id="t", instance_id="i",
                 timestamp="2026-10-03T00:00:00Z", payload=payload)


def test_confirmation_recipe_survives_a_revocation():
    answers: dict = {}
    yes = _ev(1, "message.received", {"provider_message_id": "U1", "reply_to_provider_message_id": "PROMPT", "type": "text", "text": "sim", "from": "55"})
    assert verdict(yes, "PROMPT", answers) == "answered" and answers == {"U1": "sim"}
    assert verdict(_ev(2, "message.deleted", {"provider_message_id": "U1"}), "PROMPT", answers) == "ask_again" and answers == {}
    # an answer that does not quote the prompt is not an answer to it
    other = _ev(3, "message.received", {"provider_message_id": "U2", "type": "text", "text": "sim", "from": "55"})
    assert verdict(other, "PROMPT", answers) is None and answers == {}
    # an edit of an earlier answer voids it
    verdict(yes, "PROMPT", answers)
    assert verdict(_ev(4, "message.received", {"provider_message_id": "U3", "type": "secretEncrypted", "from": "55"}), "PROMPT", answers) == "ask_again"
    assert answers == {}
