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
