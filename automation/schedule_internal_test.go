package automation

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNextRunSkipsMissedIntervals(t *testing.T) {
	now := time.Date(2026, 11, 27, 10, 30, 0, 0, time.UTC)
	schedule := Schedule{
		NextRunAt:    now.Add(-3*time.Hour - 10*time.Minute),
		EverySeconds: 3600,
	}
	next := nextRun(schedule, now)
	if !next.After(now) {
		t.Fatalf("next = %s, want after %s", next, now)
	}
	if next.Sub(schedule.NextRunAt)%time.Hour != 0 {
		t.Fatalf("next %s did not preserve the interval grid", next)
	}
	if next.Sub(now) > time.Hour {
		t.Fatalf("next = %s, more than one interval after now", next)
	}
}

func TestCompletedRunTimesAndUntil(t *testing.T) {
	now := time.Date(2026, 11, 27, 10, 30, 0, 0, time.UTC)
	schedule := Schedule{
		ID: "s", Channel: "work", Creator: "local:user:alice",
		NextRunAt: now.Add(-time.Minute), EverySeconds: 60, Times: 2,
	}
	first := completedRun(schedule, now, 7)
	if first.Done || first.NextRunAt.IsZero() {
		t.Fatalf("first run = %+v, want a next run", first)
	}
	schedule.Sent = 1
	second := completedRun(schedule, now, 8)
	if !second.Done {
		t.Fatalf("second of two runs = %+v, want done", second)
	}

	until := now.Add(30 * time.Second)
	timed := Schedule{
		ID: "s", Channel: "work", Creator: "local:user:alice",
		NextRunAt: now.Add(-time.Minute), EverySeconds: 60, UntilAt: &until,
	}
	if result := completedRun(timed, now, 9); !result.Done {
		t.Fatalf("a run past until = %+v, want done", result)
	}
}

func TestActionSpecJSONHidesParams(t *testing.T) {
	raw, err := json.Marshal(ActionSpec{Name: "post", Params: []byte(`{"text":"secret"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"name":"post"}` {
		t.Fatalf("ActionSpec JSON = %s; params must stay out of every DTO", raw)
	}
	if err := (ActionSpec{Name: "post", Params: []byte(`{"url":"https://example.com"}`)}).Validate(); err == nil {
		t.Fatal("a URL parameter was accepted")
	}
}
