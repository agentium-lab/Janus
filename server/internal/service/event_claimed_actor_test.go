package service

import (
	"bytes"
	"testing"
)

func TestAppendClaimedActor_EmptyPayload(t *testing.T) {
	got := appendClaimedActor(nil, "alice")
	if !bytes.Contains(got, []byte(`"claimed_actor":"alice"`)) {
		t.Fatalf("want claimed_actor injected, got %s", got)
	}
}

func TestAppendClaimedActor_PreservesExistingFields(t *testing.T) {
	in := []byte(`{"status":"queued","mailbox":"mb1"}`)
	got := appendClaimedActor(in, "bob")
	for _, want := range []string{`"status":"queued"`, `"mailbox":"mb1"`, `"claimed_actor":"bob"`} {
		if !bytes.Contains(got, []byte(want)) {
			t.Fatalf("missing %s in %s", want, got)
		}
	}
}

func TestAppendClaimedActor_NeverOverwrites(t *testing.T) {
	in := []byte(`{"claimed_actor":"original"}`)
	got := appendClaimedActor(in, "impostor")
	if !bytes.Equal(got, in) {
		t.Fatalf("existing claimed_actor must win, got %s", got)
	}
}

func TestAppendClaimedActor_NonObjectPayloadUntouched(t *testing.T) {
	in := []byte(`[1,2,3]`)
	if got := appendClaimedActor(in, "x"); !bytes.Equal(got, in) {
		t.Fatalf("non-object payload must pass through, got %s", got)
	}
}
