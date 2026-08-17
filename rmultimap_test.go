package redi_test

import "testing"

func TestRSetMultimap_ExtendedOperations(t *testing.T) {
	client := newTestClient(t)
	m := client.GetSetMultimap(uniqueKey(t, "setmm-extended"))
	defer m.Clear(testCtx) //nolint:errcheck

	changed, err := m.PutAll(testCtx, int64(7), "a", "b", "b")
	if err != nil || !changed {
		t.Fatalf("PutAll(non-string key) = %v, %v", changed, err)
	}
	if _, err := m.Put(testCtx, "other", "shared"); err != nil {
		t.Fatal("Put:", err)
	}

	old, err := m.ReplaceValues(testCtx, int64(7), "b", "c", "c")
	if err != nil || !sameStringSet(old, "a", "b") {
		t.Fatalf("ReplaceValues old = %v, %v", old, err)
	}
	got, err := m.Get(testCtx, int64(7))
	if err != nil || !sameStringSet(got, "b", "c") {
		t.Fatalf("Get after ReplaceValues = %v, %v", got, err)
	}

	if err := m.FastReplaceValues(testCtx, int64(7), "z"); err != nil {
		t.Fatal("FastReplaceValues:", err)
	}
	if contains, err := m.ContainsValue(testCtx, "shared"); err != nil || !contains {
		t.Fatalf("ContainsValue(shared) = %v, %v", contains, err)
	}
	if contains, err := m.ContainsValue(testCtx, "missing"); err != nil || contains {
		t.Fatalf("ContainsValue(missing) = %v, %v", contains, err)
	}
	values, err := m.Values(testCtx)
	if err != nil || !sameStringSet(values, "shared", "z") {
		t.Fatalf("Values = %v, %v", values, err)
	}
	if n, err := m.KeySize(testCtx); err != nil || n != 2 {
		t.Fatalf("KeySize = %d, %v; want 2", n, err)
	}
	if keys, err := m.ReadAllKeySet(testCtx); err != nil || len(keys) != 2 {
		t.Fatalf("ReadAllKeySet = %v, %v", keys, err)
	}
	if empty, err := m.IsEmpty(testCtx); err != nil || empty {
		t.Fatalf("IsEmpty = %v, %v; want false", empty, err)
	}

	if err := m.FastReplaceValues(testCtx, "other"); err != nil {
		t.Fatal("FastReplaceValues(empty):", err)
	}
	old, err = m.ReplaceValues(testCtx, int64(7))
	if err != nil || !sameStringSet(old, "z") {
		t.Fatalf("ReplaceValues(empty) old = %v, %v", old, err)
	}
	if n, err := m.KeySize(testCtx); err != nil || n != 0 {
		t.Fatalf("KeySize after empty replacements = %d, %v", n, err)
	}
	if empty, err := m.IsEmpty(testCtx); err != nil || !empty {
		t.Fatalf("IsEmpty after empty replacements = %v, %v", empty, err)
	}
}

func TestRListMultimap_ReplaceValuesPreservesOrder(t *testing.T) {
	client := newTestClient(t)
	m := client.GetListMultimap(uniqueKey(t, "listmm-replace"))
	defer m.Clear(testCtx) //nolint:errcheck

	old, err := m.ReplaceValues(testCtx, 42, "a", "b", "a")
	if err != nil || len(old) != 0 {
		t.Fatalf("initial ReplaceValues old = %v, %v", old, err)
	}
	old, err = m.ReplaceValues(testCtx, 42, "c", "d")
	if err != nil || !sameStrings(old, "a", "b", "a") {
		t.Fatalf("ReplaceValues old = %v, %v", old, err)
	}
	got, err := m.Get(testCtx, 42)
	if err != nil || !sameStrings(got, "c", "d") {
		t.Fatalf("Get after ReplaceValues = %v, %v", got, err)
	}

	if err := m.FastReplaceValues(testCtx, 42, "x", "y", "x"); err != nil {
		t.Fatal("FastReplaceValues:", err)
	}
	got, err = m.Get(testCtx, 42)
	if err != nil || !sameStrings(got, "x", "y", "x") {
		t.Fatalf("Get after FastReplaceValues = %v, %v", got, err)
	}
	values, err := m.Values(testCtx)
	if err != nil || !sameStrings(values, "x", "y", "x") {
		t.Fatalf("Values = %v, %v", values, err)
	}
}

func sameStringSet(values []any, want ...string) bool {
	if len(values) != len(want) {
		return false
	}
	counts := make(map[string]int, len(want))
	for _, value := range want {
		counts[value]++
	}
	for _, value := range values {
		s, ok := value.(string)
		if !ok || counts[s] == 0 {
			return false
		}
		counts[s]--
	}
	return true
}

func sameStrings(values []any, want ...string) bool {
	if len(values) != len(want) {
		return false
	}
	for i, value := range values {
		if value != want[i] {
			return false
		}
	}
	return true
}
