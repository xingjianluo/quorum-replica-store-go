package store

import "testing"

func TestDurableLogEntriesBoundaries(t *testing.T) {
	l, _ := NewDurableLog("")
	for i := int64(1); i <= 3; i++ {
		if err := l.Append(Entry{Term: 1, Index: i}); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct{ from, to, want int64 }{
		{1, 2, 1}, // [1,2) -> 1 条（index 1）
		{1, 4, 3}, // [1,4) 夹到末尾 -> 3 条
		{2, 4, 2}, // index 2,3
		{3, 4, 1}, // 仅 index 3（关键：最后一条不能丢）
		{1, 0, 3}, // to<=0 到末尾
	}
	for _, c := range cases {
		got := int64(len(l.Entries(c.from, c.to)))
		if got != c.want {
			t.Errorf("Entries(%d,%d)=%d want %d", c.from, c.to, got, c.want)
		}
	}
}
