package pool

import (
	"testing"
)

func BenchmarkPool(b *testing.B) {
	handler := func(str string) (string, error) {
		r := []rune(str)
		for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
			r[i], r[j] = r[j], r[i]
		}
		return string(r), nil
	}

	pool := New(handler, 1)
	pool.Start()
	defer pool.Close()
	runs := 10000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go func() {
			for j := 0; j < runs; j++ {
				pool.Dispatch("task")
			}
		}()

		for k := 0; k < runs; k++ {
			<-pool.Results()
		}
	}
}
