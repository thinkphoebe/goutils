// +build bench

package strftime_test

import (
	"bytes"
	"log"
	"net/http"
	_ "net/http/pprof"
	"testing"
	"time"

	magicaltux "github.com/MagicalTux/strftime"
	fastly "github.com/fastly/go-utils/strftime"
	imperfectgo "github.com/imperfectgo/go-strftime"
	jehiah "github.com/jehiah/go-strftime"
	lestrrat "github.com/lestrrat-go/strftime"
	tebeka "github.com/tebeka/strftime"
	"strftime"
)

func init() {
	go func() {
		log.Println(http.ListenAndServe("localhost:8080", nil))
	}()
}

const benchfmt = `%A %a %B %b %d %H %I %M %m %p %S %Y %y %Z`

func BenchmarkGoutils(b *testing.B) {
	var t time.Time
	for i := 0; i < b.N; i++ {
		strftime.Strftime(benchfmt, &t)
	}
}

func BenchmarkImperfectGo(b *testing.B) {
	var t time.Time
	for i := 0; i < b.N; i++ {
		imperfectgo.Format(t, benchfmt)
	}
}

func BenchmarkMagicalTux(b *testing.B) {
	var t time.Time
	for i := 0; i < b.N; i++ {
		magicaltux.EnFormat(benchfmt, t)
	}
}

func BenchmarkTebeka(b *testing.B) {
	var t time.Time
	for i := 0; i < b.N; i++ {
		tebeka.Format(benchfmt, t)
	}
}

func BenchmarkJehiah(b *testing.B) {
	// Grr, uses byte slices, and does it faster, but with more allocs
	var t time.Time
	for i := 0; i < b.N; i++ {
		jehiah.Format(benchfmt, t)
	}
}

func BenchmarkFastly(b *testing.B) {
	var t time.Time
	for i := 0; i < b.N; i++ {
		fastly.Strftime(benchfmt, t)
	}
}

func BenchmarkLestrrat(b *testing.B) {
	var t time.Time
	for i := 0; i < b.N; i++ {
		lestrrat.Format(benchfmt, t)
	}
}

func BenchmarkLestrratCachedString(b *testing.B) {
	var t time.Time
	f, _ := lestrrat.New(benchfmt)
	// This benchmark does not take into effect the compilation time
	for i := 0; i < b.N; i++ {
		f.FormatString(t)
	}
}

func BenchmarkLestrratCachedWriter(b *testing.B) {
	var t time.Time
	f, _ := lestrrat.New(benchfmt)
	var buf bytes.Buffer
	b.ResetTimer()

	// This benchmark does not take into effect the compilation time
	// nor the buffer reset time
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		buf.Reset()
		b.StartTimer()
		f.Format(&buf, t)
		f.FormatString(t)
	}
}
