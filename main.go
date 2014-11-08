
// logfmt-metrics
//
// Send l2met-formatted metrics on stdin to statsd
//
// Much of this code liberally stolen from:
// https://github.com/kr/logfmt/blob/master/example_test.go
//
// l2met <-> statsd mappings
// -------------------------
// measure# ???
// sample#  Gauge
// count#   Increment
//
// TODO figure out what to do with sources

package main

import (
    "bufio"
    "bytes"
    "fmt"
    "log"
    "github.com/kr/logfmt"
    "github.com/quipo/statsd"
    "strconv"
    "time"
    "os"
)

type Measurement struct {
    Key     string
    Val     float64
    Unit    string
    Type    string  // TODO no
}

type Measurements []*Measurement

var measurePrefix = []byte("measure#")
var samplePrefix = []byte("sample#")
var countPrefix = []byte("count#")

// Define a handler for logfmt'd lines
func (mm *Measurements) HandleLogfmt(key, val []byte) error {
    var type_ string
    var prefix []byte
    switch {
    case bytes.HasPrefix(key, measurePrefix):
        type_ = "measurement"
        prefix = measurePrefix
    case bytes.HasPrefix(key, samplePrefix):
        type_ = "sample"
        prefix = samplePrefix
    case bytes.HasPrefix(key, countPrefix):
        type_ = "count"
        prefix = countPrefix
    default:
        return nil
    }

    i := bytes.LastIndexFunc(val, isDigit)
    v, err := strconv.ParseFloat(string(val[:i+1]), 10)

    if err != nil {
        return err
    }

    m := &Measurement{
        Key:    string(key[len(prefix):]),
        Val:    v,
        Unit:   string(val[i+1:]),
        Type:   type_,
    }

    *mm = append(*mm, m)
    return nil
}

// return true if r is an ASCII digit only, as opposed to unicode.IsDigit.
func isDigit(r rune) bool {
    return '0' <= r && r <= '9'
}

func main() {
    // allocate var to store measurements during parsing
    mm := make(Measurements, 0)

    // set up reader to read from stdin
    reader := bufio.NewReader(os.Stdin)

    // set up statsd
    prefix := "test." // TODO get from env var or cmdline
    statsd_host := "localhost:8125" // TODO get from env var or cmdline
    statsd_flush_interval := time.Second * 2    // TODO env var or cmdline...

    statsd_client := statsd.NewStatsdClient(statsd_host, prefix)
    statsd_client.CreateSocket()
    stats := statsd.NewStatsdBuffer(statsd_flush_interval, statsd_client)
    defer stats.Close()

    for {
        // read from stdin
        line, err := reader.ReadString('\n')
        if err != nil {
            // stop reading on error
            // TODO test for eof
            break
        }

        // parse the logfmt line
        if err := logfmt.Unmarshal([]byte(line), &mm); err != nil {
            log.Fatalf("err=%q", err) // TODO probably don't want to fail hard
        }

        // flush line to statsd
        for _, m := range mm {
            // display the metrics we collected
            fmt.Printf("%v\n", *m)

            // statsd go
            switch m.Type {
                case "measure":
                    // TODO
                case "sample":
                    stats.Gauge(m.Key, int64(m.Val))
                case "count":
                    stats.Incr(m.Key, int64(m.Val))
                default:
                    log.Printf("Unknown measurement type, ignoring: %v\n", *m)
            }
        }
    }
    // usage: echo 'measure#a=1ms count#b=10 sample#c=100MB measure#d=1s garbage' | logfmt-metrics
    // Output:
    // {a 1 ms}
    // {b 10 }
    // {c 100 MB}
    // {d 1 s}
}

