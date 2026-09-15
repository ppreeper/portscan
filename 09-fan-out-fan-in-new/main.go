package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"sync"
	"time"
)

var (
	host    string
	ports   string
	workers int
	timeout int
	outFile string
)

func init() {
	flag.StringVar(&host, "host", "127.0.0.1", "Host to scan.")
	flag.StringVar(&ports, "ports", "", "Port(s) (e.g. 80,22-100) (no spaces).")
	flag.IntVar(&timeout, "timeout", 5, "Timeout in seconds (default is 5).")
	flag.IntVar(&workers, "workers", runtime.NumCPU(), "Number of workers (defaults to # of logical CPUs).")
	flag.StringVar(&outFile, "outfile", "scans.csv", "Destination of scan results (defaults to scans.csv)")
}

func main() {
	flag.Parse()

	portsToScan, err := parsePortsToScan(ports)
	if err != nil {
		fmt.Printf("Failed to parse ports to scan: %s\n", err)
		os.Exit(1)
	}

	in := gen(host, portsToScan...)

	// fan-out
	sc1 := scan(in)
	sc2 := scan(in)
	sc3 := scan(in)

	for s := range filter(merge(sc1, sc2, sc3)) {
		// for s := range merge(sc1, sc2, sc3) {
		fmt.Printf("%#v\n", s)
	}
}

type scanOp struct {
	host         string
	port         int
	open         bool
	scanErr      string
	scanDuration time.Duration
}

func gen(host string, ports ...int) <-chan scanOp {
	out := make(chan scanOp, len(ports))
	for _, p := range ports {
		out <- scanOp{host: host, port: p}
	}
	close(out)
	return out
}

func scan(in <-chan scanOp) <-chan scanOp {
	out := make(chan scanOp)
	go func() {
		defer close(out)
		for scan := range in {
			address := net.JoinHostPort(scan.host, fmt.Sprintf("%d", scan.port))
			start := time.Now()
			conn, err := net.Dial("tcp", address)
			scan.scanDuration = time.Since(start)
			if err != nil {
				scan.scanErr = err.Error()
			} else {
				conn.Close()
				scan.open = true
			}
			out <- scan
		}
	}()
	return out
}

func filter(in <-chan scanOp) <-chan scanOp {
	out := make(chan scanOp)
	go func() {
		defer close(out)
		for scan := range in {
			if scan.open {
				out <- scan
			}
		}
	}()
	return out
}

func merge(chans ...<-chan scanOp) <-chan scanOp {
	out := make(chan scanOp)
	wg := sync.WaitGroup{}
	// wg.Add(len(chans))

	for _, sc := range chans {
		wg.Go(func() {
			// sc <- chan scanOp
			for scan := range sc {
				out <- scan
			}
		})
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
