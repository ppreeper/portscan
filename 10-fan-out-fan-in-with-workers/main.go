package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
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
	fmt.Println("ports", ports)

	portsToScan, err := parsePortsToScan(ports)
	if err != nil {
		fmt.Printf("Failed to parse ports to scan: %s\n", err)
		os.Exit(1)
	}

	// The done channel will be shared by the entire pipeline
	// so that when it's closed it serves as a signal
	// for all the goroutines we started to exit.
	done := make(chan struct{})
	defer close(done)

	in := gen(done, host, portsToScan...)

	// fan-out
	var chans []<-chan scanOp
	for i := 0; i < workers; i++ {
		chans = append(chans, scan(done, in))
	}

	for s := range filterOpen(done, merge(done, chans...)) {
		fmt.Printf("%#v\n", s)
	}

	for s := range filterErr(done, merge(done, chans...)) {
		fmt.Printf("%#v\n", s)
		done <- struct{}{}
		return
	}

	// done chan is closed by the deferred call here
}

type scanOp struct {
	host         string
	port         int
	open         bool
	scanErr      string
	scanDuration time.Duration
}

func gen(done <-chan struct{}, host string, ports ...int) <-chan scanOp {
	out := make(chan scanOp, len(ports))
	go func() {
		defer close(out)
		for _, p := range ports {
			select {
			case out <- scanOp{host: host, port: p}:
			case <-done:
				return
			}
		}
	}()
	return out
}

func scan(done <-chan struct{}, in <-chan scanOp) <-chan scanOp {
	out := make(chan scanOp)
	go func() {
		defer close(out)
		for scan := range in {
			select {
			default:
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
			case <-done:
				return
			}
		}
	}()
	return out
}

func filterOpen(done <-chan struct{}, in <-chan scanOp) <-chan scanOp {
	out := make(chan scanOp)
	go func() {
		defer close(out)
		for scan := range in {
			select {
			default:
				if scan.open {
					out <- scan
				}
			case <-done:
				return
			}
		}
	}()
	return out
}

func filterErr(done <-chan struct{}, in <-chan scanOp) <-chan scanOp {
	out := make(chan scanOp)
	go func() {
		defer close(out)
		for scan := range in {
			select {
			default:
				if !scan.open && strings.Contains(scan.scanErr, "too many open files") {
					out <- scan
				}
			case <-done:
				return
			}
		}
	}()
	return out
}

func merge(done <-chan struct{}, chans ...<-chan scanOp) <-chan scanOp {
	out := make(chan scanOp)
	wg := sync.WaitGroup{}
	wg.Add(len(chans))

	for _, sc := range chans {
		go func(sc <-chan scanOp) {
			defer wg.Done()
			for scan := range sc {
				select {
				case out <- scan:
				case <-done:
					return
				}
			}
		}(sc)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
