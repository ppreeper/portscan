package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
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

	dest, err := os.Create(outFile)
	if err != nil {
		fmt.Printf("Failed to create scan results destination: %s\n", err)
		os.Exit(2)
	}

	// pipeline
	scanChan := store(dest, filter(scan(gen(portsToScan...))))

	// unfiltered
	// scanChan := store(dest, scan(gen(portsToScan...)))

	// broken up for explainability
	// var scanChan <-chan scanOp
	// scanChan = gen(portsToScan...)
	// scanChan = scan(scanChan)
	// scanChan = filter(scanChan)
	// scanChan = store(dest, scanChan)

	for s := range scanChan {
		if !s.open && s.scanErr != fmt.Sprintf("dial tcp %s:%d: connect: connection refused", s.host, s.port) {
			fmt.Println(s.scanErr)
		}
	}
}

type scanOp struct {
	host         string
	port         int
	open         bool
	scanErr      string
	scanDuration time.Duration
}

func (so scanOp) csvHeaders() []string {
	return []string{"host", "port", "open", "scanError", "scanDuration"}
}

func (so scanOp) asSlice() []string {
	return []string{
		strconv.FormatInt(int64(so.port), 10),
		strconv.FormatBool(so.open),
		so.scanErr,
		so.scanDuration.String(),
	}
}

func gen(ports ...int) <-chan scanOp {
	out := make(chan scanOp, len(ports))
	go func() {
		defer close(out)
		for _, p := range ports {
			out <- scanOp{port: p}
		}
	}()
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

func store(file io.Writer, in <-chan scanOp) <-chan scanOp {
	csvWriter := csv.NewWriter(file)
	out := make(chan scanOp)
	go func() {
		defer csvWriter.Flush()
		defer close(out)
		var headerWritten bool
		for scan := range in {
			if !headerWritten {
				headers := scan.csvHeaders()
				if err := csvWriter.Write(headers); err != nil {
					fmt.Println(err)
					break
				}
				headerWritten = true
			}
			values := scan.asSlice()
			if err := csvWriter.Write(values); err != nil {
				fmt.Println(err)
				break
			}
		}
	}()
	return out
}
