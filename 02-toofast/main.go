package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
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

	for _, p := range portsToScan {
		go func(p int) {
			conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", p))
			if err != nil {
				log.Printf("%d CLOSED (%s)\n", p, err)
				return
			}
			conn.Close()
			log.Printf("%d OPEN\n", p)
		}(p)
	}
	log.Println("DONE")
}
