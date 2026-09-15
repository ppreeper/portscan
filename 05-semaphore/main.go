package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"syscall"

	"golang.org/x/sync/semaphore"
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

	var openPorts []int

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		printResults(openPorts)
		os.Exit(0)
	}()

	var semMaxWeight int64 = 100_000
	var semAcquisitionWeight int64 = 100

	sem := semaphore.NewWeighted(semMaxWeight)
	ctx := context.Background()

	for _, port := range portsToScan {
		if err := sem.Acquire(ctx, semAcquisitionWeight); err != nil {
			fmt.Printf("Failed to acquire semaphore (port %d): %v\n", port, err)
			break
		}

		go func(port int) {
			defer sem.Release(semAcquisitionWeight)
			p := scan(host, port)
			if p != 0 {
				openPorts = append(openPorts, p)
			}
		}(port)
	}

	// We block here until done.
	if err := sem.Acquire(ctx, int64(semMaxWeight)); err != nil {
		fmt.Printf("Failed to acquire semaphore: %v\n", err)
	}

	printResults(openPorts)
}

func scan(host string, port int) int {
	address := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("%d CLOSED (%s)\n", port, err)
		return 0
	}
	conn.Close()
	return port
}

func printResults(ports []int) {
	sort.Ints(ports)
	fmt.Println("\nResults\n--------------")
	for _, p := range ports {
		fmt.Printf("%d - open\n", p)
	}
}
