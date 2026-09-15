package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"syscall"
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

	portsChan := make(chan int, workers)
	resultsChan := make(chan int)

	for i := 0; i < cap(portsChan); i++ { // numWorkers also acceptable here
		go worker(host, portsChan, resultsChan)
	}

	go func() {
		for _, p := range portsToScan {
			portsChan <- p
		}
	}()

	for i := 0; i < len(portsToScan); i++ {
		if p := <-resultsChan; p != 0 { // non-zero port means it's open
			openPorts = append(openPorts, p)
		}
	}

	close(portsChan)
	close(resultsChan)
	printResults(openPorts)
}

func worker(host string, portsChan <-chan int, resultsChan chan<- int) {
	for p := range portsChan {
		address := net.JoinHostPort(host, fmt.Sprintf("%d", p))
		conn, err := net.Dial("tcp", address)
		if err != nil {
			fmt.Printf("%d CLOSED (%s)\n", p, err)
			resultsChan <- 0
			continue
		}
		conn.Close()
		resultsChan <- p
	}
}

func printResults(ports []int) {
	sort.Ints(ports)
	fmt.Println("\nResults\n--------------")
	for _, p := range ports {
		fmt.Printf("%d - open\n", p)
	}
}
