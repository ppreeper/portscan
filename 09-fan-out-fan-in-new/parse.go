package main

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

func parsePortsToScan(portsFlag string) ([]int, error) {
	portTokens := strings.Split(portsFlag, ",")
	ports := []int{}

	for _, ptoken := range portTokens {
		tokenPorts := strings.Split(ptoken, "-")
		tplen := len(tokenPorts)
		if tplen < 1 || tplen > 2 {
			return nil, errors.New("invalid port range to scan")
		}
		switch tplen {
		case 1:
			p, err := strconv.Atoi(tokenPorts[0])
			if err == nil {
				if p <= 0 && p > 65535 {
					return []int{}, fmt.Errorf("port %d out of range", p)
				}
				ports = append(ports, p)
				continue
			}
		case 2:
			startPort, err := strconv.Atoi(tokenPorts[0])
			if err != nil {
				return nil, fmt.Errorf("failed to convert %d to a valid port number", ports[0])
			}
			endPort, err := strconv.Atoi(tokenPorts[1])
			if err != nil {
				return nil, fmt.Errorf("failed to convert %d to a valid port number", ports[1])
			}
			// range check
			if (startPort <= 0 && startPort > 65535) || (endPort <= 0 && endPort > 65535) {
				return nil, fmt.Errorf("port numbers must be greater than 0")
			}
			var results []int
			for p := min(startPort, endPort); p <= max(startPort, endPort); p++ {
				results = append(results, p)
			}
			ports = append(ports, results...)
		}
	}
	slices.Sort(ports)

	return ports, nil
}
