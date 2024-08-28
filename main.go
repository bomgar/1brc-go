package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"maps"
	"math"
	"os"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"syscall"
)

type MeasurementAgg struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int64
}

func main() {
	// 1. Create a file to write the profile to
	f, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer f.Close()

	// 2. Start CPU profiling
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()

	processFile(os.Args[1])
}

func processFile(filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Could not open input file: %v", err)
	}
	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalf("Could not stat file: %v", err)
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, int(fileInfo.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		log.Fatalf("Could not mmap file", err)
	}
	defer syscall.Munmap(data)

	reader := bytes.NewReader(data)
	scanner := bufio.NewScanner(reader)

	agg := make(map[string]*MeasurementAgg, 500)
	for scanner.Scan() {
		text := scanner.Text()
		split := strings.Split(text, ";")
		if len(split) != 2 {
			log.Fatalf("Invalid line: %s", text)
		}

		name := split[0]
		value, err := strconv.ParseFloat(split[1], 64)
		if err != nil {
			log.Fatalf("Could not parse value: %v", err)
		}

		stationAgg, ok := agg[name]
		if ok {
			stationAgg.Min = min(stationAgg.Min, value)
			stationAgg.Max = max(stationAgg.Max, value)
			stationAgg.Sum = stationAgg.Sum + value
			stationAgg.Count = stationAgg.Count + 1
		} else {
			agg[name] = &MeasurementAgg{
				Min:   value,
				Max:   value,
				Sum:   value,
				Count: 1,
			}
		}
	}

	for _, station := range slices.Sorted(maps.Keys(agg)) {
		stationAgg := agg[station]
		fmt.Println(station, stationAgg.Min, math.Round((stationAgg.Sum/float64(stationAgg.Count))*10.0)/10.0, stationAgg.Max)
	}

}
