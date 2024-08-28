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
	"time"
)

type Measurement struct {
	Station string
	Value   float64
}
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

func monitorChan[T any](name string, ch chan T) {
	for {
		if len(ch) == cap(ch) {
			fmt.Println("Channel full:", name)
		}
		time.Sleep(1 * time.Second)
	}
}

func readFile(filePath string, batches chan<- []string) {
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
	batch := make([]string, 0, 1000)
	for scanner.Scan() {
		line := scanner.Text()
		batch = append(batch, line)
		if len(batch) == 1000 {
			batches <- batch
			batch = make([]string, 0, 1000)
		}
	}
	close(batches)
}

func splitLine(line string) (string, string) {
	commaIndex := strings.Index(line, ";")
	if commaIndex == -1 {
		log.Fatalf("Invalid line: %s", line)
	}
	return line[:commaIndex], line[commaIndex+1:]
}

func parseLines(lineBatches <-chan []string, measurementBatches chan<- []Measurement) {

	for linesBatch := range lineBatches {

		measurementBatch := make([]Measurement, 0, len(linesBatch))
		for _, line := range linesBatch {

			name, valueString := splitLine(line)
			value, err := strconv.ParseFloat(valueString, 64)
			if err != nil {
				log.Fatalf("Could not parse value: %v", err)
			}
			measurement := Measurement{
				Station: name,
				Value:   value,
			}

			measurementBatch = append(measurementBatch, measurement)
		}
		measurementBatches <- measurementBatch
	}
	close(measurementBatches)
}

func processFile(filePath string) {
	lineBatches := make(chan []string, 50)
	go readFile(filePath, lineBatches)

	measurementBatches := make(chan []Measurement, 50)
	go parseLines(lineBatches, measurementBatches)

	monitorChan("lineBatches", lineBatches)
	monitorChan("measurementBatches", measurementBatches)

	agg := make(map[string]*MeasurementAgg, 500)
	for batch := range measurementBatches {
		for _, measurement := range batch {
			station := measurement.Station
			value := measurement.Value

			stationAgg, ok := agg[station]
			if ok {
				stationAgg.Min = min(stationAgg.Min, value)
				stationAgg.Max = max(stationAgg.Max, value)
				stationAgg.Sum = stationAgg.Sum + value
				stationAgg.Count = stationAgg.Count + 1
			} else {
				agg[station] = &MeasurementAgg{
					Min:   value,
					Max:   value,
					Sum:   value,
					Count: 1,
				}
			}
		}

	}

	for _, station := range slices.Sorted(maps.Keys(agg)) {
		stationAgg := agg[station]
		fmt.Println(station, stationAgg.Min, math.Round((stationAgg.Sum/float64(stationAgg.Count))*10.0)/10.0, stationAgg.Max)
	}
}
