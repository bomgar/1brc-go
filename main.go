package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"maps"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"sync"
	"syscall"
	"unsafe"

	"github.com/valyala/fastjson/fastfloat"
)

type Measurement struct {
	Station []byte
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

func aggregateDataInChunks(data []byte, aggregations chan<- map[string]*MeasurementAgg) {

	chunkSize := len(data) / runtime.NumCPU()
	if chunkSize == 0 {
		chunkSize = len(data)
	}
	var wg sync.WaitGroup

	goChunk := func(data []byte) {
		processChunk(data, aggregations)
		wg.Done()
	}

	start := 0
	offset := 0
	for offset < len(data) {
		offset += chunkSize
		if offset >= len(data) {
			wg.Add(1)
			go goChunk(data[start:])
			break
		}

		newlineIndex := bytes.IndexByte(data[offset:], '\n')
		if newlineIndex == -1 {
			wg.Add(1)
			go goChunk(data[start:])
			break
		} else {
			offset += newlineIndex + 1
			wg.Add(1)
			go goChunk(data[start:offset])
			start = offset
		}
	}

	wg.Wait()
	close(aggregations)
}

func processChunk(data []byte, aggregations chan<- map[string]*MeasurementAgg) {
	reader := bytes.NewReader(data)
	scanner := bufio.NewScanner(reader)
	agg := make(map[string]*MeasurementAgg, 500)
	for scanner.Scan() {
		line := scanner.Bytes()
		measurement := parseLine(line)
		value := measurement.Value
		unsafeStation := unsafe.String(&measurement.Station[0], len(measurement.Station))
		stationAgg, ok := agg[unsafeStation]
		if ok {
			stationAgg.Min = min(stationAgg.Min, value)
			stationAgg.Max = max(stationAgg.Max, value)
			stationAgg.Sum = stationAgg.Sum + value
			stationAgg.Count = stationAgg.Count + 1
		} else {
			station := string(measurement.Station)
			agg[station] = &MeasurementAgg{
				Min:   value,
				Max:   value,
				Sum:   value,
				Count: 1,
			}
		}
	}
	aggregations <- agg
}

func splitLine(line []byte) ([]byte, []byte) {
	commaIndex := bytes.IndexByte(line, ';')
	if commaIndex == -1 {
		log.Fatalf("Invalid line: %s", line)
	}
	return line[:commaIndex], line[commaIndex+1:]
}

func parseLine(line []byte) Measurement {
	name, valueRaw := splitLine(line)
	value := fastfloat.ParseBestEffort(unsafe.String(&valueRaw[0], len(valueRaw)))
	return Measurement{
		Station: name,
		Value:   value,
	}
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
		log.Fatalf("Could not mmap file: %v", err)
	}
	defer syscall.Munmap(data)

	processData(data)
}

func processData(data []byte) {
	aggregations := make(chan map[string]*MeasurementAgg)
	go aggregateDataInChunks(data, aggregations)

	totalAggreation := make(map[string]*MeasurementAgg, 500)
	for subAgg := range aggregations {
		for station, value := range subAgg {
			stationAgg, ok := totalAggreation[station]
			if ok {
				stationAgg.Min = min(stationAgg.Min, value.Min)
				stationAgg.Max = max(stationAgg.Max, value.Max)
				stationAgg.Sum = stationAgg.Sum + value.Sum
				stationAgg.Count = stationAgg.Count + value.Count
			} else {
				totalAggreation[station] = &MeasurementAgg{
					Min:   value.Min,
					Max:   value.Max,
					Sum:   value.Sum,
					Count: value.Count,
				}
			}
		}

	}

	for _, station := range slices.Sorted(maps.Keys(totalAggreation)) {
		stationAgg := totalAggreation[station]
		fmt.Println(station, stationAgg.Min, math.Round((stationAgg.Sum/float64(stationAgg.Count))*10.0)/10.0, stationAgg.Max)
	}
}
