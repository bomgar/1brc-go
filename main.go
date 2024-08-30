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

	"github.com/bomgar/1brc-go/fastfloat"
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

func readFile(data []byte, batches chan<- map[string]*MeasurementAgg) {

	nChunks := runtime.NumCPU()

	chunkSize := len(data) / nChunks
	if chunkSize == 0 {
		chunkSize = len(data)
	}

	chunks := make([]int, 0, nChunks)
	offset := 0
	for offset < len(data) {
		offset += chunkSize
		if offset >= len(data) {
			chunks = append(chunks, len(data))
			break
		}

		nlPos := bytes.IndexByte(data[offset:], '\n')
		if nlPos == -1 {
			chunks = append(chunks, len(data))
			break
		} else {
			offset += nlPos + 1
			chunks = append(chunks, offset)
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(chunks))

	start := 0
	for _, chunk := range chunks {
		go func(data []byte) {
			processChunk(data, batches)
			wg.Done()
		}(data[start:chunk])
		start = chunk
	}
	wg.Wait()
	close(batches)
}

func processChunk(data []byte, batches chan<- map[string]*MeasurementAgg) {
	reader := bytes.NewReader(data)
	scanner := bufio.NewScanner(reader)
	const batchSize = 10000
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
	batches <- agg
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
		log.Fatalf("Could not mmap file", err)
	}

	defer syscall.Munmap(data)
	measurementBatches := make(chan map[string]*MeasurementAgg, 50)
	go readFile(data, measurementBatches)

	agg := make(map[string]*MeasurementAgg, 500)
	for subAgg := range measurementBatches {
		for station, value := range subAgg {
			stationAgg, ok := agg[station]
			if ok {
				stationAgg.Min = min(stationAgg.Min, value.Min)
				stationAgg.Max = max(stationAgg.Max, value.Max)
				stationAgg.Sum = stationAgg.Sum + value.Sum
				stationAgg.Count = stationAgg.Count + value.Count
			} else {
				agg[station] = &MeasurementAgg{
					Min:   value.Min,
					Max:   value.Max,
					Sum:   value.Sum,
					Count: value.Count,
				}
			}
		}

	}

	for _, station := range slices.Sorted(maps.Keys(agg)) {
		stationAgg := agg[station]
		fmt.Println(station, stationAgg.Min, math.Round((stationAgg.Sum/float64(stationAgg.Count))*10.0)/10.0, stationAgg.Max)
	}
}
