package main

import (
	"bufio"
	"fmt"
	"log"
	"maps"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
)

type MeasurementAgg struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int64
}

func main() {
	file, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatalf("Could not open input file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	agg := make(map[string]MeasurementAgg)
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
			agg[name] = stationAgg
		} else {
			agg[name] = MeasurementAgg{
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
