package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type AggLine struct {
	Station string
	Min     float64
	Avg     float64
	Max     float64
}

func TestProcess1000(t *testing.T) {
	buffer := new(strings.Builder)
	processFile("testdata/measurements-1000.txt", buffer)
	result := buffer.String()

	expected, err := os.ReadFile("testdata/measurements-1000-agg.txt")

	require.NoError(t, err)

	resultSplit := strings.Split(result, "\n")
	expectedSplit := strings.Split(string(expected), "\n")

	require.Equal(t, len(resultSplit), len(expectedSplit))

	for i, resultString := range resultSplit {
		expectedString := expectedSplit[i]
		if resultString == "" && expectedString == "" {
			continue
		}

		result := parseLine(t, resultString)
		expected := parseLine(t, expectedString)

		assert.Equal(t, expected.Station, result.Station)
		assert.InDelta(t, expected.Min, result.Min, 0.01, "Min mismatch")
		assert.InDelta(t, expected.Avg, result.Avg, 0.2, fmt.Sprintf("Avg mismatch: %s - %s", resultString, expectedString))
		assert.InDelta(t, expected.Max, result.Max, 0.01, "Max mismatch")
	}

}

func parseLine(t *testing.T, line string) AggLine {
	split := strings.Split(line, " ")
	require.GreaterOrEqual(t, len(split), 4, fmt.Sprintf("Invalid line: %s", line))

	station := strings.Join(split[:len(split)-3], " ")

	min, err := strconv.ParseFloat(split[len(split)-3], 64)
	require.NoError(t, err)
	avg, err := strconv.ParseFloat(split[len(split)-2], 64)
	require.NoError(t, err)
	max, err := strconv.ParseFloat(split[len(split)-1], 64)
	require.NoError(t, err)

	return AggLine{
		Station: station,
		Min:     min,
		Avg:     avg,
		Max:     max,
	}
}
