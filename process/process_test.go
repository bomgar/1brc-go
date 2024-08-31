package process

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProcess(t *testing.T) {

	// Existing test (kept for reference)
	t.Run("Basic Test", func(t *testing.T) {
		input := `Tbilisi;1.6
Tehran;10.4
Tehran;11.4
`
		expected := `Tbilisi 1.6 1.6 1.6
Tehran 10.4 10.9 11.4
`
		runTest(t, input, expected)

	})

	// Additional Test Cases

	t.Run("Single Station, Multiple Values", func(t *testing.T) {
		input := `London;5.2
London;8.1
London;6.7
`
		expected := `London 5.2 6.7 8.1
`
		runTest(t, input, expected)
	})

	t.Run("Multiple Stations, Single Value Each", func(t *testing.T) {
		input := `Moscow;-3.5
Berlin;2.0
Tokyo;15.8
`
		expected := `Berlin 2 2 2
Moscow -3.5 -3.5 -3.5
Tokyo 15.8 15.8 15.8
`
		runTest(t, input, expected)
	})

	t.Run("Empty Input", func(t *testing.T) {
		input := ``
		expected := ``
		runTest(t, input, expected)
	})

}

func runTest(t *testing.T, input, expected string) {
	buffer := new(strings.Builder)
	ProcessData([]byte(input), buffer)
	result := buffer.String()
	assert.Equal(t, expected, result)
}
