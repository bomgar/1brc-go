package main

import (
	"io"
	"log"
	"os"
	"runtime/pprof"
	"syscall"

	"github.com/bomgar/1brc-go/process"
)

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

	processFile(os.Args[1], os.Stdout)
}

func processFile(filePath string, writer io.Writer) {
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

	process.ProcessData(data, writer)
}
