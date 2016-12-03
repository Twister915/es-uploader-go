package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"testing"
)

func BenchmarkProcessingFiles(b *testing.B) {
	batchSize := 9000
	workers := runtime.NumCPU()
	indexLine := fmt.Sprintf("{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\"}}", "db", "database")
	workerFunc := processLinesFunc(indexLine, documentFormat, batchSize)
	in, wg := startLineReaderPool(batchSize*workers, workers, workerFunc)
	go func() {
		defer close(in)
		fileName := "CheapAssGamer.com.txt"
		file, _ := os.Open("test_dbs/" + fileName)
		defer file.Close()
		scanner := bufio.NewScanner(file)
		scanner.Split(bufio.ScanLines)
		for n := 0; n < b.N; n++ {
			in <- &LineData{&fileName, int64(n), scanner.Text()}
		}
	}()
	wg.Wait()
}
