package main

import (
	"bufio"
	"flag"
	"fmt"
	fs "io/ioutil"
	"os"
	"runtime"
	"time"
)

const documentFormat = "{\"entrynumber\":%d,\"entry\":\"%s\",\"source\":\"%s\"}"

func main() {
	threads := flag.Int("threads", runtime.NumCPU(), "The number of threads (GOMAXPROCS)")
	workers := flag.Int("workers", runtime.NumCPU(), "The number of workers (goroutines)")
	batchSize := flag.Int("batch-size", 9000, "The number of documents to submit to elasticsearch at once")
	index := flag.String("index", "db", "The index to put documents in")
	db_type := flag.String("type", "database", "The type of the document (it's database by default because each line is a 'database' line)")
	maxBytes := flag.Int("max-bytes", 1000000000, "The max number of bytes to send in each batch")
	esUrl := flag.String("es-url", "http://localhost:9200", "The base URL for Elastic Search")

	flag.Parse()

	var startingDir string
	var start, end time.Time
	var totalLines int64
	if len(flag.Args()) == 0 {
		flag.PrintDefaults()
		return
	}

	startingDir = flag.Arg(0)

	runtime.GOMAXPROCS(*threads)
	esFullURL := fmt.Sprintf("%s/_bulk", *esUrl)
	indexLine := fmt.Sprintf("{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\"}}", *index, *db_type)
	in, wg := startLineReaderPool((*batchSize) * (*workers) * 2, *workers, processLinesFunc(indexLine, documentFormat, *batchSize, *maxBytes, uploadToESFunc(esFullURL)))

	go func(in chan<- *LineData, startingDir string) {
		handleFile := func(filePath string, fileName string) int64 {
			file, err := os.Open(filePath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[!] Cannot open file %s\n", filePath)
				panic(err)
			}
			defer file.Close()
			fmt.Fprintf(os.Stderr, "[%s] Opened\n", filePath)

			scanner := bufio.NewScanner(file)
			scanner.Split(bufio.ScanLines)

			var lineNumber int64
			for scanner.Scan() {
				in <- &LineData{&fileName, lineNumber, scanner.Text()}
				lineNumber++
			}
			fmt.Fprintf(os.Stderr, "[%s] Queued %d lines\n", filePath, lineNumber)
			return lineNumber
		}

		var handleDir func(string) int64 //needs to be defined because this is recursive and cannot reference itself while defining itself
		handleDir = func(dirName string) int64 {
			dir, err := fs.ReadDir(dirName)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Cannot open directory %s\n", startingDir)
				panic(err)
			}
			fmt.Fprintf(os.Stderr, "[%s] Loading directory\n", dirName)

			var lineCount int64
			for i := range dir {
				file := dir[i]
				if file.IsDir() {
					lineCount = lineCount + handleDir(dirName+"/"+file.Name())
					continue
				}

				lineCount = lineCount + handleFile(dirName+"/"+file.Name(), file.Name())
			}
			fmt.Fprintf(os.Stderr, "[%s] Finished directory\n", dirName)
			return lineCount
		}

		//actual bootstrap
		fmt.Fprintf(os.Stderr, "[!] Starting everything...\n")
		start = time.Now()
		totalLines = handleDir(startingDir)

		close(in)
		fmt.Fprintf(os.Stderr, "[!] Finished everything!\n")
	}(in, startingDir)

	wg.Wait()
	end = time.Now()
	linesPerSec := float64(totalLines * (1000 * 1000 * 1000)) / (float64(end.UnixNano()-start.UnixNano()))
	fmt.Fprintf(os.Stderr, "\n[I] Lines handled per second: %.02f\n", linesPerSec)
}
