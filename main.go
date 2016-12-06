package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/dustin/go-humanize"
	fs "io/ioutil"
	"math"
	"os"
	"os/signal"
	"runtime"
	"time"
)

const documentFormat = "{\"entrynumber\":%d,\"entry\":\"%s\",\"source\":\"%s\"}"

func main() {
	threads := flag.Int("threads", runtime.NumCPU(), "The number of threads (GOMAXPROCS)")
	workers := flag.Int("workers", int(math.Min(float64(runtime.NumCPU()), math.Max(2, float64(runtime.NumCPU()-4)))), "The number of workers (goroutines)")
	batchSize := flag.Int("batch-size", 1000, "The number of documents to submit to elasticsearch at once")
	index := flag.String("index", "db", "The index to put documents in")
	db_type := flag.String("type", "database", "The type of the document (it's database by default because each line is a 'database' line)")
	maxBytes := flag.Int("max-bytes", 1E9, "The max number of bytes to send in each batch")
	esUrl := flag.String("es-url", "http://localhost:9200", "The base URL for Elastic Search")
	maxUploads := flag.Int("max-uploads", 8, "The maximum total number of uploads")
	bufferSize := flag.Int("buffer", runtime.NumCPU()*1000, "The number of lines to hold in memory before they can be handled...")

	flag.Parse()

	var startingDir string
	var start time.Time
	var totalLines int64
	totalLines = 0
	if len(flag.Args()) == 0 {
		flag.PrintDefaults()
		return
	}

	startingDir = flag.Arg(0)

	runtime.GOMAXPROCS(*threads)
	indexLine := fmt.Sprintf("{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\"}}", *index, *db_type)
	esClient := NewESClient(*esUrl, *index)
	totalCounts := make(chan uint64, *workers)
	in, wg := startLineReaderPool(*bufferSize, *workers, processLinesFunc(indexLine, documentFormat, *batchSize, *maxBytes, *maxUploads, totalCounts, esClient.BulkUp))

	err := esClient.SetupForBulk()
	if err != nil {
		panic(err)
	}

	go func(in chan<- *LineData, startingDir string) {
		handleFile := func(filePath string, fileName string) int64 {
			file, err := os.Open(filePath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[!] Cannot open file %s\n", filePath)
				return 0
			}
			defer file.Close()
			fmt.Fprintf(os.Stderr, "[%s] Opened\n", filePath)

			scanner := bufio.NewScanner(file)
			scanner.Split(bufio.ScanLines)

			var lineNumber int64
			for scanner.Scan() {
				in <- &LineData{&fileName, lineNumber, scanner.Text()}
				lineNumber++
				totalLines++
			}
			fmt.Fprintf(os.Stderr, "[%s] Processed %s lines\n", filePath, humanize.Comma(lineNumber))
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
		wg.Add(1)
		defer wg.Done()

		//actual bootstrap
		fmt.Fprintf(os.Stderr, "[!] Starting everything...\n")
		start = time.Now()
		totalLines = handleDir(startingDir)
		close(in)
	}(in, startingDir)

	initCount, _ := esClient.ReadCurrentCount()
	printStat := func(key, val string) {
		fmt.Fprintf(os.Stderr, "[STATS] %s: %s\n", key, val)
	}

	printStats := func() {
		linesPerSec := func(count int64) int64 {
			return (count * (1000 * 1000 * 1000)) / (time.Now().UnixNano() - start.UnixNano())
		}

		count, _ := esClient.ReadCurrentCount()
		var memStat runtime.MemStats
		runtime.ReadMemStats(&memStat)

		fmt.Fprintf(os.Stderr, "\n")
		printStat("Goroutines", fmt.Sprintf("%d", runtime.NumGoroutine()))
		printStat("Memory In Use", humanize.Bytes(memStat.Alloc))
		printStat("Memory From System", humanize.Bytes(memStat.Sys))
		printStat("Number of GCs", fmt.Sprintf("%d", memStat.NumGC))
		printStat("Total Time GC Paused (ms)", fmt.Sprintf("%.03f", float64(memStat.PauseTotalNs)/float64(1000*1000)))
		if memStat.NumGC > 0 {
			printStat("Average Time GC Paused (ms)", fmt.Sprintf("%.04f", float64(memStat.PauseTotalNs)/float64(1000*1000*memStat.NumGC)))
		}
		printStat("ES Count", humanize.Comma(count))
		if initCount != 0 {
			printStat("ES Count Delta", humanize.Comma(count-initCount))
		}
		printStat("Started", humanize.Time(start))
		printStat("Lines Loaded", humanize.Comma(totalLines))
		printStat("Lines Per Second (ES)", humanize.Comma(linesPerSec(count-initCount)))
		printStat("Lines Per Sec (Internal)", humanize.Comma(linesPerSec(totalLines)))
		fmt.Fprintf(os.Stderr, "\n")
	}

	go func() {
		for {
			printStats()
			<-time.After(time.Second * 5)
		}
	}()

	handleSignals(esClient)

	wg.Wait()
	if err := esClient.Cleanup(); err != nil {
		panic(err)
	}
	totalDispatched := uint64(0)
	for i := 0; i < *workers; i++ {
		totalDispatched = totalDispatched + <-totalCounts
	}

	fmt.Fprintf(os.Stderr, "\n[!] Finished everything!\n[!]Lines Handled: %d", totalDispatched)

	printStats()
}

func handleSignals(client *ESClient) {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		fmt.Printf("[QUIT] Cleaning up ES\n")
		client.Cleanup()
		fmt.Printf("[QUIT] Finished...\n")
		os.Exit(0)
	}()
}
