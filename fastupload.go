package main

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"unicode/utf8"
)

type LineData struct {
	Filename    *string
	LineNumber  int64
	LineContent string
}

//this will start the "goroutine pool" that will call the worker func with the <- version of the inChannel
func startLineReaderPool(buffer, workers int, workerFunc func(work <-chan *LineData)) (inChan chan<- *LineData, waitGroup *sync.WaitGroup) {
	inCh := make(chan *LineData, buffer) //this is the actual channel
	inChan = inCh                        //this is for the return value and changes the channel to send only
	waitGroup = &sync.WaitGroup{}        //create a waitGroup instance and get the address (for return value named waitGroup)

	for i := 0; i < workers; i++ { //create one goroutine for each worker we want
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done() //make sure we decrement
			workerFunc(inCh)       //hand off ("blocking" in a sense, this will truly block when reading from the in chan, most likely, since most worker funcs just range over the channel)
		}()
	}

	return /* return values are named and assigned above */
}

//function currying! this sets up a worker function given some constant parameters
func processLinesFunc(indexString, itemFormat string, batch, maxBytes int, maxHttp chan bool, uploadFunc func(*bytes.Buffer) error) func(<-chan *LineData) {
	growLength := len(indexString) + len(itemFormat) + 2 /* two newlines */ - 6 /* 3 formatters at 2 chars each */
	//the actual worker function
	return func(lines <-chan *LineData) {
		var buffer *bytes.Buffer //create a buffer to hold the JSON representation we're building

		var pool sync.Pool
		pool.New = func() interface{} {
			buffer := bytes.Buffer{}
			return &buffer
		}

		resetBuf := func() {
			buffer = pool.Get().(*bytes.Buffer)
			buffer.Reset()
		}
		//note to self: put a sync.Pool here with a New that returns a new buffer, and use it to pass a buffer instance to the http func below

		count := 0 //keep a running count of the number of lines we've processed (the number of lines in buffer is therefore 2 * count)

		flush := func() { //create a flushing function (sends HTTP request)
			//fmt.Fprintf(os.Stderr, "%d\n", count)
			maxHttp <- true
			go func(buf *bytes.Buffer) { //split off to do our HTTP request work
				defer func() {
          pool.Put(buf)
          <-maxHttp
        }()
				err := uploadFunc(buf)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[!!] Error performing upload task...\n   %s\n[!!]Retrying in one second!", err)
				}
			}(buffer)
			resetBuf() //and reset state (buffer & count)
			count = 0
		}

		resetBuf()
		for lineData := range lines { //"range lines" will do exactly what we want here... when it's closed, we break out, and when it's open, we always get the next line we can process
			line := lineData.LineContent //the actual LINE is here
			jsonFormat(&line)            //pass it through our pretty efficient JSON escape thingy
			buffer.Grow(len(line) + growLength)

			buffer.WriteString(indexString) //this is for elasticsearch, and feels super redundant, but for each document we must supply index data
			buffer.WriteRune('\n')

			buffer.WriteString(fmt.Sprintf(itemFormat, lineData.LineNumber, line, *lineData.Filename)) //sprintf the data into the format given to us by our creator
			buffer.WriteRune('\n')

			count++ //we've added one thing, so increment the number of things

			if count >= batch || buffer.Len() > maxBytes { //and if we've hit a threshold, we should immediately flush
				flush()
			}
		}

		if count > 0 { //if we've got extra stuff left, we should flush... seeing as the channel is closed, this will be the final flush
			flush()
		}
	}
}

const hex = "0123456789abcdef"

func jsonFormat(line *string) {
	var buf *bytes.Buffer
	//now THIS is smart, I think...
	//every time the "sanitizer" (this whole function) wants to change something in the string
	//it calls this function which will check if the buffer has been created
	//now the important part: IT WILL NOT COPY THE STRING, OR CREATE A BUFFER, IF IT NEVER REPLACES ANYTHING :D
	//check below near the assignment statement for the main function for more details
	checkBufSane := func(i int) {
		if buf != nil {
			return
		}
		buf = &bytes.Buffer{}
		buf.WriteString(string([]rune(*line)[:i]))
	}

	runes := []rune(*line)
RUNELOOP:
	for i := range runes {
		b := runes[i]
		switch b {
		case '\\', '"':
			checkBufSane(i)
			buf.WriteByte('\\')
			buf.WriteRune(b)
			continue RUNELOOP
		case '\n':
			checkBufSane(i)
			buf.WriteByte('\\')
			buf.WriteByte('n')
			continue RUNELOOP
		case '\r':
			checkBufSane(i)
			buf.WriteByte('\\')
			buf.WriteByte('r')
			continue RUNELOOP
		case '\t':
			checkBufSane(i)
			buf.WriteByte('\\')
			buf.WriteByte('t')
			continue RUNELOOP
		}

		if !(0x20 <= b && b < utf8.RuneSelf) {
			checkBufSane(i)
			buf.WriteString(`\u00`)
			buf.WriteByte(hex[byte(b)>>4])
			buf.WriteByte(hex[byte(b)&0xF])
			continue
		}

		if buf != nil {
			buf.WriteRune(b)
		}
	}

	//see look we only ever modify the value of the pointer "line" if we changed something,
	//otherwise we basically did nothing other than check each char
	if buf != nil {
		*line = buf.String()
	}
}
