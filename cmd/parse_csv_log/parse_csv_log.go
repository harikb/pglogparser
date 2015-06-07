package main

import (
	//"bufio"
	"bytes"
	"fmt"
	"os"

	"github.com/harikb/yopen"

	"github.com/harikb/yacr"
	// clone of "github.com/gwen/yacr" where the only change is to
	// use a clone of Go's bufio package with one const value changed
	// two clones and one edit for just changing MaxScanTokenSize

	flag "github.com/ogier/pflag"
	dprofile "github.com/pkg/profile"
	"io"
	"log"
	"regexp"
	"runtime"
	"sync"
)

type cmdArgs struct {
	cpuProfile  bool
	memProfile  bool
	logfilename string
	numReaders  int32
}

// these are globals
var args cmdArgs

var reValues, reComments, reMultiSpaces, reMultiValues,
	reNumbers *regexp.Regexp

// LogFile represents the input file/reader with a name
type LogFile struct {
	filename string
}

var fileChan chan LogFile

func init() {

	fileChan = make(chan LogFile)

	/* A single quote followed by
	// something that is either "not a single-quote or backslash" OR
	// "backslash followed by some other non-empty character"
	// followed by a single quote
	*/

	reValues = regexp.MustCompile(`'([^'\\]|\\.)*'`)
	// tested here http://play.golang.org/p/oew9c4AfIS
	// permalink: https://gist.github.com/harikb/3ed552d3996199f0208e

	reComments = regexp.MustCompile(`--([^\n]*)\n`)

	reMultiSpaces = regexp.MustCompile(`\s+`)

	// Convert a list of literals  (?,?,?,?,?) to (?).
	// Sometimes queries use variable number of arguments in an 'in' list

	reMultiValues = regexp.MustCompile(`(\?|null|true|false|infinity)((,|\s|-)+(\?|null|true|false|infinity))+`)

	reNumbers = regexp.MustCompile(`(\d+(\.\d+)?(e(\+|-)\d+)?)`)
	// tested here http://play.golang.org/p/99qyocB8ij
	// permalink: https://gist.github.com/harikb/eea68d55d72410df8066

}

func canonicalizeQuery2(query []byte) (canonicalQuery []byte, err error) {

	inQuote := false
	alreadySubed := false
	inComment := false
	inSpace := false
	inNumber := false
	pv := byte(0) // pv == previous v
	n := byte(0)  // to peek at the next char
	l := len(query)
	for k, v := range query {

		uv := v // uv stands for unmodified v - since we change with value of v below, setting to zero sometimes.

		if k < l-1 { // peek at next char
			n = query[k+1]
		} else {
			n = 0
		}

		if !inComment {
			if v == '\'' && pv != '\\' {
				inQuote = !inQuote // toggle inQuote
				if !inQuote {
					alreadySubed = false
					v = 0 // skip the quote as well
				}
			}
		}
		if !inQuote {
			if !inComment && v == '-' && n == '-' {
				inComment = true
			}
			if inComment && v == '\n' {
				inComment = false
			}
		}
		if inQuote {
			if !alreadySubed {
				v = '?'
				alreadySubed = true
			} else {
				v = 0
			}
		}
		if inComment {
			v = 0
		}
		if v == ' ' || v == '\t' || v == '\v' || v == '\r' || v == '\n' {
			if !inSpace {
				v = ' '
				inSpace = true
			} else {
				v = 0
			}
		} else if !inComment {
			inSpace = false
		}

		followedByANumber := false
		if inNumber && n >= '0' && n <= '9' {
			followedByANumber = true
		}

		if (v >= '0' && v <= '9') || (inNumber && followedByANumber &&
			(v == '.' || v == 'e' || v == '+' || v == '-')) {
			if !inNumber {
				v = '?'
				inNumber = true
			} else {
				v = 0
			}
		} else {
			inNumber = false
		}
		if v != 0 {
			if v >= 'A' && v <= 'Z' {
				v += 32 // convert to lower case
			}
			canonicalQuery = append(canonicalQuery, v)
		}

		if v == '\\' && pv == '\\' {
			pv = 0 // so we won't consider this \ as an escape for next character.
		} else {
			pv = uv
		}
	}

	if inQuote {
		err = fmt.Errorf("Unterminated quote")
	}
	// NOTE: query may end while still in 'inComment'

	canonicalQuery = bytes.TrimRight(canonicalQuery, "; ")

	return
}

func canonicalizeQuery(query []byte) []byte {

	query = bytes.ToLower(query)
	//log.Printf("Query is >%s<", string(query))
	query = bytes.TrimRight(query, "; ")
	//log.Printf("Query is >%s<", string(query))
	if bytes.Contains(query, []byte("'")) {
		query = reValues.ReplaceAllLiteral(query, []byte("?"))
	}
	//log.Printf("Query is >%s<", string(query))
	if bytes.Contains(query, []byte("--")) {
		query = reComments.ReplaceAllLiteral(query, []byte(""))
	}
	//log.Printf("Query is >%s<", string(query))
	query = reMultiSpaces.ReplaceAllLiteral(query, []byte(" "))
	//log.Printf("Query is >%s<", string(query))
	query = reNumbers.ReplaceAllLiteral(query, []byte("?"))
	//log.Printf("Query is >%s<", string(query))
	//if bytes.Contains(query, []byte("?")) {
	//	query = reMultiValues.ReplaceAllLiteral(query, []byte("?"))
	//}
	//log.Printf("Query is >%s<", string(query))

	return query
}

func main() {

	flag.BoolVarP(&args.cpuProfile, "cprofile", "", false, "CPU profile this run")
	flag.BoolVarP(&args.memProfile, "mprofile", "", false, "Memory profile this run")
	flag.Int32VarP(&args.numReaders, "num-readers", "n", 3, "Read this many files in parallel")

	flag.Parse()

	/*  Enable this code for testing specific queries.
	q2, err := canonicalizeQuery2([]byte(`query to test`))
	if err != nil {
		log.Printf("%s", err)
	} else {
		log.Printf("Original: %s\nCanonical: %s\n", q1, q2)
	}
	return
    */

	runtime.GOMAXPROCS(runtime.NumCPU())
	log.Printf("GOMAXPROCS=%v", runtime.GOMAXPROCS(0))

	if args.cpuProfile && args.memProfile {
		log.Fatalf("Please pass only one of --cprofile or --mprofile")
	} else if args.cpuProfile {
		defer dprofile.Start(dprofile.CPUProfile, dprofile.ProfilePath(".")).Stop()
	} else if args.memProfile {
		defer dprofile.Start(dprofile.MemProfile, dprofile.ProfilePath(".")).Stop()
	}

	var loaderGroup sync.WaitGroup // See http://stackoverflow.com/a/19208908
	loaderGroup.Add(int(args.numReaders))

	for i := 0; i < int(args.numReaders); i++ {
		go parseFileWorker(&loaderGroup)
	}
	log.Printf("Started %d file loader threads", args.numReaders)

	numFiles := 0
	for _, filename := range flag.Args() {
		fileChan <- LogFile{filename}
		numFiles++
		if numFiles%20 == 0 {
			log.Printf("Loaded %d files", numFiles)
		}
	}

	log.Printf("Loaded %d files", numFiles)
	log.Printf("Closing file channel.")
	close(fileChan)
	log.Printf("Waiting for loader threads to finish")
	loaderGroup.Wait()
	log.Printf("All threads completed")

}

func parseFileWorker(wg *sync.WaitGroup) {

	totalLines := 0
Loop:
	for {

		select {

		case f, more := <-fileChan:
			if !more {
				break Loop
			}
			//log.Printf("Loading '%s'", f)
			l, err := parseFile(f.filename)
			if err != nil {
				log.Printf("Unable to parse file %v: %v", f.filename, err)
			}
			totalLines += l
			log.Printf("Worker: %d queries analyzed", totalLines)
		}
	}
	log.Printf("Ending worker: %d queries analyzed", totalLines)
	wg.Done()
}

func parseFile(filename string) (queryCount int, err error) {

	rawReader, err := yopen.NewReader(filename)
	if err != nil {
		log.Fatalf("Unable to open file %v: %v", args.logfilename, err)
	}

	csvReader := yacr.DefaultReader(rawReader)
	csvWriter := yacr.DefaultWriter(os.Stdout)

	var c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c14b, c15, c16, c17, c18, c19, c20, c21, c22, c23 []byte
	queryCount = 0
	for {
		queryCount++
		record, err := csvReader.ScanRecord(&c1, &c2, &c3, &c4, &c5, &c6, &c7, &c8, &c9, &c10, &c11, &c12, &c13, &c14, &c15, &c16, &c17, &c18, &c19, &c20, &c21, &c22, &c23)

		if err == io.EOF || record == 0 {
			break
		}
		if err != nil {
			log.Fatalf("Error in reading file %s at record %d, line %d: %v",
				filename, queryCount, csvReader.LineNumber(), err)
		}
		//log.Printf("LINE %d >%s< >%s< >%s< >%s< >%s< >%s< >%s< >%s< >%s< >%s< >%s< >%s< >%s< >%s< >%s< >%s< >%s< >%s< >%s< >%s< >%s< >%s< >%s< ", rNum, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23)
		if len(c14) > 0 {
			c14b, err = canonicalizeQuery2(c14)
			if err != nil {
				log.Printf("ERROR in %s:%d (csv line %d), %v\n%s\nReplacement:%s", filename, csvReader.LineNumber(), queryCount, c14, err, c14b)
			}
		} else {
			c14b = []byte{}
		}

		errB := csvWriter.WriteRecord(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14b, c15, c16, c17, c18, c19, c20, c21, c22, c23)
		if !errB {
			log.Fatalf("Unable to write output: %v", errB)
		}
	}
	err = rawReader.Close()
	return
}
