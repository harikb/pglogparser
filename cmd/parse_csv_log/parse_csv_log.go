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
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
)

type cmdArgs struct {
	cpuProfile        bool
	memProfile        bool
	canonicalizeQuery bool
	tsv               bool
	header            bool
	filterSessionID   string
	logfilename       string
	numReaders        int32
	debug             bool
}

// these are globals
var args cmdArgs

var reValues, reComments, reMultiSpaces, reMultiValues,
	reNumbers *regexp.Regexp

// LogFile represents the input file/reader with a name
type LogFile struct {
	filename string
}

// TimeFormat is the format of query timestamp in postgresql csv log format
var TimeFormat = "2006-01-02 15:04:05.000 MST"

// TimeFormatNoMillis is the format of session timestamp in postgresql csv log format
var TimeFormatNoMillis = "2006-01-02 15:04:05 MST"

/*
CsvLog is defined above. It mimics the format of postgresql table definition given above.
From: http://www.postgresql.org/docs/9.4/static/runtime-config-logging.html

CREATE TABLE postgres_log
(
  log_time timestamp(3) with time zone,
  user_name text,
  database_name text,
  process_id integer,
  connection_from text,
  session_id text,
  session_line_num bigint,
  command_tag text,
  session_start_time timestamp with time zone,
  virtual_transaction_id text,
  transaction_id bigint,
  error_severity text,
  sql_state_code text,
  message text,
  detail text,
  hint text,
  internal_query text,
  internal_query_pos integer,
  context text,
  query text,
  query_pos integer,
  location text,
  application_name text,
  PRIMARY KEY (session_id, session_line_num)
);

*/
type CsvLog struct {
	logTime              time.Time `colName:""`
	logTimeStr           []byte    `colName:"logTime"`
	userName             []byte
	databaseName         []byte
	processID            []byte
	connectionFrom       []byte
	sessionID            []byte
	sessionLineNum       []byte
	commandTag           []byte
	sessionStartTime     time.Time `colName:""`
	sessionStartTimeStr  []byte    `colName:"sessionStartTime"`
	virtualTransactionID []byte
	transactionID        []byte
	errorSeverity        []byte
	sqlStateCode         []byte
	message              []byte
	detail               []byte
	hint                 []byte
	internalQuery        []byte
	internalQueryPos     []byte
	context              []byte
	query                []byte
	queryPos             []byte
	location             []byte
	applicationName      []byte
}

var fileChan chan LogFile

func init() {

	fileChan = make(chan LogFile)

}

func unfoldQuery(query []byte, canonicalize bool) (canonicalQuery []byte, err error) {
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
					if canonicalize {
						v = 0 // skip the quote as well
					}
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
		if inQuote && canonicalize {
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
		if !inQuote && (v == ' ' || v == '\t' || v == '\v' || v == '\r' || v == '\n') {
			if !inSpace {
				v = ' '
				inSpace = true
			} else {
				v = 0
			}
		} else if !inComment {
			inSpace = false
		}

		if canonicalize {

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
		}

		if v != 0 {
			if canonicalize && v >= 'A' && v <= 'Z' {
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

func main() {

	flag.BoolVarP(&args.cpuProfile, "cprofile", "", false, "CPU profile this run")
	flag.BoolVarP(&args.memProfile, "mprofile", "", false, "Memory profile this run")
	flag.Int32VarP(&args.numReaders, "num-readers", "n", 3, "Read this many files in parallel")
	flag.BoolVarP(&args.canonicalizeQuery, "canonicalize-query", "c", false, "Canonicalize statements")
	flag.BoolVarP(&args.tsv, "tsv", "t", false, "Unfold lines and ouput to tsv (usually, for pipe to unix cut)")
	flag.BoolVarP(&args.header, "header", "H", false, "Print a line of header")
	flag.StringVarP(&args.filterSessionID, "filter-session-id", "f", "", "show only queries from this session id")
	flag.BoolVarP(&args.debug, "debug-print", "", false, "Print with named columns (debugging only)")

	flag.Parse()

	/*  Enable this code for testing specific queries.
	q2, err := canonicalizeQuery([]byte(`query to test`))
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

func logHeader(csvWriter *yacr.Writer, rec CsvLog) (err error) {

	var headers []string
	var sep = "," // implicit type is rune
	if args.tsv {
		sep = "\t"
	}

	st := reflect.TypeOf(rec)
	for i := 0; i < st.NumField(); i++ {
		field := st.Field(i)
		header := field.Name
		if field.Tag != "" {
			header = field.Tag.Get("colName")
		}
		if header != "" {
			headers = append(headers, header)
		}
	}

	errB := csvWriter.WriteString(strings.Join(headers, sep))
	if !errB {
		err = fmt.Errorf("Unable to write output: %v", errB)
	}

	return
}

func logRecord(csvWriter *yacr.Writer, rec CsvLog) (err error) {

	// yacr.csvWriter.writeRecord return a boolean status, unfortunately.
	errB := csvWriter.WriteRecord(
		// rec.logTime              time.Time
		rec.logTimeStr,
		rec.userName,
		rec.databaseName,
		rec.processID,
		rec.connectionFrom,
		rec.sessionID,
		rec.sessionLineNum,
		rec.commandTag,
		// rec.sessionStartTime     time.Time
		rec.sessionStartTimeStr,
		rec.virtualTransactionID,
		rec.transactionID,
		rec.errorSeverity,
		rec.sqlStateCode,
		rec.message,
		rec.detail,
		rec.hint,
		rec.internalQuery,
		rec.internalQueryPos,
		rec.context,
		rec.query,
		rec.queryPos,
		rec.location,
		rec.applicationName)
	if !errB {
		err = fmt.Errorf("Unable to write output: %v", errB)
	}
	return
}

func parseFile(filename string) (queryCount int, err error) {

	rawReader, err := yopen.NewReader(filename)
	if err != nil {
		log.Fatalf("Unable to open file %v: %v", args.logfilename, err)
	}

	var sep byte = ',' // implicit type is rune
	if args.tsv {
		sep = '\t'
	}
	csvReader := yacr.DefaultReader(rawReader)
	csvWriter := yacr.NewWriter(os.Stdout, sep, true)

	var cq []byte
	queryCount = 0

	var incompleteQueries = map[string]CsvLog{}
	headerPrinted := false

	for {
		queryCount++
		rec := CsvLog{}
		record, err := csvReader.ScanRecord(
			// &rec.logTime              time.Time
			&rec.logTimeStr,
			&rec.userName,
			&rec.databaseName,
			&rec.processID,
			&rec.connectionFrom,
			&rec.sessionID,
			&rec.sessionLineNum,
			&rec.commandTag,
			// &rec.sessionStartTime     time.Time
			&rec.sessionStartTimeStr,
			&rec.virtualTransactionID,
			&rec.transactionID,
			&rec.errorSeverity,
			&rec.sqlStateCode,
			&rec.message,
			&rec.detail,
			&rec.hint,
			&rec.internalQuery,
			&rec.internalQueryPos,
			&rec.context,
			&rec.query,
			&rec.queryPos,
			&rec.location,
			&rec.applicationName)

		if err == io.EOF || record == 0 {
			break
		}

		if err != nil {
			log.Printf("Error at file %s at record %d, line %d: %v",
				filename, queryCount, csvReader.LineNumber(), err)
			continue
		}

		rec.logTime, err = time.Parse(TimeFormat, string(rec.logTimeStr))
		if err == nil { // err/error is checked later below
			rec.sessionStartTime, err = time.Parse(TimeFormatNoMillis, string(rec.sessionStartTimeStr))
			// err/error is checked later below
		}

		if err != nil {
			log.Fatalf("Error in reading file %s at record %d, line %d, record:%+v: %v",
				filename, queryCount, csvReader.LineNumber(), rec, err)
		}

		hasDuration := bytes.Contains(rec.message, []byte("duration:"))
		hasStatement := bytes.Contains(rec.message, []byte("statement:"))

		if hasDuration && !hasStatement {
			if k, ok := incompleteQueries[string(rec.sessionID)]; ok {
				k.message = append(rec.message, k.message...) // prepend duration
				rec = k
			}
		}

		if len(rec.message) > 0 && (args.canonicalizeQuery || args.tsv) {
			cq, err = unfoldQuery(rec.message, args.canonicalizeQuery)
			if err != nil {
				log.Printf("ERROR in %s:%d (csv line %d), %v\n%s\nReplacement:%s", filename, csvReader.LineNumber(), queryCount, rec.message, err, cq)
			} else {
				rec.message = cq
			}
		}

		if args.filterSessionID == "" || (args.filterSessionID == string(rec.sessionID)) {

			if args.debug {

				log.Printf("%+v", rec)

			} else {
				if !headerPrinted {
					err = logHeader(csvWriter, rec)
					headerPrinted = true
				}
				err = logRecord(csvWriter, rec)
				if err != nil {
					log.Fatalf("Unable to write output: %v", err)
				}
			}
		}
	}
	csvWriter.Flush()
	err = rawReader.Close()
	return
}
