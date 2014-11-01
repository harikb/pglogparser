package main

import (
	//"bufio"
	//"bytes"
	//"fmt"
	"github.com/davecheney/profile"
	"github.com/harikb/yacr"
	_ "github.com/lib/pq"
	flag "github.com/ogier/pflag"
	"io"
	"log"
	"os"
)

type cmdArgs struct {
	cpuProfile      bool
	targetDirectory string
	patternsFile    string
	queryTransform  string // choices none, unfold, canonicalize
	outputFormat    string // choices csv, tsv
	sample          int
	verbose         bool
	logfilename     string
}

// these are globals
var args cmdArgs

func main() {

	//var err error

	flag.BoolVarP(&args.cpuProfile, "profile", "p", false, "CPU profile this run")
	flag.StringVarP(&args.targetDirectory, "target-directory", "t", "",
		`Directory where the parsed files should be saved. Pass "-" for output to stdout`)
	flag.StringVarP(&args.patternsFile, "patterns-file", "S", "",
		"File with patterns to substitute")
	flag.StringVarP(&args.queryTransform, "query-transform", "q", "",
		`How to transform original query.
 Choices are 'canonicalize', 'none', 'unfold'`)
	flag.StringVarP(&args.outputFormat, "output-format", "F", "",
		"Output format. Choices are csv, tsv")
	flag.BoolVarP(&args.verbose, "verbose", "v", false, "Verbose output")
	flag.IntVarP(&args.sample, "sample", "s", 0, "Sample only this many events")
	flag.StringVarP(&args.logfilename, "log-file", "l", "",
		"File to parse")
	flag.Parse()

	if args.cpuProfile {
		cfg := profile.Config{
			CPUProfile:  true,
			ProfilePath: ".", // store profiles in current directory
		}
		defer profile.Start(&cfg).Stop()
	}

	// runtime.GOMAXPROCS(10)
	// log.Printf("GOMAXPROCS=%v", runtime.GOMAXPROCS(0))

	rawReader, err := os.Open(args.logfilename)
	if err != nil {
		log.Fatalf("Unable to open file %v: %v", args.logfilename, err)
	}

	//bufReader := bufio.NewReaderSize(rawReader, 100000000)
	csvReader := yacr.DefaultReader(rawReader)

	log.Printf("Settings are %v", csvReader.GetSettings())

	//csvReader := csv.NewReader(bufReader)
	//csvReader := csv.NewReader(rawReader)

	var c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23 string
	rNum := 0
	for {
		rNum++
		record, err := csvReader.ScanRecord(&c1, &c2, &c3, &c4, &c5, &c6, &c7, &c8, &c9, &c10, &c11, &c12, &c13, &c14, &c15, &c16, &c17, &c18, &c19, &c20, &c21, &c22, &c23)
		//fmt.Printf("%v\t%v\t%v\t%v\n", c1, c2, c3, c4)
		if rNum%1000 == 0 || record == 0 {
			//log.Printf("Done with %d rows, columns=%d", rNum, record)
		}

		if err == io.EOF || record == 0 {
			break
		}
		if err != nil {
			log.Fatalf("Error in reading file at record %d, line %d: %v", rNum, csvReader.LineNumber(), err)
		}
	}
	log.Printf("total rows = %d", rNum)

}
