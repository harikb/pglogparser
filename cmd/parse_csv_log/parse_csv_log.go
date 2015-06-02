package main

import (
	//"bufio"
	//"bytes"
	//"fmt"
	"github.com/davecheney/profile"
	"github.com/harikb/yacr"
	flag "github.com/ogier/pflag"
	"io"
	"log"
	"os"
)

type cmdArgs struct {
	cpuProfile  bool
	logfilename string
}

// these are globals
var args cmdArgs

func main() {

	flag.BoolVarP(&args.cpuProfile, "profile", "p", false, "CPU profile this run")
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

	rawReader, err := os.Open(args.logfilename)
	if err != nil {
		log.Fatalf("Unable to open file %v: %v", args.logfilename, err)
	}

	csvReader := yacr.DefaultReader(rawReader)

	var c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23 string
	rNum := 0
	for {
		rNum++
		record, err := csvReader.ScanRecord(&c1, &c2, &c3, &c4, &c5, &c6, &c7, &c8, &c9, &c10, &c11, &c12, &c13, &c14, &c15, &c16, &c17, &c18, &c19, &c20, &c21, &c22, &c23)

		if err == io.EOF || record == 0 {
			break
		}
		if err != nil {
			log.Fatalf("Error in reading file at record %d, line %d: %v", rNum, csvReader.LineNumber(), err)
		}
	}
	log.Printf("total rows = %d", rNum)

}
