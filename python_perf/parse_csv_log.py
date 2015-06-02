#!/opt/local/bin/python
import csv
import argparse

def main():

    parser = argparse.ArgumentParser(
        description=
        'Parse the given csv file',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        usage = __doc__)

    # not clean argparse style, but just to match with the Go version's cmdline option
    parser.add_argument('-l', metavar='log-file', dest='logFileName',
        action='store', default = None,
        help="Logfile to parse")

    args = parser.parse_args()

    f = open(args.logFileName)

    csvReader = csv.reader(f)

    rows = 0
    for record in csvReader:
        rows += 1
        # print '\t'.join(record[0:3]) # access some fields or print something

    print "total rows = %d" % rows

if __name__ == '__main__':
    main()
