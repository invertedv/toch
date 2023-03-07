// toch moves data from files and the web into ClickHouse.
// Features of toch include:
//
//   - Multiple data formats are supported:
//
//   - tab delimited
//
//   - CSV
//
//   - Excel: XLS (linux only) and XLSX formats
//
//   - Data sets can have headers or not
//
//   - Field names can be user-supplied or changed from the data header
//
//   - Field types can be imputed or supplied
//
// Required command line arguments:
//
//	-s       source of data. This is either a file or web address.
//	-type    type of data.  The options are:
//	    -text   tab delimited
//	    -csv    comma separated
//	    -xls    Excel XLS
//	    -xlsx   Excel XLSX
//	-table   destination ClickHouse table.
//
// Optional command line arguments:
//
//	-host           IP of ClickHouse database. Default: 127.0.0.1
//	-user           ClickHouse user. Default: "default"
//	-password       ClickHouse password. Default: ""
//	-c [Y/N]        convert field names to camel case. Default N
//	-i [Y/N]        ignore read errors. Default: N
//	-skip <n>       rows to skip at beginning of file. Default: 0.
//	-q <char>       character for delimiting text. Default: "
//	-h 'f1,f2,...'  the field names are comma separated and the entire list is enclosed in single quotes. The default is to read these from the data.
//	-t 't1,t2,...'  the types are comma separated and the entire list is encludes in single quotes. The default is to infer these from the data. Supported types are:
//	    f   Float64
//	    i   Int64
//	    d   Date
//	    s   String
//	 -sheet          sheet name for Excel inputs. Default: first sheet in the workbook.
//	 -rows <S:E>     start row:end row range from which to pull data from Excel inputs. If E=0, all rows after S are taken. Default: 0:0
//	 -cols <S:E>     start column:end column range from which to pull data from Excel inputs. If E=0, all columns after S are taken. Default 0:0
//
// Notes:
//   - S and E are 0-based indices.
//   - if -h is supplied, the list must include all fields.
//   - if -t is supplied, the list must included all fields.
//   - The options -h and -t are independent: one can be supplied without the other.
//   - ctrl-R's in the data are ignored.
//   - The -skip parameter works with spreadsheets, too. It is applied within (any possible) range supplied by -rows.
//
// Values that are illegal for the field type are filled in as:
//   - Float64  the maximum value for Float64 (~E308)
//   - Int64    the maximum value for Int64 (9223372036854775807)
//   - Date     1970/1/1
//   - String   "!"
//
// # Examples
//
// The command
//
//	toch   -table laSeries -type text -s https://download.bls.gov/pub/time.series/la/la.series
//
// loads the la.series table from the Bureau of Labor Statistics into ClickHouse table laSeries.
//
// And this command
//
//	toch  -table test -type text -s https://download.bls.gov/pub/time.series/la/la.series -h 'a,b,c,d,e,f,g,h,i,j,k,l' -skip 1
//
// loads the same table as above, but overrides the field names in the table with 'a' through 'l'.
// The -skip 1 argument is used so that the header row will not be read as a part of the data.
//
// The data in this csv from the FHFA has no header row.
//
//	toch  -table msa -type csv -s https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_metro.csv -h 'name,msa,year, qtr, ind, delta' -t 's,s,i,i,f,s'
//
// The -h option supplies the headers.  The imputation wants to make the "msa" field an integer since
// all the values are digits.  The -t option is used to override that to make the field a string.
//
// The command below reads an Excel spreadsheet.  The first tab in the workbook is read starting in row 4
// (Excel calls this row 5) and column C.
//
//	toch -table XlTest -type XLSX -s test.xlsx -rows 4:0 -cols 2:0
//
// This command reads the same data, but specifies the field types to both be strings and the field names to be 'x' and 'y':
//
//	toch -table XlTest -type XLSX -s test.xlsx -rows 5:0 -cols 2:0 -h 'x,y' -t 's,s'
//
// Since the header row in the spreadsheet is not used, the starting row is one larger.  We could have also
// kept "-rows 4:0" and added "-skip 1".
package main

import (
	"flag"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/invertedv/chutils"
	"github.com/invertedv/chutils/file"
	"github.com/invertedv/chutils/sql"
	"github.com/invertedv/chutils/str"
	"github.com/xuri/excelize/v2"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// types of file formats toch handles
var types = []string{"text", "csv", "xlsx", "xls"}

// reserved field names -- ClickHouse will not allow these
var reserved = []string{"index"}

// allowed values for -t field types the user can specify
var ftypes = []string{"s", "i", "d", "f"}

// allowed values for -c camel case
var ctypes = []string{"y", "n"}

func main() {
	hostPtr := flag.String("host", "127.0.0.1", "string")
	userPtr := flag.String("user", "default", "string")
	passwordPtr := flag.String("password", "", "string")

	tablePtr := flag.String("table", "", "string")

	sTypePtr := flag.String("type", "", "string")
	sourcePtr := flag.String("s", "", "string")

	camelPtr := flag.String("c", "N", "string")
	headerPtr := flag.String("h", "", "string")
	fieldPtr := flag.String("t", "", "string")
	quotePtr := flag.String("q", `"`, "string")
	skipPtr := flag.Int("skip", 0, "int")
	ignorePtr := flag.String("i", "N", "string")

	xlRowsPtr := flag.String("rows", "0:0", "string")
	xlColsPtr := flag.String("cols", "0:0", "string")
	xlSheetPtr := flag.String("sheet", "", "string")

	flag.Parse()
	// work through the flags
	headers, fieldTypes, camel, ignore, quote, xlArea, err :=
		flags(sTypePtr, camelPtr, headerPtr, fieldPtr, quotePtr, xlRowsPtr, xlColsPtr, skipPtr, ignorePtr)
	if err != nil {
		help() // print help string
		log.Fatalln(err)
	}

	// connect to ClickHouse
	con, err := chutils.NewConnect(*hostPtr, *userPtr, *passwordPtr, clickhouse.Settings{"max_memory_usage": 40000000000})
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if e := con.Close(); e != nil {
			fmt.Println(e)
		}
	}()

	s := time.Now()
	rdr, err := buildReader(*sourcePtr, *sTypePtr, *skipPtr, quote, camel, headers, fieldTypes, xlArea, *xlSheetPtr, *tablePtr, con)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if e := rdr.Close(); e != nil {
			fmt.Println(e)
		}
	}()

	// create the writer.
	wtr := sql.NewWriter(*tablePtr, con)
	defer func() {
		if e := wtr.Close(); e != nil {
			fmt.Println(e)
		}
	}()

	// now do the transfer.  If the csv is large (>1GB), the connection will be reset if after=0
	if e := chutils.Export(rdr, wtr, 1000, ignore); e != nil {
		log.Fatalln(e)
	}
	ts := int(time.Since(s).Seconds())
	mins := ts / 60
	secs := ts % 60
	fmt.Printf("elapsed time: %d minutes %d seconds", mins, secs)
}

// buildReader creates a reader for chutils.Export. It handles options regarding field names and types
func buildReader(source string, sType string, skip int, quote rune, camel bool, headers []string, fieldTypes []string, xl []int, xlSheet string, table string, con *chutils.Connect) (*file.Reader, error) {
	// if reading a header row, need to skip it before reading data.
	if len(headers) == 0 {
		skip += 1
	}
	// Get the reader
	rdr, err := NewReader(source, sType, quote, skip, xl, xlSheet)
	if err != nil {
		return nil, err
	}
	// handle headers: read them from file
	if len(headers) == 0 {
		if err := rdr.Init("", chutils.MergeTree); err != nil {
			return nil, err
		}
		for ind, fd := range rdr.TableSpec().FieldDefs {
			if camel {
				fd.Name = toCamel(fd.Name)
			}
			if isIn(&fd.Name, reserved, false) {
				fd.Name += "1"
			}
			// we may have renamed the key...
			if ind == 0 {
				rdr.TableSpec().Key = fd.Name
			}
		}
	} else {
		// user-supplied field names
		fds := make(map[int]*chutils.FieldDef)
		// choosing ChUnknown tells Impute to figure it out.
		for ind, name := range headers {
			fds[ind] = &chutils.FieldDef{Name: name, ChSpec: chutils.ChField{Base: chutils.ChUnknown}, Legal: &chutils.LegalValues{}}
		}
		tableSpec := chutils.NewTableDef(headers[0], chutils.MergeTree, fds)
		rdr.SetTableSpec(tableSpec)
	}
	// Find field types from data
	if len(fieldTypes) == 0 {
		if err := rdr.TableSpec().Impute(rdr, 0, 0.95); err != nil {
			return nil, err
		}
	} else {
		// handle user-supplied data types
		if len(fieldTypes) != len(rdr.TableSpec().FieldDefs) {
			return nil, fmt.Errorf("supplied field types have length %d, data has %d columns", len(fieldTypes), len(rdr.TableSpec().FieldDefs))
		}
		for ind, fd := range rdr.TableSpec().FieldDefs {
			switch fieldTypes[ind] {
			case "d":
				fd.ChSpec.Base, fd.Missing = chutils.ChDate, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
			case "i":
				fd.ChSpec.Base, fd.ChSpec.Length, fd.Missing = chutils.ChInt, 64, math.MaxInt64
			case "f":
				fd.ChSpec.Base, fd.ChSpec.Length, fd.Missing = chutils.ChFloat, 64, math.MaxFloat64
			default:
				fd.ChSpec.Base, fd.Missing = chutils.ChString, "!"
			}
		}
	}
	// create the table
	if err := rdr.TableSpec().Create(con, table); err != nil {
		return nil, err
	}
	return rdr, nil
}

// NewReader creates the appropriate kind of reader
func NewReader(source string, sType string, quote rune, skip int, xl []int, xlSheet string) (*file.Reader, error) {
	if strings.Contains(strings.ToLower(source), "http") {
		// newHttp pulls the data as well.
		return newHttp(source, sType, quote, skip, xl, xlSheet)
	}
	return newFile(source, sType, quote, skip, xl, xlSheet)
}

// newHttp creates a reader for data coming via http.
// The package excelize cannot read .xls files.  So these are downloaded, converted to .xlsx and a file reader is created.
func newHttp(source string, sType string, quote rune, skip int, xl []int, xlSheet string) (*file.Reader, error) {

	// get the data.  We will put into a string reader.
	resp, err := http.Get(source)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	switch sType {
	case "text", "csv":
		return str.NewReader(string(body), sep(sType), '\n', quote, 0, skip, 0), nil
	case "xlsx":
		// excelize will parse the data which is then put into a string reader by NewXlReader
		r := strings.NewReader(string(body))
		xlr, err := excelize.OpenReader(r)
		if err != nil {
			return nil, err
		}
		return str.NewXlReader(xlr, xlSheet, xl[0], xl[1], xl[2], xl[3], quote, skip, 0), nil
	case "xls":
		// this works only on linux.  Save this as a file and then use the newFile protocol.  That
		// will use libreoffice to convert it to an XLSX so that excelize can read it.
		fileName := "/tmp/tmp.xls"
		f, e := os.Create(fileName)
		if e != nil {
			return nil, e
		}
		if _, e := f.Write(body); e != nil {
			return nil, e
		}
		if e := f.Close(); e != nil {
			return nil, e
		}
		return newFile(fileName, "xls", quote, skip, xl, xlSheet)
	default:
		return nil, fmt.Errorf("illegal -type")
	}
}

// getDir returns the directory portion of a file path
func getDir(path string) string {
	if ind := strings.LastIndex(path, "/"); ind > 0 {
		return path[0:ind]
	}
	return path
}

// newFile creates a reader for data coming from a file
func newFile(source string, sType string, quote rune, skip int, xl []int, xlSheet string) (*file.Reader, error) {
	f, err := os.Open(source)
	if err != nil {
		return nil, err
	}
	switch sType {
	case "text", "csv":
		return file.NewReader(source, sep(sType), '\n', quote, 0, skip, 0, f, 0), nil
	case "xlsx", "xls":
		// if sType = "xls" then convert to xlsx in the same directory
		if sType == "xls" {
			args := []string{"--headless", "--convert-to", "xlsx", "--outdir", getDir(source), source}
			c := exec.Command("libreoffice", args...)
			if e := c.Run(); e != nil {
				return nil, e
			}
			source = strings.Replace(source, ".xls", ".xlsx", 1)
		}

		xlr, err := excelize.OpenFile(source)
		if err != nil {
			return nil, err
		}

		return str.NewXlReader(xlr, xlSheet, xl[0], xl[1], xl[2], xl[3], quote, skip, 0), nil
	default:
		return nil, fmt.Errorf("illegal -type")
	}

}

// toCamel converts from snake case to camel case.
func toCamel(snake string) string {
	// replace spaces in field name with underscores
	snake = strings.ReplaceAll(snake, " ", "_")
	// lower-case the first character
	snake = strings.Replace(snake, snake[0:0], strings.ToLower(snake[0:0]), 1)
	const chars = "._"
	snake = strings.ToLower(snake)

	for ind := strings.IndexAny(snake, chars); ind >= 0; {
		snake = strings.Replace(snake, snake[ind:ind+2], strings.ToUpper(snake[ind+1:ind+2]), 1)
		ind = strings.IndexAny(snake, chars)
	}
	return snake
}

// isIn checks whether needle is in the stack.
// if toLower is true, needle is lower-cased
func isIn(needle *string, stack []string, toLower bool) bool {
	check := strings.ToLower(*needle)
	if toLower {
		*needle = check
	}
	for _, s := range stack {
		if s == check {
			return true
		}
	}
	return false
}

// sep returns the field separate for the source type
func sep(sType string) rune {
	switch sType {
	case "text", "xlsx":
		return '\t'
	default:
		return ','
	}
}

// flags checks that the flags are valid. It returns digested values.
// Outputs:
//   - headers      array of field names
//   - fieldTypes   array of field types
//   - camel        whether to convert to camel case
//   - quote        quote value as a rune
//   - xlArea       range on spreadsheet to pull : [row Min, row Max, col Min, col Max]
//   - err          error
func flags(sTypePtr, camelPtr, headerPtr, fieldPtr, quotePtr, xlRowsPtr, xlColsPtr *string,
	skipPtr *int, ignorePtr *string) (headers []string, fieldTypes []string, camel bool, ignore bool, quote rune, xlArea []int, err error) {

	headers = make([]string, 0)
	fieldTypes = make([]string, 0)
	camel = false
	quote = 0
	xlArea = make([]int, 0)
	err = nil

	if !isIn(sTypePtr, types, true) {
		err = fmt.Errorf("unrecognized source type: %s", *sTypePtr)
		return
	}

	if !isIn(camelPtr, ctypes, true) {
		err = fmt.Errorf("-c option is Y or N")
		return
	}
	camel = *camelPtr == "y"
	if !isIn(ignorePtr, ctypes, true) {
		err = fmt.Errorf("-c option is Y or N")
		return
	}
	ignore = *ignorePtr == "y"

	if len(*quotePtr) > 1 {
		err = fmt.Errorf("-q option is a single character")
	}
	quote = rune((*quotePtr)[0])

	if *headerPtr != "" {
		headers = strings.Split(strings.ReplaceAll(strings.ReplaceAll(*headerPtr, " ", ""), "'", ""), ",")
	}

	if *fieldPtr != "" {
		fieldTypes = strings.Split(strings.ReplaceAll(strings.ToLower(strings.ReplaceAll(*fieldPtr, " ", "")), "'", ""), ",")
		for _, f := range fieldTypes {
			if !isIn(&f, ftypes, false) {
				err = fmt.Errorf("not a valid field type: %s", f)
				return
			}
		}
	}
	if len(headers) != len(fieldTypes) && len(headers) > 0 && len(fieldTypes) > 0 {
		err = fmt.Errorf("-h headers and -t field types must have same length")
		return
	}

	if *skipPtr < 0 {
		err = fmt.Errorf("-skip value must be non-negative")
		return
	}

	if !strings.Contains(*xlRowsPtr, ":") || !strings.Contains(*xlColsPtr, ":") {
		err = fmt.Errorf("invalid XL rows/cols specs")
		return
	}
	r := strings.Split(*xlRowsPtr, ":")
	c := strings.Split(*xlColsPtr, ":")
	xlArea = make([]int, 4)
	for ind := 0; ind < 2; ind++ {
		var rx, cx int64
		rx, err = strconv.ParseInt(r[ind], 10, 32)
		if err != nil {
			return
		}
		xlArea[ind] = int(rx)
		cx, err = strconv.ParseInt(c[ind], 10, 32)
		if err != nil {
			return
		}
		xlArea[2+ind] = int(cx)
	}
	return
}

// help prints out some help when the command line arguments don't parse correctly
func help() {
	help := `
Required command line arguments:
   -s       source of data. This is either a file or web address.
   -type    type of data.  Supported types are:
         -text   tab delimited
         -csv    comma separated
         -xls    Excel XLS
         -xlsx   Excel XLSX
   -table   destination ClickHouse table.

Optional command line arguments:
   -host           IP of ClickHouse database. Default: 127.0.0.1
   -user           ClickHouse user. Default: "default"
   -password       ClickHouse password. Default: ""
   -c [Y/N]        convert field names to camel case. Default N
   -q <char>       character for delimiting text. Default: "
   -h 'f1,f2,...'  the field names are comma separated and the entire list is enclosed in single quotes. The default is to read these from the file.
   -t 't1,t2,...'  the types are comma separated and the entire list is encludes in single quotes. The default is to infer these from the file. Supported types are:
       f   Float64
       i   Int64
       d   Date
       s   String
    -sheet          sheet name for Excel inputs.  If this is omitted, the first sheet is read.
    -rows <S:E>     start row:end row range from which to pull data from Excel inputs. If E=0, all rows after S are taken. Default: 0:0
    -cols <S:E>     start column:end column range from which to pull data from Excel inputs. If E=0, all columns after S are taken. Default 0:0
Notes:
  - S and E are 0-based indices.
  - if -h is supplied, the list must include all fields.
  - if -t is supplied, the list must included all fields.
  - The options -h and -t are independent: one can be supplied without the other.
  - ctrl-R's in the data are ignored.
  - The -skip parameter works with spreadsheets, too. It is applied within (any possible) range supplied by -rows.

Values that are illegal for the field type are filled in as:
   - Float64  the maximum value for Float64 (~E308)
   - Int64    the maximum value for Int64 (9223372036854775807)
   - Date     1970/1/1
   - String   "!"

`
	fmt.Println(help)
}
