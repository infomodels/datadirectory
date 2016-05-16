package datadirectory

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// ReadMetadataFromFile reads data from an existing metadata.csv file into
// the appropriate attributes.
func (d *DataDirectory) ReadMetadataFromFile() error {

	var (
		file *os.File
		err  error
	)

	// Create a strict csv.Reader
	if file, err = os.Open(d.FilePath); err != nil {
		return err
	}

	defer file.Close()

	if err = d.ReadMetadata(file); err != nil {
		return err
	}

	return nil

}

// ReadMetadata reads metadata.csv-style data from the passed reader
// into the appropriate attributes.
func (d *DataDirectory) ReadMetadata(r io.Reader) error {

	var (
		csvReader *csv.Reader
		line      int
		err       error
	)

	csvReader = csv.NewReader(r)
	csvReader.LazyQuotes = false
	csvReader.TrimLeadingSpace = false

	// Read in the header, standardizing to lowercase and ensuring no
	// unexpected values are present.
	if d.header, err = csvReader.Read(); err != nil {
		return err
	}

	for i, headerVal := range d.header {

		var found bool

		d.header[i] = strings.ToLower(headerVal)

		for _, cHeaderVal := range canonicalHeader {
			if d.header[i] == cHeaderVal {
				found = true
				break
			}
		}

		if !found {
			return fmt.Errorf("unexpected header value: %s", headerVal)
		}
	}

	line++

	// Ensure required header values are present.
	for cHeaderVal, req := range headerReq {

		var found bool

		if req {

			for _, headerVal := range d.header {
				if headerVal == cHeaderVal {
					found = true
					break
				}
			}

			if !found {
				return fmt.Errorf("missing required header value: %s", cHeaderVal)
			}
		}
	}

	// Read records into the DataDirectory record maps.
	for {

		var recordMap map[string]string

		// Get next record, exiting if there's no more.
		record, err := csvReader.Read()

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		line++

		// Create map of header values to record values.
		recordMap = make(map[string]string)
		d.RecordMaps = append(d.RecordMaps, recordMap)

		for i, val := range record {
			if d.header[i] == "organization" || d.header[i] == "filename" || d.header[i] == "etl" {
				recordMap[d.header[i]] = val
			} else {
				recordMap[d.header[i]] = strings.ToLower(val)
			}
		}

		recordMap["line"] = strconv.Itoa(line)
	}

	return nil
}
