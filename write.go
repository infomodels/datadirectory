package datadirectory

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// WriteMetadataToFile writes data from the DataDirectory object to the
// metadata.csv file. An existing metadata.csv will be overwritten.
func (d *DataDirectory) WriteMetadataToFile() error {

	var (
		file io.Writer
		err  error
	)

	if file, err = os.OpenFile(d.FilePath, os.O_CREATE|os.O_WRONLY, 0644); err != nil {
		return err
	}

	if err = d.WriteMetadata(file); err != nil {
		return err
	}

	return nil

}

// WriteMetadata writes metadata.csv-style data from the DataDirectory object
// to the passed writer.
func (d *DataDirectory) WriteMetadata(w io.Writer) error {

	var err error

	// Write metadata header.
	if _, err = w.Write([]byte(fmt.Sprintf("\"%s\"\n", strings.Join(d.header, `","`)))); err != nil {
		return err
	}

	for _, record := range d.RecordMaps {
		var row []string
		for _, val := range d.header {
			row = append(row, record[val])
		}
		if _, err = w.Write([]byte(fmt.Sprintf("\"%s\"\n", strings.Join(row, `","`)))); err != nil {
			return err
		}
	}

	return nil
}
