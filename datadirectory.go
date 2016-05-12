package datadirectory

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/chop-dbhi/data-models-service/client"
)

const dataModelsService = "https://data-models-service.research.chop.edu"

// Default *ordered* header.
var canonicalHeader = []string{
	"organization",
	"filename",
	"checksum",
	"cdm",
	"cdm-version",
	"table",
	"etl",
	"data-version",
}

// Permitted metadata header values and whether or not they are required.
var headerReq = map[string]bool{
	"organization": true,
	"filename":     true,
	"checksum":     true,
	"cdm":          true,
	"cdm-version":  false,
	"table":        true,
	"etl":          true,
	"data-version": false,
}

// Config holds all potential configuration arguments for a DataDirectory
// object. Only the DataDirPath is required.
type Config struct {
	DataDirPath  string
	DataVersion  string
	Etl          string
	Model        string
	ModelVersion string
	Service      string
	Site         string
}

// DataDirectory represents a particular data directory and a set of metadata
// for it and the data files within it.
type DataDirectory struct {
	RecordMaps   []map[string]string
	Site         string
	Model        string
	ModelVersion string
	DataVersion  string
	Etl          string
	DirPath      string
	FilePath     string
	header       []string
	service      string
	/* serviceModels is a simplified version of data models service information
	   and should look like:
	   {
	       "pedsnet": {
	   	    "sorted": ["1.0.0", "2.0.0", "2.1.0"],
	   		"1.0.0": ["table1", "table2", ...],
	   		...
	   	},
	   	...
	   }
	*/
	serviceModels map[string]map[string]sort.StringSlice
}

// New creates a new DataDirectory object from a Config object. Only the
// Config.DataDirPath attribute is required.
func New(cfg *Config) (*DataDirectory, error) {

	var (
		c       *client.Client
		cModels *client.Models
		mFound  bool
		vFound  bool
		d       *DataDirectory
		err     error
	)

	// Return error if path not given.
	if cfg.DataDirPath == "" {
		return nil, errors.New("the DataDirectory object requires Config.DataDirPath")
	}

	// Initialize with any passed metadata information, standardizing to
	// lowercase where appropriate.
	d = &DataDirectory{
		RecordMaps:    make([]map[string]string, 0),
		Site:          cfg.Site,
		Model:         strings.ToLower(cfg.Model),
		ModelVersion:  strings.ToLower(cfg.ModelVersion),
		DataVersion:   strings.ToLower(cfg.DataVersion),
		Etl:           cfg.Etl,
		DirPath:       cfg.DataDirPath,
		FilePath:      filepath.Join(cfg.DataDirPath, "metadata.csv"),
		header:        canonicalHeader,
		service:       cfg.Service,
		serviceModels: make(map[string]map[string]sort.StringSlice),
	}

	// Initialize data models service client.
	if d.service == "" {
		d.service = dataModelsService
	}

	if c, err = client.New(d.service); err != nil {
		return nil, err
	}

	if err = c.Ping(); err != nil {
		return nil, err
	}

	// Construct serviceModels map.
	if cModels, err = c.Models(); err != nil {
		return nil, err
	}

	for _, cModel := range cModels.List() {

		// Initialize map for each model.
		if d.serviceModels[cModel.Name] == nil {
			d.serviceModels[cModel.Name] = make(map[string]sort.StringSlice)
		}

		d.serviceModels[cModel.Name]["sorted"] = append(d.serviceModels[cModel.Name]["sorted"], cModel.Version)
		d.serviceModels[cModel.Name][cModel.Version] = cModel.Tables.Names()
	}

	// Check that model and model version, if passed, exist in models retrieved
	// from service.
	if d.Model != "" {

		for model, versionInfo := range d.serviceModels {

			versions := versionInfo["sorted"]

			// Sort while we're searching.
			versions.Sort()

			if d.Model == model {

				mFound = true

				// Default to the latest model version.
				if d.ModelVersion == "" {
					d.ModelVersion = versions[len(versions)-1]
					vFound = true
					break
				}

				for _, version := range versions {
					if d.ModelVersion == version {
						vFound = true
						break
					}
				}

				break
			}
		}

		if !mFound || !vFound {
			return nil, fmt.Errorf("model '%s' version '%s' not found in data models service", d.Model, d.ModelVersion)
		}
	}

	return d, nil
}

// ReadFromFile reads data from the metadata.csv file into the RecordMaps
// attribute on the DataDirectory object.
func (d *DataDirectory) ReadFromFile() error {

	var (
		file      *os.File
		csvReader *csv.Reader
		line      int
		err       error
	)

	// Create a strict csv.Reader
	if file, err = os.Open(d.FilePath); err != nil {
		return err
	}

	defer file.Close()

	csvReader = csv.NewReader(file)
	csvReader.LazyQuotes = false
	csvReader.TrimLeadingSpace = false

	// Read in the header, standardizing to lowercase and ensuring no
	// unexpected values are present.
	if d.header, err = csvReader.Read(); err != nil {
		return err
	}

	for i, headerVal := range d.header {

		d.header[i] = strings.ToLower(headerVal)
		found := false

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

		if req {

			found := false

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
		recordMap := make(map[string]string)
		d.RecordMaps = append(d.RecordMaps, recordMap)

		for i, val := range record {
			if d.header[i] == "organization" || d.header[i] == "filename" || d.header[i] == "etl" {
				recordMap[d.header[i]] = val
			} else {
				recordMap[d.header[i]] = strings.ToLower(val)
			}
		}

		recordMap["line"] = string(line)
	}

	return nil
}

// Validate checks the validity of the RecordMaps on the DataDirectory object.
// Specifically, they are checked against any existing information on
// the DataDirectory object and then against information from the data models
// service. If all of those checks pass, then each checksum is checked for
// accuracy.
func (d *DataDirectory) Validate() error {

	var err error

	// Validate records values, except for checksums.
	for _, recordMap := range d.RecordMaps {

		var (
			mFound bool
			vFound bool
			tFound bool
		)

		// Check that required values are present.
		for cHeaderVal, req := range headerReq {
			if req && recordMap[cHeaderVal] == "" {
				return fmt.Errorf("line '%s' missing required value '%s'", recordMap["line"], cHeaderVal)
			}
		}

		// Check that site matches DataDirectory site, if present.
		if d.Site != "" && recordMap["site"] != d.Site {
			return fmt.Errorf("line '%s' site '%s' does not match expected site '%s'", recordMap["line"], recordMap["site"], d.Site)
		}

		// Check that model and version exist in the info retrieved from data
		// models service.
		for model, versionInfo := range d.serviceModels {

			versions := versionInfo["sorted"]

			if recordMap["cdm"] == model {

				mFound = true

				// Default to latest model version.
				if recordMap["cdm-version"] == "" {
					recordMap["cdm-version"] = versions[len(versions)-1]
					vFound = true
					break
				}

				for _, version := range versions {
					if recordMap["cdm-version"] == version {
						vFound = true
						break
					}
				}

				break
			}
		}

		if !mFound || !vFound {
			return fmt.Errorf("line '%s' cdm '%s' version '%s' not found in data models service", recordMap["line"], recordMap["cdm"], recordMap["cdm-version"])
		}

		// Check that model matches DataDirectory model, if present.
		if d.Model != "" && recordMap["cdm"] != d.Model {
			return fmt.Errorf("line '%s' cdm '%s' does not match expected model '%s'", recordMap["line"], recordMap["cdm"], d.Model)
		}

		// Check that model version matches DataDirectory model version, if present.
		if d.ModelVersion != "" && recordMap["cdm-version"] != d.ModelVersion {
			return fmt.Errorf("line '%s' cdm-version '%s' does not match expected model version '%s'", recordMap["line"], recordMap["cdm-version"], d.ModelVersion)
		}

		// Check that the table is present in the info retrieved from the data
		// models service.
		for _, table := range d.serviceModels[recordMap["cdm"]][recordMap["cdm-version"]] {
			if recordMap["table"] == table {
				tFound = true
				break
			}
		}

		if !tFound {
			return fmt.Errorf("line '%s' table '%s' not found in data models service", recordMap["line"], recordMap["table"])
		}

		// Check that data version matches DataDirectory data version, if both are
		// present.
		if d.DataVersion != "" && recordMap["data-version"] != "" && recordMap["data-version"] != d.DataVersion {
			return fmt.Errorf("line '%s' data-version '%s' does not match expected data version '%s'", recordMap["line"], recordMap["data-version"], d.DataVersion)
		}
	}

	// Validate record checksums.
	for _, recordMap := range d.RecordMaps {

		var (
			dataFile  *os.File
			sum       hash.Hash
			sumString string
		)

		// Check that file exists.
		if dataFile, err = os.Open(filepath.Join(d.DirPath, recordMap["filename"])); err != nil {
			return err
		}

		defer dataFile.Close()

		// Verify checksum.
		sum = sha256.New()

		log.Printf("packer: validating '%s' checksum", filepath.Base(recordMap["filename"]))

		if _, err = io.Copy(sum, dataFile); err != nil {
			return err
		}

		sumString = hex.EncodeToString(sum.Sum(nil))

		if recordMap["checksum"] != sumString {
			return fmt.Errorf("line '%s' file '%s' checksum does not match", recordMap["line"], recordMap["filename"])
		}
	}

	return nil
}

// Populate fills the RecordMaps with metadata from the files in the data
// directory. Any information missing from the DataDirectory object is collected
// through command line prompts.
func (d *DataDirectory) Populate() error {

	var (
		modelChoices   []string
		versionChoices []string
		err            error
	)

	// Collect site name (using empty choice list) if not on DataDirectory.
	if d.Site == "" {
		var sites []string
		if d.Site, err = collectInput("site name", sites); err != nil {
			return err
		}
	}

	// Create model and version choice lists from the info retrieved from data
	// models service.
	for model, versionInfo := range d.serviceModels {
		modelChoices = append(modelChoices, model)
		for _, version := range versionInfo["sorted"] {
			versionChoices = append(versionChoices, version)
		}
	}

	// Collect model if not on DataDirectory, using model choice list.
	if d.Model == "" {
		if d.Model, err = collectInput("common data model name", modelChoices); err != nil {
			return err
		}
		d.Model = strings.ToLower(d.Model)
	}

	// Collect model version if not on DataDirectory, using version choice list.
	if d.ModelVersion == "" {
		if d.ModelVersion, err = collectInput("model version", versionChoices); err != nil {
			return err
		}
		d.ModelVersion = strings.ToLower(d.ModelVersion)
	}

	// Collect etl URL (using empty choice list) if not passed.
	if d.Etl == "" {
		var etls []string
		if d.Etl, err = collectInput("etl code URL", etls); err != nil {
			return err
		}
	}

	// TODO: Implement data version tagging at sites.
	/*// Collect data version (using empty choice list) if not passed.
	if d.DataVersion == "" {
		var dataVersions []string
		if d.dataVersion, err = collectInput("data version", dataVersions); err != nil {
			return err
		}
	}*/

	// Write metadata rows.
	if err = filepath.Walk(d.DirPath, d.populateRecord); err != nil {
		return err
	}

	return nil

}

// WriteToFile writes data from the DataDirectory object to the metadata.csv file.
func (d *DataDirectory) WriteToFile() error {

	var (
		file io.Writer
		err  error
	)

	if file, err = os.OpenFile(d.FilePath, os.O_CREATE|os.O_WRONLY, 0644); err != nil {
		return err
	}

	// Write metadata header.
	if _, err = file.Write([]byte(fmt.Sprintf("\"%s\"\n", strings.Join(d.header, `","`)))); err != nil {
		return err
	}

	for _, record := range d.RecordMaps {
		var row []string
		for _, val := range d.header {
			row = append(row, record[val])
		}
		if _, err = file.Write([]byte(fmt.Sprintf("\"%s\"\n", strings.Join(row, `","`)))); err != nil {
			return err
		}
	}

	return nil
}

// makeRowWriter uses a populated DataDirectory object to create a walk function
// that can be passed to filepath.Walk in order to write csv-formatted metadata
// rows to the passed writer.
func (d *DataDirectory) populateRecord(path string, fi os.FileInfo, inErr error) error {

	var (
		relPath   string
		table     string
		tFound    bool
		dataFile  *os.File
		sum       hash.Hash
		sumString string
		recordMap map[string]string
		err       error
	)

	// Return any error passed in.
	if err = inErr; err != nil {
		return err
	}

	// Get file path relative to the base data dir.
	if relPath, err = filepath.Rel(d.DirPath, path); err != nil {
		return err
	}

	// Skip directories, non-csv files, and the metadata file itself.
	if fi.IsDir() || filepath.Ext(path) != ".csv" || relPath == "metadata.csv" {
		return nil
	}

	// If file name is present in the info retrieved from data models
	// service, use it. Otherwise, collect table name from STDIN.
	table = strings.TrimSuffix(filepath.Base(path), ".csv")

	for _, serviceTable := range d.serviceModels[d.Model][d.ModelVersion] {
		if table == serviceTable {
			tFound = true
			break
		}
	}

	if !tFound {
		if table, err = collectInput(fmt.Sprintf("table name for '%s'", path), d.serviceModels[d.Model][d.ModelVersion]); err != nil {
			return err
		}
		table = strings.ToLower(table)
	}

	// Calculate checksum.
	if dataFile, err = os.Open(path); err != nil {
		return err
	}

	defer dataFile.Close()

	sum = sha256.New()

	log.Printf("metadata: calculating '%s' checksum", filepath.Base(path))
	if _, err = io.Copy(sum, dataFile); err != nil {
		return err
	}

	sumString = hex.EncodeToString(sum.Sum(nil))

	// Create map of header values to record values.
	recordMap = make(map[string]string)
	d.RecordMaps = append(d.RecordMaps, recordMap)

	for _, val := range d.header {
		switch val {
		case "organization":
			recordMap[val] = d.Site
		case "filename":
			recordMap[val] = relPath
		case "checksum":
			recordMap[val] = sumString
		case "cdm":
			recordMap[val] = d.Model
		case "cdm-version":
			recordMap[val] = d.ModelVersion
		case "table":
			recordMap[val] = table
		case "etl":
			recordMap[val] = d.Etl
		case "data-version":
			recordMap[val] = d.DataVersion
		}
	}

	recordMap["line"] = string(len(d.RecordMaps) + 1)

	return nil
}

// collectInput collects command line input using a provided prompt string. If
// a choices list is passed, the user will be prompted repeatedely until they
// provide one of the choices.
func collectInput(prompt string, choices []string) (input string, err error) {

	for {

		fmt.Printf("Please provide %s: ", prompt)
		fmt.Scanln(&input)

		if len(choices) > 0 {

			found := false

			for _, choice := range choices {
				if strings.ToLower(input) == choice {
					found = true
				}
			}

			if !found {
				fmt.Printf("Invalid input, please choose from '%s'.\n", strings.Join(choices, ", "))
				continue
			}
		}

		break
	}

	return input, nil
}
