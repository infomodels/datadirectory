package datadirectory

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// PopulateMetadataFromData fills the DataDirectory with metadata from the
// data files. Also, any information missing from the DataDirectory object is
// collected through command line prompts.
func (d *DataDirectory) PopulateMetadataFromData() error {

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

// populateRecord is a walk function that can be passed to filepath.Walk in
// order to fill the DataDirectory file metadata for each file in the
// directory.
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

	recordMap["line"] = strconv.Itoa(len(d.RecordMaps) + 1)

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
