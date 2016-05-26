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
)

// Validate checks the validity of the DataDirectory object. Specifically, the
// file metadata is checked against any existing information on
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
		if d.Site != "" && recordMap["organization"] != d.Site {
			return fmt.Errorf("line '%s' organization '%s' does not match expected organization '%s'", recordMap["line"], recordMap["site"], d.Site)
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
