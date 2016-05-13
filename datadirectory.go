package datadirectory

import (
	"errors"
	"fmt"
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
