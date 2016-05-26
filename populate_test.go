package datadirectory_test

import (
	"testing"

	"github.com/infomodels/datadirectory"
)

func TestPopulateNumRecords(t *testing.T) {

	var (
		cfg *datadirectory.Config
		d   *datadirectory.DataDirectory
		err error
	)

	cfg = &datadirectory.Config{
		DataDirPath:  "test_data",
		Model:        "pedsnet",
		ModelVersion: "2.1.0",
		Site:         "org",
		DataVersion:  "3",
		Etl:          "https://persistentcodestorage.com/ETLScript3.sql",
	}

	d, _ = datadirectory.New(cfg)

	if err = d.PopulateMetadataFromData(); err != nil {
		t.Errorf("PopulateMetadataFromData(): error in basic function: %s", err)
	}

	if len(d.RecordMaps) != 3 {
		t.Errorf("PopulateMetadataFromData(): expected number of RecordMaps (3) does not match actual length (%d)", len(d.RecordMaps))
	}

}

func TestPopulateLenRecord(t *testing.T) {

	var (
		cfg *datadirectory.Config
		d   *datadirectory.DataDirectory
		err error
	)

	cfg = &datadirectory.Config{
		DataDirPath:  "test_data",
		Model:        "pedsnet",
		ModelVersion: "2.1.0",
		Site:         "org",
		DataVersion:  "3",
		Etl:          "https://persistentcodestorage.com/ETLScript3.sql",
	}

	d, _ = datadirectory.New(cfg)

	if err = d.PopulateMetadataFromData(); err != nil {
		t.Errorf("PopulateMetadataFromData(): error in basic function: %s", err)
	}

	if len(d.RecordMaps[0]) != 9 {
		t.Errorf("PopulateMetadataFromData(): expected length of RecordMap (9) does not match actual length (%d)", len(d.RecordMaps[0]))
	}

}

func TestPopulateRecordContent(t *testing.T) {

	var (
		cfg  *datadirectory.Config
		d    *datadirectory.DataDirectory
		sums map[string]string
		err  error
	)

	sums = make(map[string]string)
	sums["location"] = "eee663c6095229e6ed62aeb3e41cc49a714b6c74eaa363454aae7e4d7cc208bd"
	sums["care_site"] = "653e55c69802e7a5aa3838b30f6f14b49cd624c1cb7ab52831038e7ac95cc810"
	sums["provider"] = "e784eeeea4b8264cf838c209034d4b8868d036f46d72789f04ac64f30853a636"

	cfg = &datadirectory.Config{
		DataDirPath:  "test_data",
		Model:        "pedsnet",
		ModelVersion: "2.1.0",
		Site:         "org",
		DataVersion:  "3",
		Etl:          "https://persistentcodestorage.com/ETLScript3.sql",
	}

	d, _ = datadirectory.New(cfg)

	if err = d.PopulateMetadataFromData(); err != nil {
		t.Errorf("PopulateMetadataFromData(): error in basic function: %s", err)
	}

	for _, record := range d.RecordMaps {
		if record["checksum"] != sums[record["table"]] {
			t.Errorf("PopulateMetadataFromData(): expected checksum of RecordMap for '%s' (%s) does not match actual checksum (%s)", record["filename"], sums[record["table"]], record["checksum"])
		}
	}

}
