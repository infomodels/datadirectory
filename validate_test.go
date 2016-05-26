package datadirectory_test

import (
	"testing"

	"github.com/infomodels/datadirectory"
)

func TestValidate(t *testing.T) {

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

	if err = d.ReadMetadataFromFile(); err != nil {
		t.Errorf("ReadMetadataFromFile(): error in basic function: %s", err)
	}

	if err = d.Validate(); err != nil {
		t.Errorf("Validate(): error in basic function: %s", err)
	}

}

func TestValidateMissingValue(t *testing.T) {

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

	if err = d.ReadMetadataFromFile(); err != nil {
		t.Errorf("ReadMetadataFromFile(): error in basic function: %s", err)
	}

	delete(d.RecordMaps[0], "cdm") // Remove a required value from one of the records.

	if err = d.Validate(); err == nil {
		t.Errorf("Validate(): no error thrown for missing required value")
	}

}

func TestValidateMismatchSite(t *testing.T) {

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

	if err = d.ReadMetadataFromFile(); err != nil {
		t.Errorf("ReadMetadataFromFile(): error in basic function: %s", err)
	}

	d.RecordMaps[0]["organization"] = "foobar" // Change to a different Site.

	if err = d.Validate(); err == nil {
		t.Errorf("Validate(): no error thrown for mismatched Site")
	}

}

func TestValidateBogusModel(t *testing.T) {

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

	if err = d.ReadMetadataFromFile(); err != nil {
		t.Errorf("ReadMetadataFromFile(): error in basic function: %s", err)
	}

	d.RecordMaps[0]["cdm"] = "foobar" // Change to a bogus model.

	if err = d.Validate(); err == nil {
		t.Errorf("Validate(): no error thrown for bogus Model")
	}

}

func TestValidateBogusModelVersion(t *testing.T) {

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

	if err = d.ReadMetadataFromFile(); err != nil {
		t.Errorf("ReadMetadataFromFile(): error in basic function: %s", err)
	}

	d.RecordMaps[0]["cdm-version"] = "0.0.1" // Change to a bogus model version.

	if err = d.Validate(); err == nil {
		t.Errorf("Validate(): no error thrown for bogus ModelVersion")
	}

}

func TestValidateMismatchModel(t *testing.T) {

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

	if err = d.ReadMetadataFromFile(); err != nil {
		t.Errorf("ReadMetadataFromFile(): error in basic function: %s", err)
	}

	d.RecordMaps[0]["cdm"] = "pcornet"       // Change to a mismatched model.
	d.RecordMaps[0]["cdm-version"] = "1.0.0" // With a valid version for that model.

	if err = d.Validate(); err == nil {
		t.Errorf("Validate(): no error thrown for mismatched Model")
	}

}

func TestValidateMismatchModelVersion(t *testing.T) {

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

	if err = d.ReadMetadataFromFile(); err != nil {
		t.Errorf("ReadMetadataFromFile(): error in basic function: %s", err)
	}

	d.RecordMaps[0]["cdm-version"] = "2.2.0" // Change to a mismatched model version.

	if err = d.Validate(); err == nil {
		t.Errorf("Validate(): no error thrown for mismatched ModelVersion")
	}

}

func TestValidateBogusTable(t *testing.T) {

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

	if err = d.ReadMetadataFromFile(); err != nil {
		t.Errorf("ReadMetadataFromFile(): error in basic function: %s", err)
	}

	d.RecordMaps[0]["table"] = "foobar" // Change to a bogus table.

	if err = d.Validate(); err == nil {
		t.Errorf("Validate(): no error thrown for bogus table")
	}

}

func TestValidateMismatchDataVersion(t *testing.T) {

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

	if err = d.ReadMetadataFromFile(); err != nil {
		t.Errorf("ReadMetadataFromFile(): error in basic function: %s", err)
	}

	d.RecordMaps[0]["data-version"] = "5" // Change to a mismatched data version.

	if err = d.Validate(); err == nil {
		t.Errorf("Validate(): no error thrown for mismatched data version")
	}

}

func TestValidateMismatchChecksum(t *testing.T) {

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

	if err = d.ReadMetadataFromFile(); err != nil {
		t.Errorf("ReadMetadataFromFile(): error in basic function: %s", err)
	}

	d.RecordMaps[0]["checksum"] = "123abc" // Change to a mismatched checksum.

	if err = d.Validate(); err == nil {
		t.Errorf("Validate(): no error thrown for mismatched checksum")
	}

}
