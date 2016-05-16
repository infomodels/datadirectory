package datadirectory_test

import (
	"testing"

	"github.com/infomodels/datadirectory"
)

func TestNew(t *testing.T) {

	var (
		cfg *datadirectory.Config
		err error
	)

	cfg = &datadirectory.Config{
		DataDirPath: ".",
	}

	if _, err = datadirectory.New(cfg); err != nil {
		t.Errorf("New(): error in basic function, probably with data models service: %s", err)
	}

}

func TestNewModelOnly(t *testing.T) {

	var (
		cfg *datadirectory.Config
		d   *datadirectory.DataDirectory
	)

	cfg = &datadirectory.Config{
		DataDirPath: ".",
		Model:       "pedsnet",
	}

	d, _ = datadirectory.New(cfg)

	if d.ModelVersion == "" {
		t.Errorf("New(): latest ModelVersion not inferred")
	}

}

func TestUnknownModel(t *testing.T) {

	var (
		cfg *datadirectory.Config
		err error
	)

	cfg = &datadirectory.Config{
		DataDirPath: ".",
		Model:       "foo",
	}

	if _, err = datadirectory.New(cfg); err == nil {
		t.Errorf("New(): no error thrown for unknown model")
	}

}
