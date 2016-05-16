package datadirectory_test

import (
	"strings"
	"testing"

	"github.com/infomodels/datadirectory"
)

func TestReadMetadataNumRecords(t *testing.T) {

	var (
		d   *datadirectory.DataDirectory
		err error
	)

	const metadata = "organization,filename,checksum,cdm,table,etl\nfoo,./data,456,pedsnet,Person,http://foo.org/etl\n"

	d = &datadirectory.DataDirectory{}

	if err = d.ReadMetadata(strings.NewReader(metadata)); err != nil {
		t.Errorf("ReadMetadata(): error in basic function: %s", err)
	}

	if len(d.RecordMaps) != 1 {
		t.Errorf("ReadMetadata(): expected number of RecordMaps (1) does not match actual length (%d)", len(d.RecordMaps))
	}

}

func TestReadMetadataLenRecord(t *testing.T) {

	var (
		d   *datadirectory.DataDirectory
		err error
	)

	const metadata = "organization,filename,checksum,cdm,table,etl\nfoo,./data,456,pedsnet,Person,http://foo.org/etl\n"

	d = &datadirectory.DataDirectory{}

	if err = d.ReadMetadata(strings.NewReader(metadata)); err != nil {
		t.Errorf("ReadMetadata(): error in basic function: %s", err)
	}

	if len(d.RecordMaps[0]) != 7 {
		t.Errorf("ReadMetadata(): expected length of RecordMap (7) does not match actual length (%d)", len(d.RecordMaps[0]))
	}

}

func TestReadMetadataRecordLine(t *testing.T) {

	var (
		d   *datadirectory.DataDirectory
		err error
	)

	const metadata = "organization,filename,checksum,cdm,table,etl\nfoo,./data,456,pedsnet,Person,http://foo.org/etl\n"

	d = &datadirectory.DataDirectory{}

	if err = d.ReadMetadata(strings.NewReader(metadata)); err != nil {
		t.Errorf("ReadMetadata(): error in basic function: %s", err)
	}

	if d.RecordMaps[0]["line"] != "2" {
		t.Errorf("ReadMetadata(): expected 'line' of RecordMap ('2') does not match actual 'line' ('%s')", d.RecordMaps[0]["line"])
	}

}

func TestReadMetadataRecordContent(t *testing.T) {

	var (
		d   *datadirectory.DataDirectory
		err error
	)

	const metadata = "organization,filename,checksum,cdm,table,etl\nfoo,./data,456,pedsnet,Person,http://foo.org/etl\n"

	d = &datadirectory.DataDirectory{}

	if err = d.ReadMetadata(strings.NewReader(metadata)); err != nil {
		t.Errorf("ReadMetadata(): error in basic function: %s", err)
	}

	if d.RecordMaps[0]["table"] != "person" {
		t.Errorf("ReadMetadata(): expected 'table' of RecordMap ('person') does not match actual 'table' ('%s')", d.RecordMaps[0]["table"])
	}

}

func TestReadMetadataUnknownHeader(t *testing.T) {

	var (
		d   *datadirectory.DataDirectory
		err error
	)

	const metadata = "foo\nbar\n"

	d = &datadirectory.DataDirectory{}

	if err = d.ReadMetadata(strings.NewReader(metadata)); err == nil {
		t.Errorf("ReadMetadata(): no error thrown for unknown header")
	}

}

func TestReadMetadataMissingHeader(t *testing.T) {

	var (
		d   *datadirectory.DataDirectory
		err error
	)

	const metadata = "organization\nbar\n"

	d = &datadirectory.DataDirectory{}

	if err = d.ReadMetadata(strings.NewReader(metadata)); err == nil {
		t.Errorf("ReadMetadata(): no error thrown for missing header")
	}

}
