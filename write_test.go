package datadirectory

import (
	"bytes"
	"testing"
)

func TestWriteMetadata(t *testing.T) {

	var (
		d         *DataDirectory
		recordMap map[string]string
		b         bytes.Buffer
		err       error
	)

	const metadata = "\"foo\",\"bar\",\"baz\"\n\"boo\",\"far\",\"faz\"\n"

	d = &DataDirectory{
		header:     []string{"foo", "bar", "baz"},
		RecordMaps: make([]map[string]string, 0),
	}

	recordMap = make(map[string]string)
	d.RecordMaps = append(d.RecordMaps, recordMap)

	recordMap["foo"] = "boo"
	recordMap["bar"] = "far"
	recordMap["baz"] = "faz"

	if err = d.WriteMetadata(&b); err != nil {
		t.Errorf("WriteMetadata(): error in basic function: %s", err)
	}

	if b.String() != metadata {
		t.Errorf("WriteMetadata(): expected output ('%s') does not match actual output ('%s')", metadata, b.String())
	}

}
