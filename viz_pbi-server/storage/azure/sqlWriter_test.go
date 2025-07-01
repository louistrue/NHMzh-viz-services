package azure

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"viz_pbi-server/models"
)

func TestTimestampConsistency(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("error creating mock: %v", err)
	}
	defer db.Close()

	writer := NewSqlWriter(db)
	ts := "2024-05-06T13:14:15Z"
	truncated, _ := truncateRFC3339(ts)

	// Expect calls for WriteBlobData
	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO data_updates").
		ExpectExec().
		WithArgs("proj", "file.ifc", truncated, "url", "cont", "proj/file.ifc").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	blob := models.BlobData{
		Project:           "proj",
		Filename:          "file.ifc",
		Timestamp:         ts,
		StorageServiceURL: "url",
		Container:         "cont",
		BlobID:            "proj/file.ifc",
	}
	if err := writer.WriteBlobData(blob); err != nil {
		t.Fatalf("WriteBlobData failed: %v", err)
	}

	// Expect calls for WriteElements
	mock.ExpectBegin()
	mock.ExpectExec("MERGE INTO data_eav_elements").
		WithArgs("proj", "file.ifc", truncated, "id1", "param", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), "number").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	element := models.EavElementDataItem{
		Project:   "proj",
		Filename:  "file.ifc",
		Timestamp: ts,
		Id:        "id1",
		ParamName: "param",
		ParamType: "number",
	}
	if err := writer.WriteElements([]models.EavElementDataItem{element}); err != nil {
		t.Fatalf("WriteElements failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
