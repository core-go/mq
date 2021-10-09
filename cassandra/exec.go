package cassandra

import (
	"context"
	"github.com/gocql/gocql"
)

func Exec(ses *gocql.Session, query string, values...interface{}) (int64, error) {
	q := ses.Query(query, values...)
	err := q.Exec()
	if err != nil {
		return 0, err
	}
	return 1, nil
}
func ExecuteAll(ctx context.Context, ses *gocql.Session, stmts ...Statement) (int64, error) {
	return ExecuteAllWithSize(ctx, ses, 5, stmts...)
}
func ExecuteAllWithSize(ctx context.Context, ses *gocql.Session, size int, stmts ...Statement) (int64, error) {
	if stmts == nil || len(stmts) == 0 {
		return 0, nil
	}
	batch := ses.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	l := len(stmts)
	for i := 0; i < l; i++ {
		var args []interface{}
		args = stmts[i].Params
		batch.Entries = append(batch.Entries, gocql.BatchEntry{
			Stmt:       stmts[i].Query,
			Args:       args,
			Idempotent: true,
		})
		if i % size == 0 || i == l - 1 {
			err := ses.ExecuteBatch(batch)
			if err != nil {
				return int64(i + 1) , err
			}
			batch = ses.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
		}
	}
	return int64(l), nil
}

func Insert(ses *gocql.Session, table string, model interface{}, options ...*Schema) (int64, error) {
	return InsertWithVersion(ses, table, model, -1, options...)
}
func InsertWithVersion(ses *gocql.Session, table string, model interface{}, versionIndex int, options ...*Schema) (int64, error) {
	query, values := BuildToInsertWithVersion(table, model, versionIndex, false, options...)
	return Exec(ses, query, values...)
}
func Update(ses *gocql.Session, table string, model interface{}, options ...*Schema) (int64, error) {
	return UpdateWithVersion(ses, table, model, -1, options...)
}
func UpdateWithVersion(ses *gocql.Session, table string,  model interface{}, versionIndex int, options ...*Schema) (int64, error) {
	query, values := BuildToUpdateWithVersion(table, model, versionIndex, options...)
	return Exec(ses, query, values...)
}
func Save(ses *gocql.Session, table string,  model interface{}, options ...*Schema) (int64, error) {
	query, values := BuildToSave(table, model, options...)
	return Exec(ses, query, values...)
}

func InsertBatchWithSizeAndVersion(ctx context.Context, ses *gocql.Session, size int, table string, models interface{}, versionIndex int, options ...*Schema) (int64, error) {
	s, err := BuildToInsertBatchWithVersion(table, models, versionIndex, false, options...)
	if err != nil {
		return -1, err
	}
	return ExecuteAllWithSize(ctx, ses, size, s...)
}
func InsertBatchWithVersion(ctx context.Context, ses *gocql.Session, table string, models interface{}, versionIndex int, options ...*Schema) (int64, error) {
	return InsertBatchWithSizeAndVersion(ctx, ses, 5, table, models, versionIndex, options...)
}
func InsertBatch(ctx context.Context, ses *gocql.Session, table string, models interface{}, options ...*Schema) (int64, error) {
	return InsertBatchWithSizeAndVersion(ctx, ses, 5, table, models, -1, options...)
}
func UpdateBatchWithSizeAndVersion(ctx context.Context, ses *gocql.Session, size int, table string, models interface{}, versionIndex int, options ...*Schema) (int64, error) {
	s, err := BuildToUpdateBatchWithVersion(table, models, versionIndex, options...)
	if err != nil {
		return -1, err
	}
	return ExecuteAllWithSize(ctx, ses, size, s...)
}
func UpdateBatchWithVersion(ctx context.Context, ses *gocql.Session, table string, models interface{}, versionIndex int, options ...*Schema) (int64, error) {
	return UpdateBatchWithSizeAndVersion(ctx, ses, 5, table, models, versionIndex, options...)
}
func UpdateBatch(ctx context.Context, ses *gocql.Session, table string, models interface{}, options ...*Schema) (int64, error) {
	return UpdateBatchWithSizeAndVersion(ctx, ses, 5, table, models, -1, options...)
}
func SaveBatchWithSize(ctx context.Context, ses *gocql.Session, size int, table string, models interface{}, options ...*Schema) (int64, error) {
	s, err := BuildToInsertBatchWithVersion(table, models, -1, true, options...)
	if err != nil {
		return -1, err
	}
	return ExecuteAllWithSize(ctx, ses, size, s...)
}
func SaveBatch(ctx context.Context, ses *gocql.Session, table string, models interface{}, options ...*Schema) (int64, error) {
	return SaveBatchWithSize(ctx, ses, 5, table, models, options...)
}
