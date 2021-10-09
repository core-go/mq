package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

func ExecuteAll(ctx context.Context, db *sql.DB, stmts ...Statement) (int64, error) {
	if stmts == nil || len(stmts) == 0 {
		return 0, nil
	}
	tx, er1 := db.Begin()
	if er1 != nil {
		return 0, er1
	}
	var count int64
	count = 0
	for _, stmt := range stmts {
		r2, er3 := tx.ExecContext(ctx, stmt.Query, stmt.Params...)
		if er3 != nil {
			er4 := tx.Rollback()
			if er4 != nil {
				return count, er4
			}
			return count, er3
		}
		a2, er5 := r2.RowsAffected()
		if er5 != nil {
			tx.Rollback()
			return count, er5
		}
		count = count + a2
	}
	er6 := tx.Commit()
	return count, er6
}
func InsertBatch(ctx context.Context, db *sql.DB, tableName string, models interface{}, options ...*Schema) (int64, error) {
	buildParam := GetBuild(db)
	var schema *Schema
	if len(options) > 0 {
		schema = options[0]
	}
	return InsertBatchWithSchema(ctx, db, tableName, models, nil, buildParam, schema)
}
func InsertBatchWithArray(ctx context.Context, db *sql.DB, tableName string, models interface{}, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...*Schema) (int64, error) {
	buildParam := GetBuild(db)
	var schema *Schema
	if len(options) > 0 {
		schema = options[0]
	}
	return InsertBatchWithSchema(ctx, db, tableName, models, toArray, buildParam, schema)
}
func InsertBatchWithSchema(ctx context.Context, db *sql.DB, tableName string, models interface{}, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, buildParam func(i int) string, options ...*Schema) (int64, error) {
	if buildParam == nil {
		buildParam = GetBuild(db)
	}
	driver := GetDriver(db)
	query, args, er1 := BuildToInsertBatchWithSchema(tableName, models, driver, toArray, buildParam, options...)
	if er1 != nil {
		return 0, er1
	}
	x, er2 := db.ExecContext(ctx, query, args...)
	if er2 != nil {
		return 0, er2
	}
	return x.RowsAffected()
}
func UpdateBatch(ctx context.Context, db *sql.DB, tableName string, models interface{}, options ...*Schema) (int64, error) {
	buildParam := GetBuild(db)
	driver := GetDriver(db)
	boolSupport := driver == DriverPostgres
	return UpdateBatchWithVersion(ctx, db, tableName, models, -1, nil, buildParam, boolSupport, options...)
}
func UpdateBatchWithArray(ctx context.Context, db *sql.DB, tableName string, models interface{}, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...*Schema) (int64, error) {
	buildParam := GetBuild(db)
	driver := GetDriver(db)
	boolSupport := driver == DriverPostgres
	return UpdateBatchWithVersion(ctx, db, tableName, models, -1, toArray, buildParam, boolSupport, options...)
}
func UpdateBatchWithVersion(ctx context.Context, db *sql.DB, tableName string, models interface{}, versionIndex int, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, buildParam func(int) string, boolSupport bool, options ...*Schema) (int64, error) {
	if buildParam == nil {
		buildParam = GetBuild(db)
	}
	stmts, er1 := BuildToUpdateBatchWithVersion(tableName, models, versionIndex, buildParam, boolSupport, toArray, options...)
	if er1 != nil {
		return 0, er1
	}
	return ExecuteAll(ctx, db, stmts...)
}

func Save(ctx context.Context, db *sql.DB, table string, model interface{}, options...*Schema) (int64, error) {
	return SaveWithArray(ctx, db, table, model, nil, options...)
}
func SaveWithArray(ctx context.Context, db *sql.DB, table string, model interface{}, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options...*Schema) (int64, error) {
	drive := GetDriver(db)
	buildParam := GetBuild(db)
	queryString, value, err := BuildToSaveWithSchema(table, model, drive, buildParam, toArray, options...)
	if err != nil {
		return 0, err
	}
	res, err := db.ExecContext(ctx, queryString, value...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
func SaveBatch(ctx context.Context, db *sql.DB, tableName string, models interface{}, options ...*Schema) (int64, error) {
	var schema *Schema
	if len(options) > 0 {
		schema = options[0]
	}
	return SaveBatchWithArray(ctx, db, tableName, models, nil, schema)
}
func SaveBatchWithArray(ctx context.Context, db *sql.DB, tableName string, models interface{}, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...*Schema) (int64, error) {
	driver := GetDriver(db)
	stmts, er1 := BuildToSaveBatchWithArray(tableName, models, driver, toArray, options...)
	if er1 != nil {
		return 0, er1
	}
	_, err := ExecuteAll(ctx, db, stmts...)
	total := int64(len(stmts))
	return total, err
}