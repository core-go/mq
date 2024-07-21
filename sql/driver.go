package sql

import (
	"database/sql"
	"reflect"
	"strconv"
)

const (
	DriverPostgres   = "postgres"
	DriverMysql      = "mysql"
	DriverMssql      = "mssql"
	DriverOracle     = "oracle"
	DriverSqlite3    = "sqlite3"
	DriverNotSupport = "no support"
)

func GetDriver(db *sql.DB) string {
	if db == nil {
		return DriverNotSupport
	}
	driver := reflect.TypeOf(db.Driver()).String()
	switch driver {
	case "*pq.Driver":
		return DriverPostgres
	case "*godror.drv":
		return DriverOracle
	case "*mysql.MySQLDriver":
		return DriverMysql
	case "*mssql.Driver":
		return DriverMssql
	case "*sqlite3.SQLiteDriver":
		return DriverSqlite3
	default:
		return DriverNotSupport
	}
}
func BuildParam(i int) string {
	return "?"
}
func BuildOracleParam(i int) string {
	return ":" + strconv.Itoa(i)
}
func BuildMsSqlParam(i int) string {
	return "@p" + strconv.Itoa(i)
}
func BuildDollarParam(i int) string {
	return "$" + strconv.Itoa(i)
}
func GetBuildByDriver(driver string) func(i int) string {
	switch driver {
	case DriverPostgres:
		return BuildDollarParam
	case DriverOracle:
		return BuildOracleParam
	case DriverMssql:
		return BuildMsSqlParam
	default:
		return BuildParam
	}
}
func GetBuild(db *sql.DB) func(i int) string {
	driver := reflect.TypeOf(db.Driver()).String()
	switch driver {
	case "*pq.Driver":
		return BuildDollarParam
	case "*godror.drv":
		return BuildOracleParam
	case "*mssql.Driver":
		return BuildMsSqlParam
	default:
		return BuildParam
	}
}
