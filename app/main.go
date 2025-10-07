package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: sqlite <database> <command>")
		os.Exit(1)
	}

	databaseFilePath := os.Args[1]
	command := os.Args[2]

	// Check if it's a SQL query
	if len(command) > 6 && (command[:6] == "SELECT" || command[:6] == "select") {
		handleSQLQuery(databaseFilePath, command)
		return
	}

	// Handle dot commands
	switch command {
	case ".dbinfo":
		handleDbInfo(databaseFilePath)
	case ".tables":
		handleTables(databaseFilePath)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

// handleDbInfo handles the .dbinfo command
func handleDbInfo(databaseFilePath string) {
	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer databaseFile.Close()

	// Read the first 108 bytes to include file header and B-tree page header
	header := make([]byte, 108)
	_, err = databaseFile.Read(header)
	if err != nil {
		log.Fatal(err)
	}

	// Read page size from file header
	var pageSize uint16
	if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
		fmt.Println("Failed to read page size:", err)
		return
	}

	// Read number of cells from B-tree page header (offset 103-104 in page 1)
	var cellCount uint16
	if err := binary.Read(bytes.NewReader(header[103:105]), binary.BigEndian, &cellCount); err != nil {
		fmt.Println("Failed to read cell count:", err)
		return
	}

	fmt.Fprintf(os.Stderr, "Logs from your program will appear here!\n")
	fmt.Printf("database page size: %v\n", pageSize)
	fmt.Printf("number of tables: %v\n", cellCount)
}

// handleTables handles the .tables command
func handleTables(databaseFilePath string) {
	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer databaseFile.Close()

	// Read page size
	var pageSize uint16
	pageSizeBytes := make([]byte, 2)
	_, err = databaseFile.ReadAt(pageSizeBytes, 16)
	if err != nil {
		log.Fatal(err)
	}
	binary.Read(bytes.NewReader(pageSizeBytes), binary.BigEndian, &pageSize)

	// Read the entire first page
	page := make([]byte, pageSize)
	_, err = databaseFile.ReadAt(page, 0)
	if err != nil {
		log.Fatal(err)
	}

	// Read cell count from page header
	var cellCount uint16
	binary.Read(bytes.NewReader(page[103:105]), binary.BigEndian, &cellCount)

	// Read cell pointer array
	cellPointers := make([]uint16, cellCount)
	for i := 0; i < int(cellCount); i++ {
		offset := 108 + i*2
		binary.Read(bytes.NewReader(page[offset:offset+2]), binary.BigEndian, &cellPointers[i])
	}

	// Parse each cell to extract table names
	var tableNames []string
	for _, cellOffset := range cellPointers {
		cellData := page[cellOffset:]

		// Read record size (varint)
		_, bytesRead := readVarint(cellData)
		cellData = cellData[bytesRead:]

		// Read rowid (varint) - skip it
		_, bytesRead = readVarint(cellData)
		cellData = cellData[bytesRead:]

		// Read record header size
		headerSize, bytesRead := readVarint(cellData)
		headerData := cellData[bytesRead:headerSize]
		cellData = cellData[headerSize:]

		// Read serial types from header
		var serialTypes []uint64
		headerBytesRead := 0
		for headerBytesRead < len(headerData) {
			serialType, bytes := readVarint(headerData[headerBytesRead:])
			if bytes == 0 {
				break
			}
			serialTypes = append(serialTypes, serialType)
			headerBytesRead += bytes
		}

		// Skip first two columns (type, name)
		typeSize := getSerialTypeSize(serialTypes[0])
		cellData = cellData[typeSize:]
		nameSize := getSerialTypeSize(serialTypes[1])
		cellData = cellData[nameSize:]

		// Read tbl_name column (3rd column)
		tblNameSize := getSerialTypeSize(serialTypes[2])
		tblName := string(cellData[:tblNameSize])

		// Filter out internal sqlite tables
		if len(tblName) < 7 || tblName[:7] != "sqlite_" {
			tableNames = append(tableNames, tblName)
		}
	}

	// Print table names separated by space
	for i, name := range tableNames {
		if i > 0 {
			fmt.Print(" ")
		}
		fmt.Print(name)
	}
	fmt.Println()
}

// handleSQLQuery handles SQL queries
func handleSQLQuery(databaseFilePath string, query string) {
	// Parse the SQL query
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		log.Fatal(err)
	}

	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		log.Fatal("Not a SELECT statement")
	}

	// Extract table name
	tableName := sqlparser.String(selectStmt.From[0])

	// Check if it's COUNT(*) or extract column names
	isCountQuery := false
	var columnNames []string
	for _, selectExpr := range selectStmt.SelectExprs {
		switch expr := selectExpr.(type) {
		case *sqlparser.StarExpr:
			columnNames = append(columnNames, "*")
		case *sqlparser.AliasedExpr:
			if funcExpr, ok := expr.Expr.(*sqlparser.FuncExpr); ok {
				if strings.ToUpper(funcExpr.Name.String()) == "COUNT" {
					isCountQuery = true
				}
			} else {
				columnNames = append(columnNames, sqlparser.String(expr.Expr))
			}
		}
	}

	// Parse WHERE clause if present
	var whereColumn string
	var whereValue string
	hasWhere := false
	if selectStmt.Where != nil {
		if comparisonExpr, ok := selectStmt.Where.Expr.(*sqlparser.ComparisonExpr); ok {
			if comparisonExpr.Operator == "=" {
				whereColumn = sqlparser.String(comparisonExpr.Left)
				whereValueRaw := sqlparser.String(comparisonExpr.Right)
				whereValue = strings.Trim(whereValueRaw, "'\"")
				hasWhere = true
			}
		}
	}

	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer databaseFile.Close()

	// Read page size
	var pageSize uint16
	pageSizeBytes := make([]byte, 2)
	_, err = databaseFile.ReadAt(pageSizeBytes, 16)
	if err != nil {
		log.Fatal(err)
	}
	binary.Read(bytes.NewReader(pageSizeBytes), binary.BigEndian, &pageSize)

	// Read the first page (sqlite_schema)
	page := make([]byte, pageSize)
	_, err = databaseFile.ReadAt(page, 0)
	if err != nil {
		log.Fatal(err)
	}

	// Find the rootpage and CREATE TABLE statement for the given table
	rootpage, createTableSQL := findTableInfo(page, tableName)
	if rootpage == 0 {
		fmt.Printf("Table %s not found\n", tableName)
		os.Exit(1)
	}

	// Handle COUNT queries
	if isCountQuery {
		count := countRows(databaseFile, int64(pageSize), rootpage)
		fmt.Println(count)
		return
	}

	// Parse CREATE TABLE to get column indices for all requested columns
	var columnIndices []int
	for _, colName := range columnNames {
		colIndex := getColumnIndex(createTableSQL, colName)
		if colIndex == -1 {
			fmt.Printf("Column %s not found\n", colName)
			os.Exit(1)
		}
		columnIndices = append(columnIndices, colIndex)
	}

	// Get WHERE column index if needed
	var whereColumnIndex int = -1
	if hasWhere {
		whereColumnIndex = getColumnIndex(createTableSQL, whereColumn)
		if whereColumnIndex == -1 {
			fmt.Printf("WHERE column %s not found\n", whereColumn)
			os.Exit(1)
		}
	}

	// Select and print rows
	selectRows(databaseFile, int64(pageSize), rootpage, columnIndices, columnNames, createTableSQL, hasWhere, whereColumnIndex, whereValue)
}
