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

// readVarint reads a varint from the byte slice and returns the value and number of bytes read
func readVarint(data []byte) (uint64, int) {
	var result uint64
	for i := 0; i < 9; i++ {
		if i >= len(data) {
			return 0, 0
		}
		b := data[i]
		if i == 8 {
			// 9th byte: use all 8 bits
			result = (result << 8) | uint64(b)
			return result, i + 1
		}
		// Use lower 7 bits
		result = (result << 7) | uint64(b&0x7F)
		if b&0x80 == 0 {
			// High bit is 0, this is the last byte
			return result, i + 1
		}
	}
	return result, 9
}

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	// Check if it's a SQL query
	if len(command) > 6 && (command[:6] == "SELECT" || command[:6] == "select") {
		handleSQLQuery(databaseFilePath, command)
		return
	}

	switch command {
	case ".dbinfo":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		// Read the first 108 bytes to include file header and B-tree page header
		header := make([]byte, 108)

		_, err = databaseFile.Read(header)
		if err != nil {
			log.Fatal(err)
		}

		var pageSize uint16
		if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}

		// Read number of cells from the B-tree page header
		// The page header starts at offset 100 (after the 100-byte file header)
		// Cell count is at offset 3-4 of the page header (offset 103-104 in the file)
		var cellCount uint16
		if err := binary.Read(bytes.NewReader(header[103:105]), binary.BigEndian, &cellCount); err != nil {
			fmt.Println("Failed to read cell count:", err)
			return
		}

		// You can use print statements as follows for debugging, they'll be visible when running tests.
		fmt.Fprintln(os.Stderr, "Logs from your program will appear here!")

		// Uncomment this to pass the first stage
		fmt.Printf("database page size: %v\n", pageSize)
		fmt.Printf("number of tables: %v\n", cellCount)
	case ".tables":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}
		defer databaseFile.Close()

		// Read the first page (page 1, which contains sqlite_schema)
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

		// Read cell count from page header (offset 103-104 in page 1)
		var cellCount uint16
		binary.Read(bytes.NewReader(page[103:105]), binary.BigEndian, &cellCount)

		// Read cell pointer array (starts at offset 108, after 8-byte page header)
		// Note: On page 1, the page header starts at offset 100 (after the 100-byte file header)
		cellPointers := make([]uint16, cellCount)
		for i := 0; i < int(cellCount); i++ {
			offset := 108 + i*2
			binary.Read(bytes.NewReader(page[offset:offset+2]), binary.BigEndian, &cellPointers[i])
		}

		// Parse each cell to extract table names
		var tableNames []string
		for _, cellOffset := range cellPointers {
			// Read the cell
			cellData := page[cellOffset:]

			// Read record size (varint)
			recordSize, bytesRead := readVarint(cellData)
			_ = recordSize // We don't need this for now
			cellData = cellData[bytesRead:]

			// Read rowid (varint) - skip it
			_, bytesRead = readVarint(cellData)
			cellData = cellData[bytesRead:]

			// Now we're at the record
			// Read record header size
			headerSize, bytesRead := readVarint(cellData)
			headerStart := cellData
			cellData = cellData[bytesRead:]

			// Read serial types from header
			var serialTypes []uint64
			headerBytesRead := bytesRead
			for headerBytesRead < int(headerSize) {
				serialType, bytes := readVarint(cellData)
				serialTypes = append(serialTypes, serialType)
				cellData = cellData[bytes:]
				headerBytesRead += bytes
			}

			// Now cellData points to the record body
			// We need to read the columns based on serial types
			// sqlite_schema columns: type, name, tbl_name, rootpage, sql

			// Skip the first column (type)
			typeSize := getSerialTypeSize(serialTypes[0])
			cellData = cellData[typeSize:]

			// Skip the second column (name)
			nameSize := getSerialTypeSize(serialTypes[1])
			cellData = cellData[nameSize:]

			// Read the third column (tbl_name) - this is what we want!
			tblNameSize := getSerialTypeSize(serialTypes[2])
			tblName := string(cellData[:tblNameSize])

			// Filter out internal sqlite tables (those starting with "sqlite_")
			if len(tblName) < 7 || tblName[:7] != "sqlite_" {
				tableNames = append(tableNames, tblName)
			}

			// Reset cellData to point after the header for debugging
			cellData = headerStart[headerSize:]
		}

		// Print table names separated by space
		for i, name := range tableNames {
			if i > 0 {
				fmt.Print(" ")
			}
			fmt.Print(name)
		}
		fmt.Println()
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

// getSerialTypeSize returns the size in bytes for a given serial type
func getSerialTypeSize(serialType uint64) int {
	if serialType == 0 {
		return 0 // NULL
	} else if serialType == 1 {
		return 1 // 8-bit int
	} else if serialType == 2 {
		return 2 // 16-bit int
	} else if serialType == 3 {
		return 3 // 24-bit int
	} else if serialType == 4 {
		return 4 // 32-bit int
	} else if serialType == 5 {
		return 6 // 48-bit int
	} else if serialType == 6 {
		return 8 // 64-bit int
	} else if serialType == 7 {
		return 8 // float
	} else if serialType == 8 || serialType == 9 {
		return 0 // constant 0 or 1
	} else if serialType >= 12 && serialType%2 == 0 {
		// BLOB: (N-12)/2 bytes
		return int((serialType - 12) / 2)
	} else if serialType >= 13 && serialType%2 == 1 {
		// String: (N-13)/2 bytes
		return int((serialType - 13) / 2)
	}
	return 0
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
			// SELECT *
			columnNames = append(columnNames, "*")
		case *sqlparser.AliasedExpr:
			// Check if it's COUNT(*)
			if funcExpr, ok := expr.Expr.(*sqlparser.FuncExpr); ok {
				if strings.ToUpper(funcExpr.Name.String()) == "COUNT" {
					isCountQuery = true
				}
			} else {
				// Regular column selection
				columnNames = append(columnNames, sqlparser.String(expr.Expr))
			}
		}
	}

	// Parse WHERE clause if present
	var whereColumn string
	var whereValue string
	hasWhere := false
	if selectStmt.Where != nil {
		// Simple WHERE parsing: column = 'value'
		if comparisonExpr, ok := selectStmt.Where.Expr.(*sqlparser.ComparisonExpr); ok {
			if comparisonExpr.Operator == "=" {
				whereColumn = sqlparser.String(comparisonExpr.Left)
				// Extract the value (remove quotes)
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

	// For COUNT queries, we need to count all rows across all pages
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

	// Traverse B-tree and print matching rows
	traverseAndPrint(databaseFile, int64(pageSize), rootpage, columnIndices, hasWhere, whereColumnIndex, whereValue, createTableSQL, columnNames)
}

// findTableInfo searches sqlite_schema for the table and returns its rootpage and CREATE TABLE SQL
func findTableInfo(page []byte, tableName string) (int, string) {
	// Read cell count from page header (offset 103-104 in page 1)
	var cellCount uint16
	binary.Read(bytes.NewReader(page[103:105]), binary.BigEndian, &cellCount)

	// Read cell pointer array
	cellPointers := make([]uint16, cellCount)
	for i := 0; i < int(cellCount); i++ {
		offset := 108 + i*2
		binary.Read(bytes.NewReader(page[offset:offset+2]), binary.BigEndian, &cellPointers[i])
	}

	// Parse each cell to find the table
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
		cellData = cellData[bytesRead:]

		// Read serial types from header
		var serialTypes []uint64
		headerBytesRead := bytesRead
		for headerBytesRead < int(headerSize) {
			serialType, bytes := readVarint(cellData)
			serialTypes = append(serialTypes, serialType)
			cellData = cellData[bytes:]
			headerBytesRead += bytes
		}

		// Now we're at the record body
		// sqlite_schema columns: type, name, tbl_name, rootpage, sql
		bodyStart := cellData

		// Skip type column
		typeSize := getSerialTypeSize(serialTypes[0])
		cellData = cellData[typeSize:]

		// Skip name column
		nameSize := getSerialTypeSize(serialTypes[1])
		cellData = cellData[nameSize:]

		// Read tbl_name column
		tblNameSize := getSerialTypeSize(serialTypes[2])
		tblName := string(cellData[:tblNameSize])
		cellData = cellData[tblNameSize:]

		// Check if this is the table we're looking for
		if tblName == tableName {
			// Read rootpage column (serial type should be 1 for 8-bit int)
			rootpageValue := int(cellData[0])
			cellData = cellData[1:]

			// Read sql column (5th column)
			sqlSize := getSerialTypeSize(serialTypes[4])
			sqlText := string(cellData[:sqlSize])

			return rootpageValue, sqlText
		}

		// Reset for next iteration
		cellData = bodyStart
	}

	return 0, ""
}

// getColumnIndex parses the CREATE TABLE statement and returns the index of the given column
func getColumnIndex(createTableSQL string, columnName string) int {
	// Simple parser: extract column names from CREATE TABLE statement
	// Find the opening parenthesis
	startIdx := strings.Index(createTableSQL, "(")
	if startIdx == -1 {
		return -1
	}

	// Find the closing parenthesis
	endIdx := strings.LastIndex(createTableSQL, ")")
	if endIdx == -1 {
		return -1
	}

	// Extract the columns section
	columnsStr := createTableSQL[startIdx+1 : endIdx]

	// Split by comma to get individual column definitions
	// This is a simplified approach that works for basic schemas
	columns := strings.Split(columnsStr, ",")

	for i, colDef := range columns {
		// Extract the column name (first word after trimming)
		colDef = strings.TrimSpace(colDef)
		parts := strings.Fields(colDef)
		if len(parts) > 0 {
			colName := parts[0]
			if strings.EqualFold(colName, columnName) {
				return i
			}
		}
	}

	return -1
}

// countRows counts all rows in a B-tree by traversing all pages
func countRows(file *os.File, pageSize int64, pageNum int) int {
	pageOffset := int64(pageNum-1) * pageSize
	page := make([]byte, pageSize)
	_, err := file.ReadAt(page, pageOffset)
	if err != nil {
		return 0
	}

	// Determine page header offset
	headerOffset := 0
	if pageNum == 1 {
		headerOffset = 100 // First page has 100-byte file header
	}

	pageType := page[headerOffset]
	var cellCount uint16
	binary.Read(bytes.NewReader(page[headerOffset+3:headerOffset+5]), binary.BigEndian, &cellCount)

	if pageType == 0x0d {
		// Leaf page - return cell count
		return int(cellCount)
	} else if pageType == 0x05 {
		// Interior page - traverse all child pages
		totalCount := 0

		// Read rightmost pointer (4 bytes at offset 8 in page header)
		var rightmostPointer uint32
		binary.Read(bytes.NewReader(page[headerOffset+8:headerOffset+12]), binary.BigEndian, &rightmostPointer)

		// Read cell pointer array
		cellPointerOffset := headerOffset + 12 // Interior page header is 12 bytes
		for i := 0; i < int(cellCount); i++ {
			var cellPointer uint16
			offset := cellPointerOffset + i*2
			binary.Read(bytes.NewReader(page[offset:offset+2]), binary.BigEndian, &cellPointer)

			// Read left child pointer from cell (first 4 bytes)
			var leftChildPointer uint32
			binary.Read(bytes.NewReader(page[cellPointer:cellPointer+4]), binary.BigEndian, &leftChildPointer)

			// Recursively count rows in left child
			totalCount += countRows(file, pageSize, int(leftChildPointer))
		}

		// Add rows from rightmost child
		totalCount += countRows(file, pageSize, int(rightmostPointer))

		return totalCount
	}

	return 0
}

// traverseAndPrint traverses the B-tree and prints rows matching the criteria
func traverseAndPrint(file *os.File, pageSize int64, pageNum int, columnIndices []int, hasWhere bool, whereColumnIndex int, whereValue string, createTableSQL string, columnNames []string) {
	pageOffset := int64(pageNum-1) * pageSize
	page := make([]byte, pageSize)
	_, err := file.ReadAt(page, pageOffset)
	if err != nil {
		return
	}

	// Determine page header offset
	headerOffset := 0
	if pageNum == 1 {
		headerOffset = 100 // First page has 100-byte file header
	}

	pageType := page[headerOffset]
	var cellCount uint16
	binary.Read(bytes.NewReader(page[headerOffset+3:headerOffset+5]), binary.BigEndian, &cellCount)

	if pageType == 0x0d {
		// Leaf page - process all cells
		cellPointerOffset := headerOffset + 8 // Leaf page header is 8 bytes
		for i := 0; i < int(cellCount); i++ {
			var cellPointer uint16
			offset := cellPointerOffset + i*2
			binary.Read(bytes.NewReader(page[offset:offset+2]), binary.BigEndian, &cellPointer)

			processCell(page[cellPointer:], columnIndices, hasWhere, whereColumnIndex, whereValue, createTableSQL, columnNames)
		}
	} else if pageType == 0x05 {
		// Interior page - traverse all child pages
		// Read rightmost pointer
		var rightmostPointer uint32
		binary.Read(bytes.NewReader(page[headerOffset+8:headerOffset+12]), binary.BigEndian, &rightmostPointer)

		// Read cell pointer array and traverse left children
		cellPointerOffset := headerOffset + 12 // Interior page header is 12 bytes
		for i := 0; i < int(cellCount); i++ {
			var cellPointer uint16
			offset := cellPointerOffset + i*2
			binary.Read(bytes.NewReader(page[offset:offset+2]), binary.BigEndian, &cellPointer)

			// Read left child pointer from cell (first 4 bytes)
			var leftChildPointer uint32
			binary.Read(bytes.NewReader(page[cellPointer:cellPointer+4]), binary.BigEndian, &leftChildPointer)

			// Recursively traverse left child
			traverseAndPrint(file, pageSize, int(leftChildPointer), columnIndices, hasWhere, whereColumnIndex, whereValue, createTableSQL, columnNames)
		}

		// Traverse rightmost child
		traverseAndPrint(file, pageSize, int(rightmostPointer), columnIndices, hasWhere, whereColumnIndex, whereValue, createTableSQL, columnNames)
	}
}

// processCell extracts and prints column values from a single cell
func processCell(cellData []byte, columnIndices []int, hasWhere bool, whereColumnIndex int, whereValue string, createTableSQL string, columnNames []string) {
	// Read record size (varint)
	_, bytesRead := readVarint(cellData)
	cellData = cellData[bytesRead:]

	// Read rowid (varint) - we need to keep this for INTEGER PRIMARY KEY
	rowid, bytesRead := readVarint(cellData)
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

	// Extract all column values from the record
	allColumnValues := make([]string, len(serialTypes))
	offset := 0
	for i, serialType := range serialTypes {
		colSize := getSerialTypeSize(serialType)
		allColumnValues[i] = readColumnValue(cellData[offset:offset+colSize], serialType)
		offset += colSize
	}

	// Check WHERE condition if present
	if hasWhere {
		if allColumnValues[whereColumnIndex] != whereValue {
			return // Skip this row
		}
	}

	// Check if any requested column is an INTEGER PRIMARY KEY (uses rowid)
	// Parse the schema to detect this
	isPKColumn := make([]bool, len(columnNames))
	for i, colName := range columnNames {
		// Simple heuristic: check if column is "id" and appears as "integer primary key" in schema
		if strings.Contains(strings.ToLower(createTableSQL), strings.ToLower(colName)+" integer primary key") {
			isPKColumn[i] = true
		}
	}

	// Extract only the requested columns in the order they were requested
	var columnValues []string
	for i, colIndex := range columnIndices {
		// If this column is the INTEGER PRIMARY KEY, use rowid instead
		if isPKColumn[i] {
			columnValues = append(columnValues, fmt.Sprintf("%d", rowid))
		} else {
			columnValues = append(columnValues, allColumnValues[colIndex])
		}
	}

	// Print the values separated by |
	fmt.Println(strings.Join(columnValues, "|"))
}

// readColumnValue reads a column value based on its serial type and returns it as a string
func readColumnValue(data []byte, serialType uint64) string {
	if serialType == 0 {
		return "" // NULL
	} else if serialType == 1 {
		// 8-bit twos-complement integer
		return fmt.Sprintf("%d", int8(data[0]))
	} else if serialType == 2 {
		// 16-bit big-endian integer
		var val int16
		binary.Read(bytes.NewReader(data), binary.BigEndian, &val)
		return fmt.Sprintf("%d", val)
	} else if serialType == 3 {
		// 24-bit big-endian integer
		val := int32(data[0])<<16 | int32(data[1])<<8 | int32(data[2])
		// Sign extend if negative
		if val&0x800000 != 0 {
			val |= ^0xFFFFFF
		}
		return fmt.Sprintf("%d", val)
	} else if serialType == 4 {
		// 32-bit big-endian integer
		var val int32
		binary.Read(bytes.NewReader(data), binary.BigEndian, &val)
		return fmt.Sprintf("%d", val)
	} else if serialType == 5 {
		// 48-bit big-endian integer
		val := int64(data[0])<<40 | int64(data[1])<<32 | int64(data[2])<<24 |
			int64(data[3])<<16 | int64(data[4])<<8 | int64(data[5])
		// Sign extend if negative
		if val&0x800000000000 != 0 {
			val |= ^0xFFFFFFFFFFFF
		}
		return fmt.Sprintf("%d", val)
	} else if serialType == 6 {
		// 64-bit big-endian integer
		var val int64
		binary.Read(bytes.NewReader(data), binary.BigEndian, &val)
		return fmt.Sprintf("%d", val)
	} else if serialType == 7 {
		// 64-bit IEEE float
		var val float64
		binary.Read(bytes.NewReader(data), binary.BigEndian, &val)
		return fmt.Sprintf("%f", val)
	} else if serialType == 8 {
		return "0" // constant 0
	} else if serialType == 9 {
		return "1" // constant 1
	} else if serialType >= 12 && serialType%2 == 0 {
		// BLOB
		return string(data)
	} else if serialType >= 13 && serialType%2 == 1 {
		// String
		return string(data)
	}
	return ""
}
