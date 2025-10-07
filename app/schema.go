package main

import (
	"bytes"
	"encoding/binary"
	"strings"
)

// TableInfo contains information about a database table
type TableInfo struct {
	Name      string
	Rootpage  int
	CreateSQL string
}

// findTableInfo searches sqlite_schema for the table and returns its info
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

// isIntegerPrimaryKey checks if a column is declared as INTEGER PRIMARY KEY
func isIntegerPrimaryKey(createTableSQL string, columnName string) bool {
	return strings.Contains(strings.ToLower(createTableSQL), strings.ToLower(columnName)+" integer primary key")
}
