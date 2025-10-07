package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
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
	// For now, we only handle SELECT COUNT(*) FROM <table>
	// Simple parsing: split by space and get the last word as table name
	parts := bytes.Fields([]byte(query))
	tableName := string(parts[len(parts)-1])

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

	// Find the rootpage for the given table
	rootpage := findRootpage(page, tableName)
	if rootpage == 0 {
		fmt.Printf("Table %s not found\n", tableName)
		os.Exit(1)
	}

	// Read the table's root page
	tablePageOffset := int64((rootpage - 1)) * int64(pageSize)
	tablePage := make([]byte, pageSize)
	_, err = databaseFile.ReadAt(tablePage, tablePageOffset)
	if err != nil {
		log.Fatal(err)
	}

	// Count cells in the table page
	// The page header for non-first pages starts at offset 0
	var cellCount uint16
	binary.Read(bytes.NewReader(tablePage[3:5]), binary.BigEndian, &cellCount)

	fmt.Println(cellCount)
}

// findRootpage searches sqlite_schema for the table and returns its rootpage
func findRootpage(page []byte, tableName string) int {
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
			return rootpageValue
		}

		// Reset for next iteration
		cellData = bodyStart
	}

	return 0
}
