package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
)

const (
	PageTypeInteriorTable = 0x05
	PageTypeLeafTable     = 0x0d
)

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

	if pageType == PageTypeLeafTable {
		// Leaf page - return cell count
		return int(cellCount)
	} else if pageType == PageTypeInteriorTable {
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

// RowProcessor is a function that processes a single row
type RowProcessor func(rowid uint64, columnValues []string) bool

// traverseBTree traverses the B-tree and calls the processor for each row
// The processor returns true to continue, false to stop
func traverseBTree(file *os.File, pageSize int64, pageNum int, processor RowProcessor) {
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

	if pageType == PageTypeLeafTable {
		// Leaf page - process all cells
		cellPointerOffset := headerOffset + 8 // Leaf page header is 8 bytes
		for i := 0; i < int(cellCount); i++ {
			var cellPointer uint16
			offset := cellPointerOffset + i*2
			binary.Read(bytes.NewReader(page[offset:offset+2]), binary.BigEndian, &cellPointer)

			// Parse the record
			rowid, columnValues, err := parseRecord(page[cellPointer:])
			if err == nil {
				if !processor(rowid, columnValues) {
					return // Stop processing
				}
			}
		}
	} else if pageType == PageTypeInteriorTable {
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
			traverseBTree(file, pageSize, int(leftChildPointer), processor)
		}

		// Traverse rightmost child
		traverseBTree(file, pageSize, int(rightmostPointer), processor)
	}
}

// selectRows selects rows matching the given criteria
func selectRows(file *os.File, pageSize int64, rootpage int, columnIndices []int, columnNames []string, createTableSQL string, hasWhere bool, whereColumnIndex int, whereValue string) {
	// Check which columns are INTEGER PRIMARY KEY
	isPKColumn := make([]bool, len(columnNames))
	for i, colName := range columnNames {
		isPKColumn[i] = isIntegerPrimaryKey(createTableSQL, colName)
	}

	processor := func(rowid uint64, allColumnValues []string) bool {
		// Check WHERE condition if present
		if hasWhere {
			if allColumnValues[whereColumnIndex] != whereValue {
				return true // Continue to next row
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
		return true // Continue processing
	}

	traverseBTree(file, pageSize, rootpage, processor)
}
