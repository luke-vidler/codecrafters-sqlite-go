package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

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

// parseRecord parses a record from cell data and returns all column values
func parseRecord(cellData []byte) (rowid uint64, columnValues []string, err error) {
	// Read record size (varint)
	_, bytesRead := readVarint(cellData)
	cellData = cellData[bytesRead:]

	// Read rowid (varint)
	rowid, bytesRead = readVarint(cellData)
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
	columnValues = make([]string, len(serialTypes))
	offset := 0
	for i, serialType := range serialTypes {
		colSize := getSerialTypeSize(serialType)
		columnValues[i] = readColumnValue(cellData[offset:offset+colSize], serialType)
		offset += colSize
	}

	return rowid, columnValues, nil
}
