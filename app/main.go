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

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

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
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
