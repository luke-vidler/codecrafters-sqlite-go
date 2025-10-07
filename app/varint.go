package main

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
