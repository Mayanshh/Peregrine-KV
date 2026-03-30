package engine

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// ReplayWAL is called during Engine.Open() to restore data from the Write-Ahead Log.
func (e *Engine) ReplayWAL(walPath string) error {
	f, err := os.Open(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No WAL exists yet, fresh start
		}
		return err
	}
	defer f.Close()

	var recoveredCount int

	for {
		// Read [KeyLen(4)][ValLen(4)]
		header := make([]byte, 8)
		_, err := io.ReadFull(f, header)
		if err == io.EOF {
			break // Successfully reached end of WAL
		}
		if err != nil {
			return fmt.Errorf("corrupted WAL header: %v", err)
		}

		keyLen := binary.LittleEndian.Uint32(header[0:4])
		valLen := binary.LittleEndian.Uint32(header[4:8])

		// Read [Key][Val]
		data := make([]byte, keyLen+valLen)
		_, err = io.ReadFull(f, data)
		if err != nil {
			return fmt.Errorf("corrupted WAL data: %v", err)
		}

		key := data[:keyLen]
		val := data[keyLen:]

		// Restore to MemTable silently (bypassing the WAL write since it's already in the WAL)
		e.memtable.Put(key, val)
		recoveredCount++
	}

	if recoveredCount > 0 {
		fmt.Printf("INFO: Recovered %d entries from WAL.\n", recoveredCount)
	}
	return nil
}