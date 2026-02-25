package ws

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// generateConnID produces a unique connection identifier combining a millisecond
// timestamp and 8 bytes of crypto-random hex for collision resistance.
func generateConnID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("conn_%d_%s", time.Now().UnixMilli(), hex.EncodeToString(b))
}
