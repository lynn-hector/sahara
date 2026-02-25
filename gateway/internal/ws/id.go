package ws

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

func generateConnID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("conn_%d_%s", time.Now().UnixMilli(), hex.EncodeToString(b))
}
