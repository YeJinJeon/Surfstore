package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	hashes := []string{}
	for serverHash, _ := range c.ServerMap {
		hashes = append(hashes, serverHash)
	}
	sort.Strings(hashes)

	responsibleServer := ""
	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockId {
			responsibleServer = c.ServerMap[hashes[i]]
			break
		}
	}
	if responsibleServer == "" {
		responsibleServer = c.ServerMap[hashes[0]]
	}
	return responsibleServer
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	serverMap := make(map[string]string)
	cHashRing := ConsistentHashRing{}
	for _, serverAddr := range serverAddrs {
		serverName := "blockstore" + serverAddr
		serverHash := cHashRing.Hash(serverName)
		serverMap[serverHash] = serverAddr
	}
	cHashRing.ServerMap = serverMap
	return &cHashRing
}
