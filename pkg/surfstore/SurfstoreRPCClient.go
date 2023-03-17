package surfstore

import (
	context "context"
	"errors"
	"fmt"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = success.Flag
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = out.Hashes

	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// connect to the server
	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewRaftSurfstoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	f, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*serverFileInfoMap = f.FileInfoMap

	return conn.Close()
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// connect to the server
	var c RaftSurfstoreClient
	var conn *grpc.ClientConn
	for ind, _ := range surfClient.MetaStoreAddrs {
		// fmt.Println(">>>>>>>>", ind)
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[ind], grpc.WithInsecure())
		defer conn.Close()
		if errors.Is(err, ERR_NOT_LEADER) {
			fmt.Println("Not Leader")
			continue
		} else if errors.Is(err, ERR_SERVER_CRASHED) {
			fmt.Println("Server is crashed")
			continue
		} else if err != nil {
			return err
		}
		c = NewRaftSurfstoreClient(conn)
		break
	}

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	v, err := c.UpdateFile(ctx, fileMetaData)
	if err != nil {
		conn.Close()
		return err
	}
	*latestVersion = v.Version
	return nil
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	bh, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashes = bh.Hashes
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	fmt.Println("**** in Client GetBlockStoreMap!!")
	// connect to the server
	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewRaftSurfstoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	blockHashes := BlockHashes{Hashes: blockHashesIn}
	bmap, err := c.GetBlockStoreMap(ctx, &blockHashes)
	if err != nil {
		conn.Close()
		return err
	}
	bsm := make(map[string][]string)
	for k, blockhashes := range bmap.BlockStoreMap {
		bsm[k] = blockhashes.Hashes
	}
	*blockStoreMap = bsm
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewRaftSurfstoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ba, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockStoreAddrs = ba.BlockStoreAddrs

	return conn.Close()
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
