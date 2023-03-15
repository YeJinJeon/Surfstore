package surfstore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
)

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// fmt.Println(">>>>>>>>>>>>>>> Client Syncing >>>>>>>>>>>>>>>")
	base_dir := client.BaseDir
	idx_file := base_dir + "/index.db"
	if _, err := os.Stat(idx_file); errors.Is(err, os.ErrNotExist) {
		indexFile, _ := os.Create(idx_file)
		indexFile.Close()
	}
	// fmt.Println("============= Before Sync local directory ===========")
	local_metaData, err := LoadMetaFromMetaFile(base_dir)
	if err != nil {
		log.Println("Error during loading local meta data", err)
	}
	// PrintMetaMap(local_metaData)

	//Sync local
	// fmt.Println("====================== Sync Local =======================")
	curr_files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Println("Error during reading basedir: ", err)
	}
	// generate hash map of current local base directory
	local_hash := make(map[string][]string) //filename: hash_list
	for _, curr_f := range curr_files {
		if curr_f.Name() == "index.db" {
			continue
		}
		fmt.Print("Working on ", curr_f.Name(), " ")

		// open file
		fi, err := os.Open(filepath.Join(base_dir, curr_f.Name()))
		if err != nil {
			log.Println("Error during opening local file in sync local")
		}
		defer fi.Close()
		fis, err := fi.Stat()
		if err != nil {
			log.Fatal(err)
		}

		var numBlocks int = int(math.Ceil(float64(fis.Size()) / float64(client.BlockSize)))
		for i := 0; i < numBlocks; i++ {
			buf := make([]byte, client.BlockSize)
			n, err := fi.Read(buf) // read up to len(buf) bytes, maybe lesser
			if err != nil && err != io.EOF {
				log.Println(err)
			}
			if err == io.EOF {
				break
			}
			hash := GetBlockHashString(buf[:n])
			local_hash[curr_f.Name()] = append(local_hash[curr_f.Name()], hash)
		}

		meta_hash, ok := local_metaData[curr_f.Name()]
		if ok {
			if !stringSlicesEqual(meta_hash.BlockHashList, local_hash[curr_f.Name()]) { //update FileMetaData
				local_metaData[curr_f.Name()].Version += 1
				local_metaData[curr_f.Name()].BlockHashList = local_hash[curr_f.Name()]
				fmt.Println("Update existing file on local db")
			} else {
				fmt.Println("Same with a existing file on local db")
			}
		} else { // new file
			fmd := FileMetaData{
				Filename:      curr_f.Name(),
				Version:       1,
				BlockHashList: local_hash[curr_f.Name()],
			}
			local_metaData[curr_f.Name()] = &fmd
			fmt.Println("Create new file on local db")
		}
	}

	// detect files deleted on local
	for metafile, metaData := range local_metaData { //before sync
		if _, ok := local_hash[metafile]; !ok { //current
			if len(metaData.BlockHashList) != 1 || metaData.BlockHashList[0] != "0" {
				fmt.Printf("Working on %s Update deleted file on local db\n", metafile)
				deleted_hash := []string{"0"}
				local_metaData[metafile].Version += 1
				local_metaData[metafile].BlockHashList = deleted_hash
			}
		}
	}

	//update local index.db
	WriteMetaFile(local_metaData, client.BaseDir)
	// fmt.Println("================= After Sync local directory =================")
	//local_metaData2, err := LoadMetaFromMetaFile(base_dir)
	// if err != nil {
	// 	log.Println("Error during loading local meta data", err)
	// }
	// PrintMetaMap(local_metaData2)

	// get server file info
	server_index := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&server_index); err != nil {
		log.Println("Error during reading server index")
	}

	// get blockstore address
	var blockStoreAddrs []string
	if err := client.GetBlockStoreAddrs(&blockStoreAddrs); err != nil {
		log.Println("Could not get blockStoreAddr: ", err)
	}

	// check if server has updated or new file
	// fmt.Println("================= Sync with Remote directory =================")
	flag := false
	for sf, sfmd := range server_index {
		if lfmd, ok := local_metaData[sf]; ok {
			if sfmd.Version > lfmd.Version { // server has updated file
				flag = true
				// update local meta data
				local_metaData[sf].BlockHashList = sfmd.BlockHashList
				local_metaData[sf].Version += 1
				// download data from server
				blockStoreMap := make(map[string][]string)
				client.GetBlockStoreMap(sfmd.BlockHashList, &blockStoreMap) // get block store server of blockhashlist
				download_block(client, base_dir, sf, sfmd, blockStoreMap)
			} else if sfmd.Version == lfmd.Version && !stringSlicesEqual(local_metaData[sf].BlockHashList, sfmd.BlockHashList) { //conflict!
				flag = true
				// overwrite server meta data to local meta data
				local_metaData[sf].BlockHashList = sfmd.BlockHashList
				// download data from server
				blockStoreMap := make(map[string][]string)
				client.GetBlockStoreMap(sfmd.BlockHashList, &blockStoreMap) // get block store server of blockhashlist
				download_block(client, base_dir, sf, sfmd, blockStoreMap)
			}
		} else { //server has new file
			flag = true
			// update local meta data
			local_metaData[sf] = sfmd
			// download data from server
			blockStoreMap := make(map[string][]string)
			client.GetBlockStoreMap(sfmd.BlockHashList, &blockStoreMap) // get block store server of blockhashlist
			download_block(client, base_dir, sf, sfmd, blockStoreMap)
		}
	}
	//update local index.db
	if flag {
		WriteMetaFile(local_metaData, client.BaseDir)
		// local_metaData3, err := LoadMetaFromMetaFile(base_dir)
		// if err != nil {
		// 	log.Println("Error during loading local meta data", err)
		// }
		// PrintMetaMap(local_metaData3)
	} else {
		// fmt.Println("No updates on Remote directory")
	}

	// get consistent hash ring
	hashRing := NewConsistentHashRing(blockStoreAddrs)

	// check if local has updated or new file
	// fmt.Println("================= Sync local changes to Remote directory =================")
	for lf, lfmd := range local_metaData {
		if _, ok := server_index[lf]; ok {
			if lfmd.Version > server_index[lf].Version {
				// update server meta data
				var latestVersion int32
				err := client.UpdateFile(lfmd, &latestVersion) // update version and hashlist
				if err != nil {
					log.Println("error during updating meta data on server")
				}
				// put blocks to server
				upload_block(client, base_dir, lf, hashRing)
			}
		} else { //local has new file
			// update server meta data
			var latestVersion int32
			err := client.UpdateFile(lfmd, &latestVersion)
			if err != nil {
				log.Println("error during updating meta data on server")
			}
			// put blocks to server
			upload_block(client, base_dir, lf, hashRing)
		}
	}
}

func upload_block(client RPCClient, localdir string, localfile string, consistenthashRing *ConsistentHashRing) error {

	if _, err := os.Stat(filepath.Join(localdir, localfile)); errors.Is(err, os.ErrNotExist) {
		// fmt.Println("No file to upload!")
		return err
	}

	// fmt.Printf("uploading %s to server\n", localfile)
	f, err := os.Open(filepath.Join(localdir, localfile))
	if err != nil {
		log.Println("Error during uploading to server(opening): ", err)
	}
	defer f.Close()
	fs, err := f.Stat()
	if err != nil {
		log.Println("Error during uploading to server(checking status): ", err)
	}

	var numBlocks int = int(math.Ceil(float64(fs.Size()) / float64(client.BlockSize)))
	for i := 0; i < numBlocks; i++ {
		buf := make([]byte, client.BlockSize)
		n, err := f.Read(buf) // read up to len(buf) bytes, maybe lesser
		if err != nil && err != io.EOF {
			log.Println(err)
		}
		if err == io.EOF {
			break
		}
		block := Block{BlockData: buf[:n], BlockSize: int32(client.BlockSize)}
		blockHash := GetBlockHashString(block.BlockData)
		var success bool
		blockStoreAddr := consistenthashRing.GetResponsibleServer(blockHash)
		if err := client.PutBlock(&block, blockStoreAddr, &success); err != nil {
			log.Println("Error during putting block to server: ", err)
		}
	}
	return nil
}

func download_block(client RPCClient, localdir string, remotefile string, remotefileMetadata *FileMetaData, blockStoreMap map[string][]string) error {
	// fmt.Printf("Downloading %s from server\n", remotefile)
	f, err := os.Create(filepath.Join(localdir, remotefile))
	if err != nil {
		log.Println("Error creating file: ", err)
	}
	defer f.Close()

	// delete deleted file
	if len(remotefileMetadata.BlockHashList) == 1 && remotefileMetadata.BlockHashList[0] == "0" {
		if err := os.Remove(filepath.Join(localdir, remotefile)); err != nil {
			log.Println("Could not remove local file: ", err)
			return err
		}
	}

	writer := bufio.NewWriter(f)
	var data []byte

	for _, hash := range remotefileMetadata.BlockHashList {
		var block_data Block
		blockStoreAddr := check(hash, blockStoreMap)
		if err := client.GetBlock(hash, blockStoreAddr, &block_data); err != nil {
			log.Println("Error during getting block from server: ", err)
		}
		data = append(data, block_data.BlockData...)
	}

	_, err = writer.Write(data)
	if err != nil {
		log.Println("Error during writing data to local base dir: ", err)
	}
	writer.Flush() // flush the buffer to the fil

	return nil
}

func check(blockhash string, blockStoreMap map[string][]string) (serverAddr string) {
	for server, hashlist := range blockStoreMap {
		for _, hash := range hashlist {
			if hash == blockhash {
				return server
			}
		}
	}
	return
}
