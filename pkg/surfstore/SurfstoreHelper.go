package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `INSERT into indexes(
		fileName,
		version,
		hashIndex,
		hashValue
	) values(?, ?, ?, ?)`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Println("Error During Meta Write Back: ", err)
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Println("Error During Meta Write Back")
	}
	statement.Exec()

	statement2, err := db.Prepare(insertTuple)
	if err != nil {
		log.Println("Error During Meta Write Back")
	}

	for _, item := range fileMetas {
		for hashind, hash := range item.BlockHashList {
			_, err2 := statement2.Exec(item.Filename, item.Version, hashind, hash)
			if err2 != nil {
				log.Println("Error During Inserting Meta Data")
			}
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `SELECT DISTINCT filename FROM indexes`

const getTuplesByFileName string = `SELECT * FROM indexes WHERE filename = ? ORDER BY hashIndex`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Println("Error When Opening Meta")
	}

	// get distinct filenames
	rows, err := db.Query(getDistinctFileName)
	if err != nil {
		log.Println("Error during getting distinct filenames from index.db")
	}
	if rows == nil {
		log.Println("local db is empty!")
		return fileMetaMap, nil
	}
	fns := make([]string, 0)
	for rows.Next() {
		var f string
		err := rows.Scan(&f)
		if err != nil {
			log.Println("Error during getting distinct filenames")
		}
		fns = append(fns, f)
	}

	// get tuples by filename and generate FileMetaData
	for _, f := range fns {
		stm, err := db.Prepare(getTuplesByFileName)
		if err != nil {
			log.Println("Error During Meta Write Back")
		}
		var version int
		var filename string
		var hashindex int
		var hash_list []string
		rows, err = stm.Query(f)
		if err != nil {
			log.Println("Error during getting distinct filenames from index.db")
		}
		for rows.Next() {
			var hashvalue string
			err := rows.Scan(&filename, &version, &hashindex, &hashvalue)
			if err != nil {
				log.Fatal(err)
			}
			hash_list = append(hash_list, hashvalue)
		}
		fmd := &FileMetaData{
			Filename:      filename,
			Version:       int32(version),
			BlockHashList: hash_list,
		}
		fileMetaMap[f] = fmd
	}
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	//fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		fmt.Println("\t", len(filemeta.BlockHashList))
		if len(filemeta.BlockHashList) == 1 {
			for _, blockHash := range filemeta.BlockHashList {
				fmt.Println("\t", blockHash)
			}
		}
	}

	//fmt.Println("---------END PRINT MAP--------")

}
