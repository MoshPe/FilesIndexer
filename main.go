package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/blevesearch/bleve"
	_ "github.com/blevesearch/bleve/config"
	"github.com/blevesearch/bleve/search/highlight/format/ansi"
	"log"
	"os"
	"path/filepath"
)

const (
	batchSize  = 1000      // Number of lines to process in each batch
	bufferSize = 64 * 1024 // Buffer size for reading lines from file
)

func indexFolder(index bleve.Index, folderPath string) error {
	err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			fmt.Println("Indexing File:", path)
			if err := indexFile(index, path); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func indexFile(index bleve.Index, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("Error closing file: %v", err)
		}
	}(file)

	reader := bufio.NewReaderSize(file, bufferSize)
	var lines []string
	lineNumber := 1
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		lines = append(lines, line)
		if len(lines) >= batchSize {
			if err := indexLines(index, filePath, lineNumber-len(lines)+1, lines); err != nil {
				return err
			}
			lines = nil
		}
		lineNumber++
	}

	if len(lines) > 0 {
		if err := indexLines(index, filePath, lineNumber-len(lines)+1, lines); err != nil {
			return err
		}
	}

	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func indexLines(index bleve.Index, filePath string, startLineNumber int, lines []string) error {
	batch := index.NewBatch()
	for i, line := range lines {
		docID := fmt.Sprintf("%s@%d", filePath, startLineNumber+i)
		doc := map[string]interface{}{
			"line":    startLineNumber + i,
			"content": line,
		}
		if err := batch.Index(docID, doc); err != nil {
			log.Printf("Error indexing line %d in file %s: %s\n", startLineNumber+i, filePath, err)
		}
	}

	err := index.Batch(batch)
	if err != nil {
		log.Fatalln(err)
	}
	return nil
}

func searchFiles(index bleve.Index, query string) (*bleve.SearchResult, error) {
	//searchQuery := bleve.NewWildcardQuery(query)
	searchQuery := bleve.NewMatchQuery(query)
	searchRequest := bleve.NewSearchRequestOptions(searchQuery, 20, 0, true)
	searchRequest.Highlight = bleve.NewHighlightWithStyle(ansi.Name)
	searchResults, err := index.Search(searchRequest)
	if err != nil {
		return nil, err
	}
	fmt.Println(searchResults)
	return searchResults, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: your_app_name folder_path")
		return
	}

	folderPath := os.Args[1]

	// Ensure the provided path is absolute
	absFolderPath, err := filepath.Abs(folderPath)
	if err != nil {
		fmt.Println("Error! not an absolute folder:", err)
		return
	}

	// Check if the provided path exists
	_, err = os.Stat(absFolderPath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("Folder does not exist:", absFolderPath)
		} else {
			fmt.Println("Error:", err)
		}
		return
	}

	// Open or create a new index
	indexMapping := bleve.NewIndexMapping()
	indexMapping.DefaultMapping.AddFieldMappingsAt("content", bleve.NewTextFieldMapping())
	//index, err := bleve.Open("example.bleve")
	//if errors.Is(err, bleve.ErrorIndexPathDoesNotExist) {
	//	index, err = bleve.New("example.bleve", indexMapping)
	//}
	index, err := bleve.NewMemOnly(indexMapping)
	if err != nil {
		log.Fatalf("Error creating/opening index: %s", err)
	}
	defer func(index bleve.Index) {
		err := index.Close()
		if err != nil {
			log.Fatalf("Error closing index: %s", err)
		}
	}(index)

	// Index files
	fmt.Println("Started indexing...")
	fmt.Println("Indexing folder:", folderPath)
	if err := indexFolder(index, folderPath); err != nil {
		log.Fatalf("Error indexing folder %s: %s", folderPath, err)
	}
	fmt.Println("Finished indexing")

	// Perform search
	//38941
	query := "I come to woo ladies" // Replace with your search query
	_, err = searchFiles(index, query)
	if err != nil {
		log.Fatalf("Error searching index: %s", err)
	}
}
