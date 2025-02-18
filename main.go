package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type Data struct {
	Date    string
	Open    float64
	High    float64
	Low     float64
	Close   float64
	Volume  float64
	OpenInt float64
}

func getMySQL() *sql.DB {
	db, err := sql.Open("mysql", "root:root@(127.0.0.1:3306)/ETFS_db?parseTime=true")
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func main() {
	db := getMySQL()
	defer db.Close()

	filePath := flag.String("file", "", "CSV file to import")
	tableName := flag.String("table", "ETFs", "Table name to import data")
	flag.Parse()
	
	//Multiple file add at a time...

	// files, err := filepath.Glob(filepath.Join(*filePath, "*.csv"))
	// if err != nil || len(files) == 0 {
	// 	log.Fatal("No CSV files found")
	// }


	//More then one file add at a time...
	
	files := strings.Split(*filePath, ",")
	if len(files) == 0 || files[0] == "" {
		log.Fatal("No CSV files specified")
	}

	db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (Date DATE, Open FLOAT, High FLOAT, Low FLOAT, Close FLOAT, Volume FLOAT, OpenInt FLOAT, FileName VARCHAR(255))`, *tableName))

	for _, file := range files {
		fmt.Printf("Processing file: %s\n", file)
		processCSV(db, file, *tableName)
	}

	startTime := time.Now()
	fmt.Printf("Start Time: %s\n", startTime.Format(time.RFC3339))
	fmt.Println("All Data Inserted Successfully!")
}

func processCSV(db *sql.DB, filePath, tableName string) {
	fileName := filepath.Base(filePath)

	csvFile, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer csvFile.Close()

	df := csv.NewReader(csvFile)
	data, err := df.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	var ETFs []Data

	for i, value := range data {
		if i == 0 {
			continue // Skip header
		}

		open, _ := strconv.ParseFloat(value[1], 64)
		high, _ := strconv.ParseFloat(value[2], 64)
		low, _ := strconv.ParseFloat(value[3], 64)
		closePrice, _ := strconv.ParseFloat(value[4], 64)
		volume, _ := strconv.ParseFloat(value[5], 64)
		openInt, _ := strconv.ParseFloat(value[6], 64)

		ETFs = append(ETFs, Data{
			Date:    value[0],
			Open:    open,
			High:    high,
			Low:     low,
			Close:   closePrice,
			Volume:  volume,
			OpenInt: openInt,
		})
	}

	for _, ETFS := range ETFs {
		db.Exec(fmt.Sprintf(`INSERT INTO %s (Date, Open, High, Low, Close, Volume, OpenInt, FileName) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, tableName),
			ETFS.Date, ETFS.Open, ETFS.High, ETFS.Low, ETFS.Close, ETFS.Volume, ETFS.OpenInt, fileName)
	}

	fmt.Printf("Finished processing: %s\n", filePath)
}


