package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

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
	flag.Parse()
	file, _ := os.Open(*filePath)
	// Create table once before inserting data
	db.Exec(`CREATE TABLE IF NOT EXISTS ETFs (Date DATE,Open FLOAT,High FLOAT,Low FLOAT,Close FLOAT,Volume FLOAT,OpenInt FLOAT
	)`)

	df := csv.NewReader(file)
	data,_ := df.ReadAll()

	var ETFs []Data

	// Read data and convert values
	for i, value := range data {
		if i == 0 {
			continue // Skip header row
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

	// Insert data into MySQL
	for _, ETFS := range ETFs {
		db.Exec(`INSERT INTO ETFs (Date, Open, High, Low, Close, Volume, OpenInt) 
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			ETFS.Date, ETFS.Open, ETFS.High, ETFS.Low, ETFS.Close, ETFS.Volume, ETFS.OpenInt)
	}

	startTime := time.Now()
	fmt.Printf("Start Time: %s\n", startTime.Format(time.RFC3339))
	fmt.Println("Data Inserted Successfully!")
}

