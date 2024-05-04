package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq" // Use the PostgreSQL driver
)

type DataBaseConnection struct {
	Host   string
	Port   string
	User   string
	Pass   string
	DBName string
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	db, err := databaseConnection(getDataBaseConnection())
	if err != nil {
		log.Fatal("Error connecting to the database: ", err)
	}
	defer db.Close()

	fmt.Println("Successfully connected to the database!")
}
func getDataBaseConnection() DataBaseConnection {
	return DataBaseConnection{
		Host:   os.Getenv("DB_HOST"),
		Port:   os.Getenv("DB_PORT"),
		User:   os.Getenv("DB_USER"),
		Pass:   os.Getenv("DB_PASS"),
		DBName: os.Getenv("DB_NAME"),
	}
}
func databaseConnection(dbv DataBaseConnection) (*sql.DB, error) {
	// dbv := DataBaseConnection{
	// 	Host:   os.Getenv("DB_HOST"),
	// 	Port:   os.Getenv("DB_PORT"),
	// 	User:   os.Getenv("DB_USER"),
	// 	Pass:   os.Getenv("DB_PASS"),
	// 	DBName: os.Getenv("DB_NAME"),
	// }
	connStr := fmt.Sprintf("user=%s dbname=%s password=%s host=%s port=%s sslmode=disable", dbv.User, dbv.DBName, dbv.Pass, dbv.Host, dbv.Port)
	fmt.Println("Connection string:", connStr) // Debugging purpose: It's good to verify that your connection string is formatted correctly

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	// Optional: Check the database connection
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}
