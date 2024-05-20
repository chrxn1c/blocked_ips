package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	_ "github.com/lib/pq"
	"golang.org/x/text/encoding/charmap"
	"log"
	"os"
	"strconv"
	"strings"
	"github.com/redis/go-redis/v9"
)

func createPostgresDB(db *sql.DB) {
	createDatabaseQuery := "SELECT 'CREATE DATABASE blocked_ips ENCODING UTF-8' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'blocked_ips');"
	_, err := db.Exec(createDatabaseQuery)
	if err != nil {
		print("error while creating database")
	}

	createDomainNameQuery := "CREATE TABLE IF NOT EXISTS domains (" +
		"id SERIAL PRIMARY KEY, " +
		"domain_name VARCHAR(255));"
	_, err = db.Exec(createDomainNameQuery)
	if err != nil {
		panic("error while creating domain name table")
	}

	createNullableDomainNameQuery := "INSERT INTO domains (id, domain_name) VALUES (DEFAULT, 'NULLABLE_DOMAIN');"
	_, err = db.Exec(createNullableDomainNameQuery)
	if err != nil {
		// panic("error while creating nullable domain")
	}

	createIPsTableQuery := "CREATE TABLE IF NOT EXISTS ips (" +
		"id SERIAL PRIMARY KEY, " +
		"ip_address TEXT, " +
		"domain_name_id INTEGER, " +
		"CONSTRAINT fk_domain" + " FOREIGN KEY(domain_name_id) REFERENCES domains(id)" +
		");"
	_, err = db.Exec(createIPsTableQuery)
	if err != nil {
		panic("error while creating ips table")
	}

	createURLQuery := "CREATE TABLE IF NOT EXISTS urls (" +
		"id SERIAL PRIMARY KEY, " +
		"url TEXT, " +
		"domain_name_id INTEGER, " +
		"CONSTRAINT fk_domain_name" + " FOREIGN KEY(domain_name_id) REFERENCES domains(id)" +
		");"
	_, err = db.Exec(createURLQuery)
	if err != nil {
		panic("error while creating urls table")
	}
	//
	//createIPtoDomainLinkTableQuery := "CREATE TABLE IF NOT EXISTS ip_to_domain (" +
	//	"ip_address inet REFERENCES ips (ip_address) ON UPDATE CASCADE ON DELETE CASCADE, " +
	//	"domain_name VARCHAR(255) REFERENCES domains (domain_name) ON UPDATE CASCADE ON DELETE CASCADE, " +
	//	"CONSTRAINT pk_ip PRIMARY KEY (ip_address, domain_name) " +
	//	");"
	//
	//_, err = db.Exec(createLinkTableQuery)
	//if err != nil {
	//	return
	//}
	//
	//createURLtoDomainLinkTableQuery := "CREATE TABLE IF NOT EXISTS url_to_domain" +
	//	""

}

func parseStringAndGetIPs(row *string) []string {
	// println(*row)
	separator := strings.Index(*row, ";")
	return strings.Split((*row)[:separator], "|")
}

func parseStringAndGetDomain(row *string) string {
	beginningSeparator := strings.Index(*row, ";")
	secondSeparator := strings.Index((*row)[beginningSeparator+1:], ";") + 1 + beginningSeparator
	return (*row)[beginningSeparator+1 : secondSeparator]
}

func parseStringsAndGetURLs(row *string) []string {
	beginningSeparator := strings.Index(*row, ";")
	secondSeparator := strings.Index((*row)[beginningSeparator+1:], ";") + 1 + beginningSeparator

	if secondSeparator == len(*row)-1 {
		return []string{}
	}

	if (*row)[secondSeparator+1] == '"' {
		return []string{(*row)[secondSeparator+1 : len(*row)-1]}
	}

	return strings.Split((*row)[secondSeparator+1:], " | ")
}

func parseStringsAndGetURLsAgain(row *string) []string {
	panic("")
}

func readCsvFile(filePath string) [][]string {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Unable to read input file "+filePath, err)
	}
	defer f.Close()
	csvReader := csv.NewReader(charmap.ISO8859_15.NewDecoder().Reader(f))
	// csvReader := csv.NewReader(f)
	csvReader.FieldsPerRecord = -1
	csvReader.LazyQuotes = true
	csvReader.Comma = '\t'
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal("Unable to parse file as CSV for "+filePath, err)
	}

	return records
}

type Entity struct {
	ipAddresses []string
	domainName  string
	URLs        []string
}

func fillNewRecordInDB(db *sql.DB, entity *Entity) {
	// fill new domain name
	updateDomainTable := "INSERT INTO domains (id, domain_name) VALUES (DEFAULT, " + "'" + (*entity).domainName + "'" + ") RETURNING id;"

	var domainNameID int64 = 1
	if (*entity).domainName != "" {
		err := db.QueryRow(updateDomainTable).Scan(&domainNameID)
		if err != nil {
			panic("error while updating domain table")
		}

		if err != nil {
			panic("error while getting id of last executed query regarding domain table")
		}
	}

	// fill ip addresses for this domain name
	for _, ipAddress := range (*entity).ipAddresses {
		updateIPAddressTable := "INSERT INTO ips (ip_address, domain_name_id) VALUES (" + "'" + ipAddress + "'" + "," + strconv.FormatInt(domainNameID, 10) + ");"

		_, err := db.Exec(updateIPAddressTable)
		if err != nil {
			panic("error while updating ip address table")
		}
	}

	// fill URLs for this domain name

	if len((*entity).URLs) > 0 {
		for _, URL := range (*entity).URLs {
			updateURLTable := "INSERT INTO urls VALUES (DEFAULT, " + "'" + URL + "'" + ", " + strconv.FormatInt(domainNameID, 10) + ");"

			_, err := db.Exec(updateURLTable)
			if err != nil {
				print("error while updating urls table")
			}
		}
	}

}

const (
	postgresHost     = "postgres"
	postgresPort     = 5432
	postgresUser     = "postgres"
	postgresPassword = "admin"
	postgresDbname   = "blocked_ips"
)

func fillNewRecordInRedis(redisDB *Client, entity *Entity, ctx *Context ) {
	for _, ip_address := range (*entity).ipAddresses {
		err := redisDB.Set(ctx, ip_address, "true", 0).Err()
		if err != nil {
			panic(err)
		}
	}
}

func main() {
	// create psql db and init connection
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		postgresHost, postgresPort, postgresUser, postgresPassword, postgresDbname)

	postgres, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}

	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			fmt.Println("Error occured while closing connection with database container")
		}
	}(postgres)
	createPostgresDB(postgres)

	// create redis db and init connection
	ctx := context.Background()
	redisDB := redis.NewClient(&redis.Options{
		Addr:	  "redis:6379",
		Password: "", // no password set
		DB:		  0,  // use default DB
	})


	records := readCsvFile("dump.csv")

	var IPs []string
	var domainName string
	var URLs []string

	for _, value := range records[1:] {
		for _, IPsAndDomain := range value {
			IPs = parseStringAndGetIPs(&IPsAndDomain)
			domainName = parseStringAndGetDomain(&IPsAndDomain)
			URLs = parseStringsAndGetURLs(&IPsAndDomain)

			recordInDB := Entity{IPs, domainName, URLs}
			fillNewRecordInRedis(redisDB, &recordInDB)
			fillNewRecordInDB(postgres, &recordInDB)
		}
	}

	print("Script is over")
}
