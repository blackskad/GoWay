/**
 * Database schema migration framework.
 *
 * TODO: in the long run, open source this.
 *
 * Properties:
 *  - no "down" migrations, only "up"
 *  - plain text files containing sql statements
 *  - located in ${GOPATH}/setup/migrations
 *  - filename format: "v([1-9][0-9]?)-([a-zA-Z]*)\.sql"
 * 
 * Migrations are applied in the increasing order of version in the filename. So
 * in the example below, first the v1-InitialSchema migration is applied, then
 * the v2-AddSchemaTable migration.
 * 
 * Example:
 *    v1-InitialSchema.sql
 *    v2-AddSchemaTable.sql
 */
package goway

import (
	"log"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var regex, _ = regexp.Compile("^v([1-9][0-9]?)-([a-zA-Z]+).sql$")

type SchemaVersion struct {
	Number  int
	Name    string
	Applied time.Time
}

type Migration struct {
	File        os.FileInfo
	Number      int
	Description string
}

type Migrations struct {
	m []Migration
}

func (s Migrations) Less(i, j int) bool {
	return s.m[i].Number < s.m[j].Number
}

func (s Migrations) Len() int {
	return len(s.m)
}

func (s Migrations) Swap(i, j int) {
	s.m[i], s.m[j] = s.m[j], s.m[i]
}

func getCurrentVersion(db *sql.DB) (schema *SchemaVersion, err error) {
	rows, err := db.Query("SELECT version, name, applied FROM db_versions ORDER BY version DESC LIMIT 1")

	// oh, I wish there was a better way to check this...
	if err != nil {
		if strings.Contains(err.Error(), "1146") {
			_, err = db.Exec("CREATE TABLE db_versions (version INTEGER PRIMARY KEY NOT NULL, name VARCHAR(50) NOT NULL, applied DATETIME)")
			if err != nil {
				return nil, err
			}
			return &SchemaVersion{0, "-", time.Time{}}, nil
		} else {
			return nil, err
		}
	}
	// don't forget to close the rows
	defer rows.Close()

	// try to get the schema version
	// if the table didn't exist yet, create it
	schema = &SchemaVersion{}
	rows.Next()
	err = rows.Scan(&schema.Number, &schema.Name, &schema.Applied)
	if err != nil {
		return nil, err
	}
	return schema, nil
}

func getAvailableMigrations(location string) ([]Migration, error) {
	files, err := ioutil.ReadDir(location)
	if err != nil {
		return nil, err
	}

	migrations := make([]Migration, len(files))
	i := 0
	for _, file := range files {
		matches := regex.FindAllStringSubmatch(file.Name(), -1)
		if matches != nil && len(matches) == 1 {
			version, _ := strconv.Atoi(matches[0][1])
			migrations[i] = Migration{file, version, matches[0][2]}
			i = i + 1
			//} else {
			//	println(fmt.Sprintf("Ignoring migration file %s. Filename didn't match pattern!", files[idx].Name()))
		}
	}
	sortable := Migrations{migrations[:i]}
	sort.Sort(sortable)
	return sortable.m, nil
}

func (migration Migration) apply(db *sql.DB, location string) error {
	println(fmt.Sprintf("Applying migration %d - '%s'", migration.Number, migration.Description))

	content, _ := ioutil.ReadFile(fmt.Sprintf("%s/%s", location, migration.File.Name()))
	if len(content) == 0 {
		return errors.New("Empty migrations file")
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	statements := strings.Split(string(content), ";")
	for _, statement := range statements {
		statement = strings.TrimSpace(statement)
		if len(statement) > 0 {
			_, err = tx.Exec(statement)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
	}
	_, err = tx.Exec("INSERT INTO db_versions (version, name, applied) VALUES (?, ?, NOW())", migration.Number, migration.Description)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func Upgrade(db *sql.DB, location string) {
	println("Performing db schema upgrade...")

	// Get a list of available migrations.
	migrations, err := getAvailableMigrations(location)
	if err != nil {
		log.Fatalf("Failed to get a list of available database schema migrations: %s", err.Error())
	}

	// Get the current version in the db.
	current, err := getCurrentVersion(db)
	if err != nil {
		log.Fatalf("Failed to get the current database schema version: %s", err)
	}
	println(fmt.Sprintf("Current db version: %d, applied %s", current.Number, current.Applied))

	// Apply the pending migrations one by one in statements
	for _, migration := range migrations {
		if migration.Number > current.Number {
			err = migration.apply(db, location)
			if err != nil {
				panic(fmt.Sprintf("Failed to update the database schema to version %d: %s", migration.Number, err))
			}
		}
	}

	// Get the current version in the db.
	current, err = getCurrentVersion(db)
	if err != nil {
		log.Fatalf("Failed to get the current database schema version: %s", err)
	}
	println(fmt.Sprintf("Updated db version: %d, applied %s", current.Number, current.Applied))
}
