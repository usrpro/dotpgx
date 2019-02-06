package dotpgx

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type queryMap map[string]string

// Merge one or more queryMaps into the current.
// Any tags that are declared multiple times get overwritten
// and set to the last occurence.
func merge(maps ...queryMap) (qm queryMap) {
	qm = make(queryMap)
	for _, m := range maps {
		for k, v := range m {
			qm[k] = v
		}
	}
	return
}

// ParseSql parses and stores SQL queries from a io.Reader.
// Queries should end with a semi-colon.
// It stores queries by their "--name: <name>" tag.
// If no name tag is specified, an incremental number will be appointed.
// This might come in handy for sequential execution (like migrations).ParseSql
// Parsed queries get appended to the current map.
// If a name tag was already present, it will get overwritten by the new one parsed.
// The serial value is stored inside the DB object,
// so it is safe to call this function multiple times
func (db *DB) ParseSql(r io.Reader) error {
	sc := bufio.NewScanner(r)
	comment := false
	var tag string
	qm := make(queryMap)
	for sc.Scan() {
		// Read the line
		line := sc.Text()
		if err := sc.Err(); err != nil {
			return err
		}
		// Sanetize leading and trailing whitespace
		line = strings.TrimSpace(line)
		// Line with name tag?
		if strings.HasPrefix(line, "-- name:") {
			tag = strings.TrimSpace(strings.TrimPrefix(line, "-- name:"))
			// Initialise to empty query body, overwites any previous query with the same name
			qm[tag] = ""
			continue
		}
		// Skip empty and comment lines
		if len(line) == 0 || strings.HasPrefix(line, "--") {
			continue
		}
		// Still inside comment block?
		if comment {
			if strings.HasSuffix(line, "*/") {
				comment = false
			}
			continue
		}
		// Beginning of comment block?
		if strings.HasPrefix(line, "/*") {
			comment = true
			continue
		}
		// Not in comment block and no tag set?
		if len(tag) == 0 {
			// Default to an auto-incremented tag number.
			tag = fmt.Sprintf("%06d", db.qn)
			db.qn++
		}
		// Inside of query body?
		if len(tag) > 0 {
			// Cut away inline comments
			q := strings.TrimSpace(strings.Split(line, "--")[0])
			if len(qm[tag]) == 0 {
				qm[tag] = q
			} else {
				// Join with the existing body
				j := []string{qm[tag], q}
				qm[tag] = strings.Join(j, " ")
			}

			// End of query body reached?
			if strings.HasSuffix(line, ";") {
				tag = ""
			}
			continue
		}
	}
	db.qm = merge(db.qm, qm)
	return nil
}

// ParseFiles opens one or more files and feeds them to ParseSql
func (db *DB) ParseFiles(files ...string) error {
	for _, f := range files {
		f, err := os.Open(f)
		if err != nil {
			return err
		}
		err = db.ParseSql(f)
		f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// ParseFileGlob passes all files that match glob to ParseFiles.
// Subsequently those files get fed into DB.ParseSql.
// See filepath.glob for behavior.
func (db *DB) ParseFileGlob(glob string) error {
	files, err := filepath.Glob(glob)
	if err != nil {
		return err
	}
	return db.ParseFiles(files...)
}

// ParsePath is a convenience wrapper.
// It uses ParseFileGlob to load all files in path, with a .sql suffix.
func (db *DB) ParsePath(path string) error {
	s := []string{
		path,
		"*.sql",
	}
	return db.ParseFileGlob(strings.Join(s, "/"))
}
