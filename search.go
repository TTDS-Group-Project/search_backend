package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"

	"github.com/a2800276/porter"
	_ "github.com/a2800276/porter"
	_ "github.com/golang-collections/collections"
	"github.com/golang-collections/collections/set"
	_ "github.com/lib/pq"
)

// pointer to DB
var db *sql.DB

func main() {
	initDB()

	posting := getPosting("apple")
	set := createSetFromPosting(posting)

	posting1 := getPosting("cat")
	set1 := createSetFromPosting(posting1)

	result := ANDhelper(set, set1)

	result = PhraseSearch(posting, posting1)

	result = ProxitmitySearch(posting, posting1, 3)

	fmt.Println(result)

	list := make([]*map[string][]int, 0)

	list = append(list, posting)
	list = append(list, posting1)
	list = append(list, posting1)

	rank := RankedSearch(&list)

	fmt.Println(rank)

}

// Connect to the PostgreSQL database
func initDB() {
	connStr := "host=localhost port=5432 user=postgres password=* dbname=ttds sslmode=disable"
	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
}

// remove stop words and tokenize free text search
func preProcessFreeTextSearch(search string) []string {
	stopWords := set.New()
	filtered := []string{}
	for _, term := range strings.Fields(search) {
		processed_term := preProcessTerm(term)
		if !stopWords.Has(processed_term) {
			filtered = append(filtered, processed_term)
		}
	}

	return filtered
}

// process term and tokenise
func preProcessTerm(term string) string {
	nonAlphanumericRegex := regexp.MustCompile(`[^a-zA-Z0-9 ]+`)
	term = nonAlphanumericRegex.ReplaceAllString(term, "")
	term = strings.ToLower(term)
	term = porter.Stem(term)
	return term

}

// get posting from inverted index on DB, return as a map
func getPosting(term string) *map[string][]int {
	row := db.QueryRow("SELECT postings FROM inverted_index WHERE term = $1", term)

	var posting_JSON []byte
	posting := make(map[string][]int)
	switch err := row.Scan(&posting_JSON); err {
	case sql.ErrNoRows:
		break
	case nil:
		JSON_error := json.Unmarshal(posting_JSON, &posting)
		if JSON_error != nil {
			break
		}
	default:
		break
	}

	return &posting

}

// create a set of documetn IDs from posting
func createSetFromPosting(posting *map[string][]int) *set.Set {
	set := set.New()
	for doc_id := range *posting {
		set.Insert(doc_id)
	}

	return set
}

// AND two sets together
func ANDhelper(set1 *set.Set, set2 *set.Set) *set.Set {
	return set1.Intersection(set2)
}

// OR two sets together
func ORhelper(set1 *set.Set, set2 *set.Set) *set.Set {
	return set1.Union(set2)
}

// phrase search for two postings
func PhraseSearch(left_posting *map[string][]int, right_posting *map[string][]int) *set.Set {

	results := set.New()

	for docID, left_term_positions := range *left_posting {
		if right_term_positions, present := (*right_posting)[docID]; present {
			for _, left_term_pos := range left_term_positions {
				for _, right_term_pos := range right_term_positions {
					if right_term_pos-left_term_pos == 1 {
						results.Insert(docID)
						break
					}
				}
			}
		}
	}

	return results
}

// proximity search for two postings
func ProxitmitySearch(left_posting *map[string][]int, right_posting *map[string][]int, proximity int) *set.Set {

	results := set.New()

	for docID, left_term_positions := range *left_posting {
		if right_term_positions, present := (*right_posting)[docID]; present {
			for _, left_term_pos := range left_term_positions {
				for _, right_term_pos := range right_term_positions {
					if math.Abs(float64(right_term_pos-left_term_pos)) <= float64(proximity) {
						results.Insert(docID)
						break
					}
				}
			}
		}
	}

	return results
}

// ranked search for a list of postings
func RankedSearch(postings *[]*map[string][]int) *[]string {
	N := 1000

	scores_map := make(map[string]float64)

	// calculate weight for each document in each posting
	for _, posting := range *postings {
		for docID, occurences := range *posting {
			term_frequency := (1 + math.Log10(float64(len(occurences))))
			inv_doc_frequency := math.Log10(float64(N / len(*posting)))
			weight := term_frequency * inv_doc_frequency
			scores_map[docID] = scores_map[docID] + weight
		}

	}

	// sort the documents based on the weight
	docIDs := make([]string, 0, len(scores_map))
	for docID := range scores_map {
		docIDs = append(docIDs, docID)
	}
	sort.SliceStable(docIDs, func(i, j int) bool {
		return scores_map[docIDs[i]] > scores_map[docIDs[j]]
	})

	return &docIDs
}
