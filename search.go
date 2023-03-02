package search_backend

import (
	"database/sql"
	"encoding/json"

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
var a *sql.DB

type ArticleData struct {
	Id          string `db:"id_doc"`
	Date        string `db:"publication"`
	Link        string `db:"url"`
	Sentiment   string `db:"sentiment"`
	Author      string `db:"author"`
	Body        string `db:"abstract"`
	Categories  string `db:"-"`
	Title       string `db:"-"`
	Cover_image string `db:"-"`
	Publisher   string `db:"-"`
}

/*

func main() {
	db := initDB()

	posting := GetPosting("morbi", db)
	//set := CreateSetFromPosting(posting)

	posting1 := GetPosting("diam", db)
	//set1 := CreateSetFromPosting(posting1)

	result := PhraseSearch(posting, posting1)

	fmt.Println(result)

	/*

		fmt.Println(set)
		for _, result := range results {
			fmt.Println(result)
			fmt.Println("------------")
		}



		categories := []string{"crime"}
		sentiment := ""
		start_date := ""
		end_date := ""
		author := ""

		merge := true

		results := FilteredSearch(sentiment, author, categories, start_date, end_date, result, merge)

		results = HydrateDocIDSet(set)

		fmt.Println(set)
		for _, result := range results {
			fmt.Println(result)
			fmt.Println("------------")
		}

		rank := RankedSearchComplete("sit eget donec")

		fmt.Println(rank)


}

// Connect to the PostgreSQL database
func initDB() *sql.DB {
	connStr := "host=34.78.135.69 port=5432 user=postgres password=ttds1234 dbname=articles sslmode=disable"
	var err error
	var db *sql.DB
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		panic(err)

	}

	return db

}

*/

// remove stop words and tokenize free text search
func PreProcessFreeTextSearch(search string) []string {
	stopWords := set.New()
	filtered := []string{}
	for _, term := range strings.Fields(search) {
		processed_term := PreProcessTerm(term)
		if !stopWords.Has(processed_term) {
			filtered = append(filtered, processed_term)
		}
	}

	return filtered
}

// process term and tokenise
func PreProcessTerm(term string) string {
	nonAlphanumericRegex := regexp.MustCompile(`[^a-zA-Z0-9 ]+`)
	term = nonAlphanumericRegex.ReplaceAllString(term, "")
	term = strings.ToLower(term)
	term = porter.Stem(term)
	return term

}

// hydrate a set of docID with article content
func HydrateDocIDSet(set *set.Set, db *sql.DB) []ArticleData {
	var results []ArticleData

	var HydrateDocID = func(docID interface{}) {
		row := db.QueryRow("SELECT id_doc, publication, url, sentiment, author, abstract FROM doc_atts WHERE id_doc = $1", docID)
		var ad ArticleData
		switch err := row.Scan(&ad.Id, &ad.Date, &ad.Link, &ad.Sentiment, &ad.Author, &ad.Body); err {
		case sql.ErrNoRows:
			break
		case nil:
			results = append(results, ad)
			break
		default:
			break
		}
	}

	set.Do(HydrateDocID)

	return results
}

// hydrate a list of docIDs with article content
func HydrateDocIDList(list *[]string, db *sql.DB) []ArticleData {
	var results []ArticleData

	var HydrateDocID = func(docID interface{}) {
		row := db.QueryRow("SELECT id_doc, publication, url, sentiment, author, abstract FROM doc_atts WHERE id_doc = $1", docID)
		var ad ArticleData
		switch err := row.Scan(&ad.Id, &ad.Date, &ad.Link, &ad.Sentiment, &ad.Author, &ad.Body); err {
		case sql.ErrNoRows:
			break
		case nil:
			results = append(results, ad)
			break
		default:
			break
		}
	}

	for _, docID := range *list {
		HydrateDocID(docID)
	}

	return results
}

// hydrate a list of docIDs with article content with aset of filtered docIDs
func HydrateDocIDListFiltered(list *[]string, db *sql.DB, filtered *set.Set) []ArticleData {
	var results []ArticleData

	var HydrateDocID = func(docID interface{}) {
		row := db.QueryRow("SELECT id_doc, publication, url, sentiment, author, abstract FROM doc_atts WHERE id_doc = $1", docID)
		var ad ArticleData
		switch err := row.Scan(&ad.Id, &ad.Date, &ad.Link, &ad.Sentiment, &ad.Author, &ad.Body); err {
		case sql.ErrNoRows:
			break
		case nil:
			if filtered.Has(ad.Id) {
				results = append(results, ad)
			}
			break
		default:
			break
		}
	}

	for _, docID := range *list {
		HydrateDocID(docID)
	}

	return results
}

// run filtered search with parameters, additionaly can be supplied a set of doc IDs from a ranked or boolean search to merge with
func FilteredSearch(sentiment []string, author string, categories []string, start_date string, end_date string, boolean_results *set.Set, merge bool, db *sql.DB) *[]ArticleData {
	conditions := make([]string, 0)

	if len(sentiment) != 0 {
		var sentiment_condition []string
		for _, sentiment_type := range sentiment {
			condition := "sentiment = '" + sentiment_type + "'"
			sentiment_condition = append(sentiment_condition, condition)
		}
		conditions = append(conditions, strings.Join(sentiment_condition, " OR "))
	}

	if author != "" {
		condition := "author = '" + author + "'"
		conditions = append(conditions, condition)
	}

	/*
		if publisher != "" {
			condition := "publisher = '" + publisher + "'"
			conditions = append(conditions, condition)
		}
	*/

	if start_date != "" {
		condition := "publication >= '" + start_date + "'"
		conditions = append(conditions, condition)
	}

	if end_date != "" {
		condition := "publication <= '" + end_date + "'"
		conditions = append(conditions, condition)
	}

	if len(categories) != 0 {
		for _, category := range categories {
			conditions = append(conditions, category)
		}
	}

	query := "SELECT id_doc, publication, url, sentiment, author, abstract FROM doc_atts WHERE "
	where_clause := strings.Join(conditions, " AND ")

	query = query + where_clause

	rows, err := db.Query(query)
	if err != nil {
		return nil
	}

	defer rows.Close()
	var results []ArticleData
	for rows.Next() {
		var ad ArticleData
		if err := rows.Scan(&ad.Id, &ad.Date, &ad.Link, &ad.Sentiment, &ad.Author, &ad.Body); err != nil {
			return &results
		}
		if !merge {
			results = append(results, ad)
		} else {
			if boolean_results.Has(ad.Id) {
				results = append(results, ad)
			}
		}
	}

	return &results

}

// run filtered search with parameters, and return a set of docIDs that can be merged and hydrated for ranked search
func FilteredSearchSet(sentiment []string, author string, categories []string, start_date string, end_date string, db *sql.DB) *set.Set {
	conditions := make([]string, 0)

	if len(sentiment) != 0 {
		var sentiment_condition []string
		for _, sentiment_type := range sentiment {
			condition := "sentiment = '" + sentiment_type + "'"
			sentiment_condition = append(sentiment_condition, condition)
		}
		conditions = append(conditions, strings.Join(sentiment_condition, " OR "))
	}

	if author != "" {
		condition := "author = '" + author + "'"
		conditions = append(conditions, condition)
	}

	/*
		if publisher != "" {
			condition := "publisher = '" + publisher + "'"
			conditions = append(conditions, condition)
		}
	*/

	if start_date != "" {
		condition := "publication >= '" + start_date + "'"
		conditions = append(conditions, condition)
	}

	if end_date != "" {
		condition := "publication <= '" + end_date + "'"
		conditions = append(conditions, condition)
	}

	if len(categories) != 0 {
		for _, category := range categories {
			conditions = append(conditions, category)
		}
	}

	query := "SELECT id_doc, publication, url, sentiment, author, abstract FROM doc_atts WHERE "
	where_clause := strings.Join(conditions, " AND ")

	query = query + where_clause

	rows, err := db.Query(query)
	if err != nil {
		return nil
	}

	defer rows.Close()
	results := set.New()
	for rows.Next() {
		var ad ArticleData
		if err := rows.Scan(&ad.Id, &ad.Date, &ad.Link, &ad.Sentiment, &ad.Author, &ad.Body); err != nil {
			return results
		}
		results.Insert(ad.Id)
	}

	return results

}

// get posting from inverted index on DB, return as a map
func GetPosting(term string, db *sql.DB) *map[string][]int {
	processed_term := PreProcessTerm(term)
	row := db.QueryRow("SELECT doc_pos FROM word_index WHERE word = $1", processed_term)

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
func CreateSetFromPosting(posting *map[string][]int) *set.Set {
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

// phrase search for two postings using linear merge
func PhraseSearchFast(left_posting *map[string][]int, right_posting *map[string][]int) *set.Set {

	results := set.New()

	if len(*left_posting) == 0 || len(*right_posting) == 0 {
		return results
	}

	for docID, left_term_positions := range *left_posting {
		if right_term_positions, present := (*right_posting)[docID]; present {
			right_list_size := len(right_term_positions)
			left_list_size := len(left_term_positions)
			right_list_counter := 0
			left_list_counter := 0
			for right_list_counter < right_list_size && left_list_counter < left_list_size {
				if right_term_positions[right_list_counter]-left_term_positions[left_list_counter] == 1 {
					results.Insert(docID)
					left_list_counter++
					right_list_counter++
				} else if left_term_positions[left_list_counter]+1 < right_term_positions[right_list_counter] {
					left_list_counter++
				} else {
					right_list_counter++
				}
			}
		}
	}

	return results
}

// proximity search for two postings using  linear merge
func ProxitmitySearchFast(left_posting *map[string][]int, right_posting *map[string][]int, proximity int) *set.Set {

	results := set.New()

	if len(*left_posting) == 0 || len(*right_posting) == 0 {
		return results
	}

	for docID, left_term_positions := range *left_posting {
		if right_term_positions, present := (*right_posting)[docID]; present {
			right_list_size := len(right_term_positions)
			left_list_size := len(left_term_positions)
			right_list_counter := 0
			left_list_counter := 0
			for right_list_counter < right_list_size && left_list_counter < left_list_size {
				if math.Abs(float64(right_term_positions[right_list_counter]-left_term_positions[left_list_counter])) <= float64(proximity) {
					results.Insert(docID)
					left_list_counter++
					right_list_counter++
				} else if left_term_positions[left_list_counter] < right_term_positions[right_list_counter] {
					left_list_counter++
				} else {
					right_list_counter++
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

// ranked search for a string search
func RankedSearchComplete(search string, db *sql.DB) *[]string {

	search_terms := PreProcessFreeTextSearch(search)
	var postings []*map[string][]int
	for _, term := range search_terms {
		postings = append(postings, GetPosting(term, db))
	}

	N := 1000 // TODO : query DB to get number of documents

	scores_map := make(map[string]float64)

	// calculate weight for each document in each posting
	for _, posting := range postings {
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
