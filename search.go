package search_backend

import (
	"database/sql"
	"encoding/json"
	"log"
	"strconv"

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

type ArticleLen struct {
	abstract string
}

type RowCount struct {
	count int
}

type Udid struct {
	id_doc string
}

/*
func main() {
	db := initDB()

	len := getArticleLen("add62d00-e6ea-3113-be35-89730acd8f48", db)

	fmt.Println(len)

	/*

		posting := GetPosting("morbi", db)
		//set := CreateSetFromPosting(posting)

		posting1 := GetPosting("diam", db)
		//set1 := CreateSetFromPosting(posting1)

		result := PhraseSearch(posting, posting1)

		fmt.Println(result)

		set := getNotSetFromString("diam", db, 100)
		fmt.Println(set)
		fmt.Println(set.Len())

		fmt.Println(HydrateDocIDSet(set, 20, db))


	//set := getNotSetFromString("diam", db, 100)
	posting := GetPosting("diam", db)
	set1 := CreateSetFromPosting(posting)

	authors := []string{}
	sentiment := []string{}
	datefrom := "2000-01-01"
	dateto := ""
	categories := []string{}
	publishers := []string{}

	res := FilteredSearch(sentiment, authors, categories, publishers, datefrom, dateto, set1, true, 100, db)
	for _, a := range *res {
		fmt.Println(a)
	}

	//res1 := FilteredSearchSet(sentiment, authors, categories, publishers, datefrom, dateto, 5, db)
	//fmt.Println(res1)

	stopwords := set.New()

	ranked := BM25RankedSearchComplete("Morbi Diam Cum", stopwords, db)
	fmt.Println("-------------------------------------------")
	disp := HydrateDocIDList(ranked, 50, db)
	fmt.Println(disp)
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

func InitStopWords() {
	stop_words := []string{"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"}
	stopwords := set.New()
	for _, word := range stop_words {
		stopwords.Insert(word)
	}
}

// remove stop words and tokenize free text search
func PreProcessFreeTextSearch(search string, stopwords *set.Set) []string {

	filtered := []string{}
	for _, term := range strings.Fields(search) {
		processed_term := PreProcessTerm(term)
		if !stopwords.Has(processed_term) {
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
func HydrateDocIDSet(set *set.Set, limit int, db *sql.DB) []ArticleData {
	var results []ArticleData

	var HydrateDocID = func(docID interface{}) {
		if len(results) >= limit {
			return
		}
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
func HydrateDocIDList(list *[]string, limit int, db *sql.DB) []ArticleData {
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
		if len(results) >= limit {
			return results
		}
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
			results = append(results, ad)
			break
		default:
			break
		}
	}

	for _, docID := range *list {
		if filtered.Has(docID) {
			HydrateDocID(docID)
		}
	}

	return results
}

// run filtered search with parameters, additionaly can be supplied a set of doc IDs from a ranked or boolean search to merge with
func FilteredSearch(sentiment []string, authors []string, categories []string, publishers []string, datefrom string, dateto string, boolean_results *set.Set, merge bool, limit int, db *sql.DB) *[]ArticleData {
	conditions := make([]string, 0)

	if len(sentiment) != 0 {
		var sentiment_condition []string
		for _, sentiment_type := range sentiment {
			condition := "sentiment = '" + sentiment_type + "'"
			sentiment_condition = append(sentiment_condition, condition)
		}
		conditions = append(conditions, strings.Join(sentiment_condition, " OR "))
	}

	if len(publishers) != 0 {
		var publishers_condition []string
		for _, publisher := range publishers {
			condition := "publisher = '" + publisher + "'"
			publishers_condition = append(publishers_condition, condition)
		}
		//conditions = append(conditions, strings.Join(publishers_condition, " OR "))
	}

	if len(authors) != 0 {
		var authors_condition []string
		for _, author := range authors {
			condition := "author = '" + author + "'"
			authors_condition = append(authors_condition, condition)
		}
		conditions = append(conditions, strings.Join(authors_condition, " OR "))
	}

	if datefrom != "" {
		condition := "publication >= '" + datefrom + "'"
		conditions = append(conditions, condition)
	}

	if dateto != "" {
		condition := "publication <= '" + dateto + "'"
		conditions = append(conditions, condition)
	}

	if len(categories) != 0 {
		var categories_condition []string
		for _, category_type := range categories {
			condition := "category = '" + category_type + "'"
			categories_condition = append(categories_condition, condition)
		}
		conditions = append(conditions, strings.Join(categories_condition, " OR "))
	}

	query := "SELECT id_doc, publication, url, sentiment, author, abstract FROM doc_atts WHERE "
	where_clause := strings.Join(conditions, " AND ")

	query = query + where_clause

	query = query + " limit " + strconv.Itoa(limit)

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
func FilteredSearchSet(sentiment []string, authors []string, publishers []string, categories []string, start_date string, end_date string, limit int, db *sql.DB) *set.Set {
	conditions := make([]string, 0)

	if len(sentiment) != 0 {
		var sentiment_condition []string
		for _, sentiment_type := range sentiment {
			condition := "sentiment = '" + sentiment_type + "'"
			sentiment_condition = append(sentiment_condition, condition)
		}
		conditions = append(conditions, strings.Join(sentiment_condition, " OR "))
	}

	if len(categories) != 0 {
		var categories_condition []string
		for _, category_type := range categories {
			condition := "category = '" + category_type + "'"
			categories_condition = append(categories_condition, condition)
		}
		conditions = append(conditions, strings.Join(categories_condition, " OR "))
	}

	if len(publishers) != 0 {
		var publishers_condition []string
		for _, publisher := range publishers {
			condition := "publisher = '" + publisher + "'"
			publishers_condition = append(publishers_condition, condition)
		}
		conditions = append(conditions, strings.Join(publishers_condition, " OR "))
	}

	if len(authors) != 0 {
		var authors_condition []string
		for _, author := range authors {
			condition := "author = '" + author + "'"
			authors_condition = append(authors_condition, condition)
		}
		conditions = append(conditions, strings.Join(authors_condition, " OR "))
	}

	if start_date != "" {
		condition := "publication >= '" + start_date + "'"
		conditions = append(conditions, condition)
	}

	if end_date != "" {
		condition := "publication <= '" + end_date + "'"
		conditions = append(conditions, condition)
	}

	query := "SELECT id_doc, publication, url, sentiment, author, abstract FROM doc_atts WHERE "
	where_clause := strings.Join(conditions, " AND ")

	query = query + where_clause

	query = query + " limit " + strconv.Itoa(limit)

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

// get set of documents that do not contain a term (NOT), can supply limit
func getNotSetFromString(term string, db *sql.DB, limit int) *set.Set {

	results := set.New()
	processed_term := PreProcessTerm(term)
	row := db.QueryRow("SELECT doc_pos FROM word_index WHERE word = $1", processed_term)

	var negated_posting_json []byte
	negated_posting := make(map[string][]int)
	switch err := row.Scan(&negated_posting_json); err {
	case sql.ErrNoRows:
		break
	case nil:
		JSON_error := json.Unmarshal(negated_posting_json, &negated_posting)
		if JSON_error != nil {
			break
		}
	default:
		break
	}

	query := "SELECT id_doc FROM doc_atts limit " + strconv.Itoa(limit)

	rows, err := db.Query(query)
	if err != nil {
		return nil
	}

	for rows.Next() {
		var doc_id Udid
		if err := rows.Scan(&doc_id.id_doc); err != nil {
			log.Println("Error scanning DB row in getNotSet")
			return results
		}

		if _, ok := negated_posting[doc_id.id_doc]; !ok {
			results.Insert(doc_id.id_doc)
		}
	}

	return results
}

// get set of documents by giving a set of documents that are negated (NOT), can supply limit
func getNotSetFromSet(not_set *set.Set, db *sql.DB, limit int) *set.Set {

	results := set.New()
	rows, err := db.Query("SELECT TOP " + strconv.FormatInt(int64(limit), 10) + " id_doc FROM doc_atts")
	if err != nil {
		return nil
	}

	for rows.Next() {
		var doc_id Udid
		if err := rows.Scan(&doc_id.id_doc); err != nil {
			log.Println("Error scanning DB row in getNotSet")
			return results
		}

		if !not_set.Has(doc_id.id_doc) {
			results.Insert(doc_id.id_doc)
		}
	}

	return results
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
func RankedSearch(postings *[]*map[string][]int, db *sql.DB) *[]string {

	row := db.QueryRow("SELECT count(1) FROM doc_atts")
	var count RowCount
	row.Scan(&count.count)

	N := count.count // TODO : query DB to get number of documents

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

// tf idf ranked search for a string search
func RankedSearchComplete(search string, stopwords *set.Set, db *sql.DB) *[]string {

	search_terms := PreProcessFreeTextSearch(search, stopwords)
	var postings []*map[string][]int
	for _, term := range search_terms {
		postings = append(postings, GetPosting(term, db))
	}

	row := db.QueryRow("SELECT count(1) FROM doc_atts")
	var count RowCount
	row.Scan(&count.count)

	N := count.count // TODO : query DB to get number of documents

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

// BM25 ranked search for a string search
func BM25RankedSearchComplete(search string, stopwords *set.Set, db *sql.DB) *[]string {

	k := 1.2
	b := 0.75

	av_doclen := 150

	search_terms := PreProcessFreeTextSearch(search, stopwords)
	var postings []*map[string][]int
	for _, term := range search_terms {
		postings = append(postings, GetPosting(term, db))
	}

	row := db.QueryRow("SELECT count(1) FROM doc_atts")
	var count RowCount
	row.Scan(&count.count)

	N := count.count // TODO : query DB to get number of documents

	scores_map := make(map[string]float64)

	// calculate weight for each document in each posting
	for _, posting := range postings {

		for docID, occurences := range *posting {
			term_frequency := float64(len(occurences))
			inv_doc_frequency := math.Log10(float64(N / len(*posting)))
			doc_len := getArticleLen(docID, db)
			bm25 := ((term_frequency) * (k + 1)) / (term_frequency + k*(1-b+b*(float64(doc_len)/float64(av_doclen))))
			weight := bm25 * inv_doc_frequency
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

// helper to get length of document
func getArticleLen(doc_id string, db *sql.DB) int {

	row := db.QueryRow("SELECT abstract FROM doc_atts WHERE id_doc = $1", doc_id)
	var al ArticleLen
	switch err := row.Scan(&al.abstract); err {
	case sql.ErrNoRows:
		return 0
	case nil:
		return len(al.abstract)
	default:
		return 0
	}
}

// helper to get length of document
func getArticleText(doc_id string, db *sql.DB) string {

	row := db.QueryRow("SELECT abstract FROM doc_atts WHERE id_doc = $1", doc_id)
	var al ArticleLen
	text := ""
	switch err := row.Scan(&al.abstract); err {
	case sql.ErrNoRows:
		return text
	case nil:
		text = al.abstract
		return text
	default:
		return text
	}
}

// query expansion, returns list of new terms to add to search
func QueryExpansion(search string, stopwords *set.Set, n_d int, n_t int, db *sql.DB) *[]string {
	all_docs := RankedSearchComplete(search, stopwords, db)
	top_docs := (*all_docs)[0:n_d]

	top_doc_text := []string{}
	for _, doc_id := range top_docs {
		top_doc_text = append(top_doc_text, getArticleText(doc_id, db))
	}

	all_top_doc_text := strings.Join(top_doc_text, " ")

	search_terms := PreProcessFreeTextSearch(all_top_doc_text, stopwords)
	var postings map[string]map[string][]int
	for _, term := range search_terms {
		postings[term] = *GetPosting(term, db)
	}

	row := db.QueryRow("SELECT count(1) FROM doc_atts")
	var count RowCount
	row.Scan(&count.count)

	N := count.count // TODO : query DB to get number of documents

	var scores_map map[string]float64

	for term, posting := range postings {
		for _, occurences := range posting {
			term_frequency := (1 + math.Log10(float64(len(occurences))))
			inv_doc_frequency := math.Log10(float64(N / len(posting)))
			weight := term_frequency * inv_doc_frequency
			scores_map[term] = scores_map[term] + weight
		}

	}

	// sort the documents based on the weight
	terms := make([]string, 0, len(scores_map))
	for term := range scores_map {
		terms = append(terms, term)
	}
	sort.SliceStable(terms, func(i, j int) bool {
		return scores_map[terms[i]] > scores_map[terms[j]]
	})

	res := terms[0:n_t]

	return &res

}
