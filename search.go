package search_backend

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
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

type ArticleData struct {
	Id          string `db:"udid"`
	Date        string `db:"date"`
	Link        string `db:"url"`
	Sentiment   string `db:"sentiment"`
	Author      string `db:"-"`
	Body        string `db:"abstract"`
	Category    string `db:"category"`
	Title       string `db:"title"`
	Cover_image string `db:"image"`
	Publisher   string `db:"publisher"`
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

func mainr() {
	db := initDB()

	len := getArticleLen("pGNSvxOud3unew", db)

	fmt.Println(len)

	temp := set.New()

	qe := QueryExpansionSearchTFIDF("florida shooting", temp, 5, 5, db)

	fmt.Println(*qe)

}

func mainx() {
	db := initDB()

	search := []string{"senior", "hamas", "leader", "ismail"}
	res := NWordPhraseSearch(search, db)

	fmt.Println(res)

	//set1 := CreateSetFromPosting(posting1)

	//result := PhraseSearch(posting, posting1)

	//res := HydrateDocIDSet(posset, 10, db)

	//fmt.Println(res)

	/*
		fmt.Println("-------------")
		authors := []string{}
		sentiment := []string{}
		datefrom := ""
		dateto := ""
		categories := []string{}
		publishers := []string{"go"}

		//temp := set.New()

		fil1 := FilteredSearchSet(sentiment, authors, categories, publishers, datefrom, dateto, 10000, db)

		fil1






			query := "SELECT udid FROM attributes WHERE (publisher ='go')"

			rows, err := db.Query(query)
			if err != nil {
				return
			}

			defer rows.Close()
			for rows.Next() {
				var ad ArticleData
				if err := rows.Scan(&ad.Publisher); err != nil {
					continue
				}
				fmt.Println(ad.Publisher)
			}

	*/

}

// Connect to the PostgreSQL database
func initDB() *sql.DB {
	connStr := "host=34.76.187.212 port=5432 user=postgres password=ttds1234 dbname=v4 sslmode=disable"
	var err error
	var db *sql.DB
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		panic(err)

	}

	return db

}

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

func MergeBooleanWithFilters(udid_set *set.Set, filtered_set *set.Set) *[]string {
	merge := udid_set.Intersection(filtered_set)
	return SetToList(merge)
}

func MergeRankedWithFilters(udid_list *[]string, filtered_set *set.Set) *[]string {
	merge := []string{}
	for _, docID := range *udid_list {
		if filtered_set.Has(docID) {
			merge = append(merge, docID)
		}
	}

	return &merge
}

//ss

// NOT USED
// hydrate a set of docID with article content
func HydrateDocIDSetFast(udid_set *set.Set, limit int, db *sql.DB) *[]ArticleData {
	var results []ArticleData

	in_string := CreateSQLStringFromSet(udid_set)

	query := "SELECT udid, date, url, sentiment, author, abstract, publisher, image, category, title FROM attributes WHERE udid IN " + in_string + " limit " + strconv.Itoa(limit)

	rows, err := db.Query(query)
	if err != nil {
		log.Println("ERROR executing query " + query)
		return nil
	}

	defer rows.Close()
	for rows.Next() {
		var ad ArticleData
		if err := rows.Scan(&ad.Id, &ad.Date, &ad.Link, &ad.Sentiment, &ad.Author, &ad.Body, &ad.Publisher, &ad.Cover_image, &ad.Category, &ad.Title); err != nil {
			log.Println("ERROR scanning row in HydrateDocIDSet, skipping")
			continue
		}
		results = append(results, ad)
	}

	return &results
}

func CreateSQLStringFromSet(all_docs *set.Set) string {
	list := []string{}
	var addString = func(docID interface{}) {
		list = append(list, "'"+docID.(string)+"'")
	}

	all_docs.Do(addString)
	return "(" + strings.Join(list, ",") + ")"
}

func CreateSQLStringFromList(all_docs []string) string {
	list := []string{}
	for _, doc := range all_docs {
		list = append(list, "'"+doc+"'")
	}
	return "(" + strings.Join(list, ",") + ")"
}

// USE THIS ALL THE TIME
// fast hydration for boolean or filtered or ranked docIDs
// for plain boolean hydration, use helper to convert to list and slice and then use this
// for plain ranked hydration, use this directly after slicing
// for plain filtered hydration, use FilteredSearchList and slice and use this
// for merging ranked or boolean with filters, use
// MergeBooleanWithFilters or MergeRankedWithFilters and slice and pass into this
func HydrateDocIDListFast(list *[]string, db *sql.DB) *[]ArticleData {
	var results []ArticleData

	if len(*list) == 0 {
		return &results
	}

	in_string := CreateSQLStringFromList(*list)

	query := "SELECT udid, date, url, sentiment, author, abstract, publisher, image, category, title FROM attributes WHERE udid IN " + in_string

	rows, err := db.Query(query)
	if err != nil {
		log.Println("ERROR executing query " + query)
		return nil
	}

	ad_map := make(map[string]ArticleData)
	defer rows.Close()
	for rows.Next() {
		var ad ArticleData
		if err := rows.Scan(&ad.Id, &ad.Date, &ad.Link, &ad.Sentiment, &ad.Author, &ad.Body, &ad.Publisher, &ad.Cover_image, &ad.Category, &ad.Title); err != nil {
			log.Println("ERROR scanning row in HydrateDocIDListFast skipping")
			continue
		}
		if ad.Date == "1900-01-01T00:00:00Z" {
			ad.Date = "N/A"
		}
		ad_map[ad.Id] = ad
	}

	for _, docID := range *list {
		if val, ok := ad_map[docID]; ok {
			results = append(results, val)
		}
	}

	return &results
}

// NOT USED ANYMORE
// hydrate a list of docIDs with article content with aset of filtered docIDs
func HydrateDocIDListFilteredFast(list *[]string, limit int, db *sql.DB, filtered *set.Set) *[]ArticleData {
	var results []ArticleData

	in_string := CreateSQLStringFromList(*list)

	query := "SELECT udid, date, url, sentiment, author, abstract, publisher, image, category, title FROM attributes WHERE udid IN " + in_string + " limit " + strconv.Itoa(limit)

	rows, err := db.Query(query)
	if err != nil {
		return nil
	}

	ad_map := make(map[string]ArticleData)
	defer rows.Close()
	for rows.Next() {
		var ad ArticleData
		if err := rows.Scan(&ad.Id, &ad.Date, &ad.Link, &ad.Sentiment, &ad.Author, &ad.Body, &ad.Publisher, &ad.Cover_image, &ad.Category, &ad.Title); err != nil {
			continue
		}
		ad_map[ad.Id] = ad
	}

	for _, docID := range *list {
		if val, ok := ad_map[docID]; ok {
			if filtered.Has(docID) {
				results = append(results, val)
			}
		}
	}

	return &results
}

// run filtered search with parameters, and return a set of docIDs that can be merged
func FilteredSearchSet(sentiment []string, authors []string, categories []string, publishers []string, datefrom string, dateto string, limit int, db *sql.DB) *set.Set {
	conditions := make([]string, 0)

	if len(sentiment) != 0 {
		var sentiment_condition []string
		for _, sentiment_type := range sentiment {
			condition := "sentiment = '" + sentiment_type + "'"
			sentiment_condition = append(sentiment_condition, condition)
		}
		conditions = append(conditions, "("+strings.Join(sentiment_condition, " OR ")+")")
	}

	if len(publishers) != 0 {
		var publishers_condition []string
		for _, publisher := range publishers {
			condition := "publisher = '" + publisher + "'"
			publishers_condition = append(publishers_condition, condition)
		}
		conditions = append(conditions, "("+strings.Join(publishers_condition, " OR ")+")")
	}

	if len(authors) != 0 {
		var authors_condition []string
		for _, author := range authors {
			condition := "author = '" + author + "'"
			authors_condition = append(authors_condition, condition)
		}
		conditions = append(conditions, "("+strings.Join(authors_condition, " OR ")+")")
	}

	if datefrom != "" {
		condition := "date >= '" + datefrom + "'"
		conditions = append(conditions, "("+condition+")")
	}

	if dateto != "" {
		condition := "date <= '" + dateto + "'"
		conditions = append(conditions, "("+condition+")")
	}

	if len(categories) != 0 {
		var categories_condition []string
		for _, category_type := range categories {
			condition := "category = '" + category_type + "'"
			categories_condition = append(categories_condition, condition)
		}
		conditions = append(conditions, "("+strings.Join(categories_condition, " OR ")+")")
	}

	query := "SELECT udid FROM attributes WHERE "
	where_clause := strings.Join(conditions, " AND ")

	query = query + where_clause

	query = query + " limit " + strconv.Itoa(limit)
	results := set.New()
	rows, err := db.Query(query)
	if err != nil {
		log.Println("ERROR executing query " + query)
		return results
	}

	defer rows.Close()
	for rows.Next() {
		var ad ArticleData
		if err := rows.Scan(&ad.Id); err != nil {
			log.Println("ERROR scanning row in FilterSearc, skipping")
			continue
		}
		results.Insert(ad.Id)
	}

	return results
}

// run filtered search with parameters, and return a list of docID to hydrate ONLY WHEN NO QUERY FROM USERS
func FilteredSearchList(sentiment []string, authors []string, categories []string, publishers []string, datefrom string, dateto string, limit int, db *sql.DB) *[]string {
	conditions := make([]string, 0)

	if len(sentiment) != 0 {
		var sentiment_condition []string
		for _, sentiment_type := range sentiment {
			condition := "sentiment = '" + sentiment_type + "'"
			sentiment_condition = append(sentiment_condition, condition)
		}
		conditions = append(conditions, "("+strings.Join(sentiment_condition, " OR ")+")")
	}

	if len(publishers) != 0 {
		var publishers_condition []string
		for _, publisher := range publishers {
			condition := "publisher = '" + publisher + "'"
			publishers_condition = append(publishers_condition, condition)
		}
		conditions = append(conditions, "("+strings.Join(publishers_condition, " OR ")+")")
	}

	if len(authors) != 0 {
		var authors_condition []string
		for _, author := range authors {
			condition := "author = '" + author + "'"
			authors_condition = append(authors_condition, condition)
		}
		conditions = append(conditions, "("+strings.Join(authors_condition, " OR ")+")")
	}

	if datefrom != "" {
		condition := "date >= '" + datefrom + "'"
		conditions = append(conditions, "("+condition+")")
	}

	if dateto != "" {
		condition := "date <= '" + dateto + "'"
		conditions = append(conditions, "("+condition+")")
	}

	if len(categories) != 0 {
		var categories_condition []string
		for _, category_type := range categories {
			condition := "category = '" + category_type + "'"
			categories_condition = append(categories_condition, condition)
		}
		conditions = append(conditions, "("+strings.Join(categories_condition, " OR ")+")")
	}

	query := "SELECT udid FROM attributes WHERE "
	where_clause := strings.Join(conditions, " AND ")

	query = query + where_clause

	query = query + " limit " + strconv.Itoa(limit)

	rows, err := db.Query(query)
	if err != nil {
		log.Println("ERROR executing query " + query)
		return nil
	}

	defer rows.Close()
	results := []string{}
	for rows.Next() {
		var ad ArticleData
		if err := rows.Scan(&ad.Id); err != nil {
			log.Println("ERROR scanning row in FilterSearc, skipping")
			continue
		}
		results = append(results, ad.Id)
	}

	return &results
}

func SetToList(input *set.Set) *[]string {
	var results []string

	var addToList = func(docID interface{}) {
		results = append(results, docID.(string))
	}

	input.Do(addToList)

	return &results
}

func ListToSet(input *[]string) *set.Set {
	results := set.New()

	for _, docID := range *input {
		results.Insert(docID)
	}

	return results
}

// get posting from inverted index on DB, return as a map
func GetPosting(term string, db *sql.DB) *map[string][]int {
	processed_term := PreProcessTerm(term)
	row := db.QueryRow("SELECT index FROM word_index WHERE word = $1", processed_term)

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
func GetNotSetFromString(term string, db *sql.DB, limit int) *set.Set {

	results := set.New()
	processed_term := PreProcessTerm(term)
	row := db.QueryRow("SELECT index FROM word_index WHERE word = $1", processed_term)

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

	query := "SELECT udid FROM attributes limit " + strconv.Itoa(limit)

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
func GetNotSetFromSet(not_set *set.Set, db *sql.DB, limit int) *set.Set {

	results := set.New()
	query := "SELECT udid FROM attributes limit " + strconv.Itoa(limit)

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

// helper function
func GetArticlesInAllTerms(postings *[]*map[string][]int) *set.Set {
	init_set := set.New()
	var list []*set.Set
	for _, posting := range *postings {
		posting_set := set.New()
		for docID, _ := range *posting {
			posting_set.Insert(docID)
		}
		init_set = posting_set
		list = append(list, posting_set)

	}

	for _, temp := range list {
		init_set = init_set.Intersection(temp)
	}

	return init_set

}

// N word phrase search for n words
func NWordPhraseSearch(words []string, db *sql.DB) *set.Set {

	var postings []*(map[string][]int)

	for _, word := range words {
		postings = append(postings, GetPosting(word, db))
	}
	relevant_docIDs := GetArticlesInAllTerms(&postings)

	results := set.New()

	rel_list := SetToList(relevant_docIDs)

	for _, docID := range *rel_list {

		temp := (postings)[0]
		for _, start := range (*temp)[docID] {
			if CheckForSequencePos(&postings, start, docID) {
				results.Insert(docID)
				break
			}

		}

	}

	return results

}

// helper function
func CheckForSequencePos(postings *[]*map[string][]int, start_index int, docID string) bool {
	curr_index := start_index
	for _, posting := range *postings {
		ok := false
		occurences := (*posting)[docID]
		for _, occ := range occurences {
			if occ == curr_index {
				ok = true
				break
			}
		}

		if !ok {
			return false
		}

		curr_index = curr_index + 1

	}

	return true
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

func GetNumArticles(db *sql.DB) int {
	row := db.QueryRow("SELECT count(1) FROM attributes")
	var count RowCount
	switch err := row.Scan(&count.count); err {
	case sql.ErrNoRows:
		return 0
	case nil:
		return count.count
	default:
		return 0
	}
}

// tf idf ranked search for a string search
func TFIDFRankedSearchComplete(search string, stopwords *set.Set, db *sql.DB) *[]string {

	search_terms := PreProcessFreeTextSearch(search, stopwords)
	var postings []*map[string][]int
	for _, term := range search_terms {
		postings = append(postings, GetPosting(term, db))
	}

	row := db.QueryRow("SELECT count(1) FROM attributes")
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

	av_doclen := 35

	search_terms := PreProcessFreeTextSearch(search, stopwords)
	var postings []*map[string][]int
	for _, term := range search_terms {
		postings = append(postings, GetPosting(term, db))
	}

	row := db.QueryRow("SELECT count(1) FROM attributes")
	var count RowCount
	row.Scan(&count.count)

	N := count.count // TODO : query DB to get number of documents

	scores_map := make(map[string]float64)

	// calculate weight for each document in each posting
	for _, posting := range postings {

		for docID, occurences := range *posting {
			term_frequency := float64(len(occurences))
			inv_doc_frequency := math.Log10(float64(N / len(*posting)))
			//doc_len := getArticleLen(docID, db)
			doc_len := 35
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

	row := db.QueryRow("SELECT abstract FROM attributes WHERE udid = $1", doc_id)
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

	row := db.QueryRow("SELECT abstract FROM attributes WHERE udid = $1", doc_id)
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
	all_docs := TFIDFRankedSearchComplete(search, stopwords, db)
	top_docs := (*all_docs)[0:n_d]

	top_doc_text := []string{}
	for _, doc_id := range top_docs {
		top_doc_text = append(top_doc_text, getArticleText(doc_id, db))
	}

	all_top_doc_text := strings.Join(top_doc_text, " ")

	search_terms := PreProcessFreeTextSearch(all_top_doc_text, stopwords)
	term_postings := make(map[string](map[string][]int))
	for _, term := range search_terms {
		term_postings[term] = *GetPosting(term, db)
	}

	row := db.QueryRow("SELECT count(1) FROM attributes")
	var count RowCount
	row.Scan(&count.count)

	N := count.count // TODO : query DB to get number of documents

	scores_map := make(map[string]float64)

	for term, posting := range term_postings {
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

func QueryExpansionSearchTFIDF(search string, stopwords *set.Set, n_d int, n_t int, db *sql.DB) *[]string {
	all_docs := TFIDFRankedSearchComplete(search, stopwords, db)
	top_docs := (*all_docs)[0:n_d]

	top_doc_text := []string{}
	for _, doc_id := range top_docs {
		top_doc_text = append(top_doc_text, getArticleText(doc_id, db))
	}

	all_top_doc_text := strings.Join(top_doc_text, " ")

	search_terms := PreProcessFreeTextSearch(all_top_doc_text, stopwords)
	term_postings := make(map[string](map[string][]int))
	final_search_terms := []string{}
	for i := 1; i < 10; i++ {
		final_search_terms = append(final_search_terms, search_terms[rand.Intn(len(search_terms))])
	}
	for _, term := range final_search_terms {
		term_postings[term] = *GetPosting(term, db)
	}

	row := db.QueryRow("SELECT count(1) FROM attributes")
	var count RowCount
	row.Scan(&count.count)

	N := count.count // TODO : query DB to get number of documents

	scores_map := make(map[string]float64)

	for term, posting := range term_postings {
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

	new_query := search + " " + strings.Join(res, " ")

	return TFIDFRankedSearchComplete(new_query, stopwords, db)

}

func QueryExpansionSearchBM25(search string, stopwords *set.Set, n_d int, n_t int, db *sql.DB) *[]string {
	all_docs := BM25RankedSearchComplete(search, stopwords, db)
	top_docs := (*all_docs)[0:n_d]

	top_doc_text := []string{}
	for _, doc_id := range top_docs {
		top_doc_text = append(top_doc_text, getArticleText(doc_id, db))
	}

	all_top_doc_text := strings.Join(top_doc_text, " ")

	search_terms := PreProcessFreeTextSearch(all_top_doc_text, stopwords)
	term_postings := make(map[string](map[string][]int))
	final_search_terms := []string{}
	for i := 1; i < 10; i++ {
		final_search_terms = append(final_search_terms, search_terms[rand.Intn(len(search_terms))])
	}
	for _, term := range final_search_terms {
		term_postings[term] = *GetPosting(term, db)
	}

	row := db.QueryRow("SELECT count(1) FROM attributes")
	var count RowCount
	row.Scan(&count.count)

	N := count.count // TODO : query DB to get number of documents

	scores_map := make(map[string]float64)

	for term, posting := range term_postings {
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

	new_query := search + " " + strings.Join(res, " ")

	return BM25RankedSearchComplete(new_query, stopwords, db)

}
