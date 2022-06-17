package main

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

var dbb *sql.DB
var dbDriver = "sqlite3"
var dbName = "store.db"

type Wallet struct {
	id      uint32
	address string
}

func main() {
	ticker := time.NewTicker(4 * time.Hour)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				getDebankDataForAllWallets()
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

	http.HandleFunc("/usd", usd)
	http.HandleFunc("/fetch", fetch)

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}

func getDB() *sql.DB {
	var err error

	if dbb != nil {
		return dbb
	}

	firstStart := false
	if _, err = os.Stat(dbName); errors.Is(err, os.ErrNotExist) {
		firstStart = true
		_, err = os.Create(dbName)
		if err != nil {
			log.Fatal(err)
		}
	}

	dbb, err = sql.Open(dbDriver, dbName)
	if err != nil {
		panic(err)
	}

	if firstStart {
		initDatabase(dbb)
	}

	return dbb
}

func getDebankDataForAllWallets() {
	wallets := getWallets()
	for _, wallet := range *wallets {
		data := getDebankData(wallet.address)
		insertDebankData(&wallet.id, &data)
	}
}

func getDebankData(address string) string {
	url := fmt.Sprintf("https://openapi.debank.com/v1/user/token_list?id=%s&is_all=true", address)
	resp, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	return string(body)
}

func insertDebankData(id *uint32, data *string) {
	db := getDB()
	_, err := db.Exec("INSERT INTO debank_api_results (data, create_at, address_id) VALUES ($1, $2, $3)", data, time.Now(), id)
	if err != nil {
		log.Fatal(err)
	}
}

func getWallets() *[]Wallet {
	db := getDB()
	rows, err := db.Query("SELECT id, address FROM address")

	if err != nil {
		log.Fatal(err)
	}

	var wallets []Wallet

	for rows.Next() {
		wallet := new(Wallet)
		_ = rows.Scan(&wallet.id, &wallet.address)
		wallets = append(wallets, *wallet)
	}

	err = rows.Close()
	if err != nil {
		log.Fatal(err)
	}

	return &wallets
}

func initDatabase(db *sql.DB) {
	addresses := [8]string{
		"0xba8a8f39b2315d4bc725c026ce3898c2c7e74f57",
		"0x2bd4284509bf6626d5def7ef20d4ca38ce71792e",
		"0x3ea91c76b176779d10cc2a27fd2687888886f0c2",
		"0xe8e94110e568fd45c8eb578bef0f36b5f154b794",
		"0x21bce0768110b9a8c50942be257637a843a7eac6",
		"0x9429614ccabfb2b24f444f33ede29d4575ebcdd1",
		"0x12244c23101f66741dae553c8836a9b2fd4e413a",
		"0x8c2753ee27ba890fbb60653d156d92e1c334f528",
	}

	_, err := db.Exec(`
			CREATE TABLE IF NOT EXISTS address(
				id INTEGER PRIMARY KEY AUTOINCREMENT, 
				address CHAR(42) UNIQUE
			);
		`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS debank_api_results(
				id INTEGER PRIMARY KEY AUTOINCREMENT, 
				data CLOB,
				create_at DATETIME,
				address_id INTEGER,
				FOREIGN KEY(address_id) REFERENCES address(id)
			);
		`)
	if err != nil {
		log.Fatal(err)
	}

	for _, address := range addresses {
		_, err = db.Exec("INSERT INTO address (address) VALUES ($1)", address)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func fetch(w http.ResponseWriter, req *http.Request) {
	getDebankDataForAllWallets()
	fmt.Fprintf(w, "OK\n")
}

func usd(w http.ResponseWriter, req *http.Request) {
	db := getDB()
	rows, err := db.Query("SELECT a.address, r.create_at FROM address AS a LEFT JOIN debank_api_results AS r ON a.id = r.address_id")
	if err != nil {
		log.Fatal(err)
	}

	header := `<tr><th>%s</th><th>%s</th><th>%s</th></tr>`
	tmpl := `<tr><td>0</td><td>%s</td><td>%s</td></tr>`

	fmt.Fprintf(w, "<table>")
	fmt.Fprintf(w, header, "usd", "address", "created_at")
	for rows.Next() {
		var address string
		var createdAt time.Time
		_ = rows.Scan(&address, &createdAt)
		fmt.Fprintf(w, tmpl, address, createdAt.Format(time.RFC850))
	}
	err = rows.Close()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Fprintf(w, "</table>")
}
