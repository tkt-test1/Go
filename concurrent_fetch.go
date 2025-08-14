package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// DataFetcher は外部からデータを取得するインターフェース
type DataFetcher interface {
	Fetch() (string, error)
	GetSourceName() string
}

// UserFetcher はユーザーデータを取得するモック
type UserFetcher struct{}

func (f *UserFetcher) Fetch() (string, error) {
	// 処理に時間がかかることをシミュレート
	time.Sleep(time.Duration(rand.Intn(500)+500) * time.Millisecond)
	return "User data fetched successfully.", nil
}

func (f *UserFetcher) GetSourceName() string {
	return "User Service"
}

// ProductFetcher はプロダクトデータを取得するモック
type ProductFetcher struct{}

func (f *ProductFetcher) Fetch() (string, error) {
	time.Sleep(time.Duration(rand.Intn(500)+500) * time.Millisecond)
	// エラーをシミュレート
	if rand.Intn(10) < 2 {
		return "", fmt.Errorf("failed to fetch product data")
	}
	return "Product data fetched successfully.", nil
}

func (f *ProductFetcher) GetSourceName() string {
	return "Product Service"
}

// OrderFetcher は注文データを取得するモック
type OrderFetcher struct{}

func (f *OrderFetcher) Fetch() (string, error) {
	time.Sleep(time.Duration(rand.Intn(500)+500) * time.Millisecond)
	return "Order data fetched successfully.", nil
}

func (f *OrderFetcher) GetSourceName() string {
	return "Order Service"
}

// FetchResult は goroutine の結果を格納する構造体
type FetchResult struct {
	Source string
	Data   string
	Error  error
}

// fetchAndSend はデータを取得し、チャネルに結果を送信する関数
func fetchAndSend(fetcher DataFetcher, ch chan<- FetchResult, wg *sync.WaitGroup) {
	defer wg.Done()
	data, err := fetcher.Fetch()
	ch <- FetchResult{
		Source: fetcher.GetSourceName(),
		Data:   data,
		Error:  err,
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// 取得するフェッチャーのリスト
	fetchers := []DataFetcher{
		&UserFetcher{},
		&ProductFetcher{},
		&OrderFetcher{},
	}

	// goroutine の完了を待つための WaitGroup
	var wg sync.WaitGroup
	// 結果を受け取るチャネル
	resultsChannel := make(chan FetchResult, len(fetchers))

	fmt.Println("=== データの並行取得を開始 ===")

	for _, fetcher := range fetchers {
		wg.Add(1)
		// goroutine を起動してデータを非同期に取得
		go fetchAndSend(fetcher, resultsChannel, &wg)
	}

	// 全ての goroutine が完了するまで待機
	wg.Wait()
	close(resultsChannel)

	fmt.Println("=== 全ての goroutine が完了 ===")

	// チャネルから結果を読み出す
	for result := range resultsChannel {
		if result.Error != nil {
			fmt.Printf("❌ %s からのエラー: %v\n", result.Source, result.Error)
		} else {
			fmt.Printf("✅ %s からの結果: %s\n", result.Source, result.Data)
		}
	}

	fmt.Println("=== 処理を終了 ===")
}
