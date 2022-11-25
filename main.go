package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"runtime"
	"sort"
	"sync"

	"github.com/ofen/getblock-go/eth"
	"github.com/olekukonko/tablewriter"
)

var wg sync.WaitGroup

type BalanceChange struct {
	address string
	balance big.Int
}

func main() {
	// Use all available cores
	// Not really necessary since the network is the bottleneck
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Get the api key from the environment variable
	apiKey := os.Getenv("GETBLOCK_API_KEY")

	if apiKey == "" {
		panic("No API Key provided!")
	}

	// Run the parser function
	runParser(apiKey)
}

func runParser(apiKey string) {

	// Set the number of blocks to parse
	blocksToProcess := 100

	// Configure our worker pool and the IO channels
	// We send the block to parse and receive an array of balance changes
	// Worker pool size is 8, performance is limited by the network speed more than the CPU
	input := make(chan int, blocksToProcess)
	output := make(chan []BalanceChange, blocksToProcess)
	workers := 8

	// Initialze client for Ethereum RPC
	client := eth.New(apiKey)

	// Get the latest block number
	blockNumberResponse, err := client.BlockNumber(context.Background())
	if err != nil {
		fmt.Println("Cannot get latest block number - Exiting!")
		panic(err)
	}

	// The library returns a big.Int, but the blocknumber should never overflow an integer
	// At least not for a long time. For the sake of simplicity we convert it to a int here
	// But will panic if it does not fit into an int
	if !blockNumberResponse.IsInt64() {
		panic("Block number is too big!")
	}

	blockNumber := int(blockNumberResponse.Int64())
	fmt.Println("Latest block number: ", blockNumber)

	// Increment waitgroup counter and create go routines
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go parseBlocks(input, output, client)
	}

	// Producer: load up input channel with jobs
	// Each job is a block number to be processed
	for x := blockNumber - blocksToProcess; x <= blockNumber; x++ {
		input <- x
	}

	// Close input channel since no more jobs are being sent to input channel
	close(input)

	// Wait for all goroutines to finish processing
	wg.Wait()

	// Close output channel since all workers have finished processing
	close(output)

	// Create a balance map to keep track of the total balance changes for each address
	balances := map[string]big.Int{}

	// Read each chunk from output channel
	for result := range output {

		// Process each change from the chunk
		for _, balanceChange := range result {
			balance := balances[balanceChange.address]
			balance = *balance.Add(&balance, &balanceChange.balance)
			balances[balanceChange.address] = balance
		}

	}

	// Sort addresses by total balance change
	keys := make([]string, 0, len(balances))

	for key := range balances {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool {
		balanceOne := balances[keys[i]]
		balanceTwo := balances[keys[j]]
		return balanceOne.Cmp(&balanceTwo) > 0
	})

	// Render a pretty table with the results
	renderTable(keys, balances)
}

func parseBlocks(input chan int, output chan []BalanceChange, client *eth.Client) {
	defer wg.Done()

	// Fetch Block Data from Blockchain
	block, err := client.GetBlockByNumber(context.Background(), big.NewInt(int64(<-input)), true)

	if err != nil {
		fmt.Println(err)
		return
	}

	balances := []BalanceChange{}

	// Iterate through all transactions in the block
	// Add the balance change for each address
	// This is for both to and from addresses, since they both changed
	for _, tx := range block.Transactions {
		// !!! If the value is zero this is most likely a smart contract call or a token transfer !!!
		// The value of ERC20 token transactions is not processed in the same way as a normal transaction
		// The value is always zero, but the token transfer is processed by the smart contract
		// Thus we can ignore these transactions since they will always be zero
		if tx.Value.Cmp(big.NewInt(0)) > 0 {
			balances = append(balances, BalanceChange{balance: *tx.Value, address: tx.From})
			balances = append(balances, BalanceChange{balance: *tx.Value, address: tx.To})
		}
	}

	// Consumer: Send the proccessed chunk back to the output channel
	output <- balances
}

// Render a pretty table with the results
func renderTable(addresses []string, balances map[string]big.Int) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"#", "Address", "Total Change (ETH)"})

	for i, address := range addresses {
		balance := balances[address]
		table.Append([]string{fmt.Sprintf("%d", i+1), address, eth.Wei2ether(&balance).String()})
		i++
	}

	table.Render()
}
