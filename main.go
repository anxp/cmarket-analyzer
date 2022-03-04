package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/anxp/bncclient"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type processedData struct {
	IsError bool
	ErrorDescriptions []string
	AbsentSymbols []string
	SymbolsWithNotEnoughData []string
	ProcessedDataBySymbol map[string]analyzedDataOneSymbolByPeriods
}

func main() {

	const ThirtyDaysMS = 30 * 24 * 60 * 60 * 1000
	const SevenDaysMS = 7 * 24 * 60 * 60 * 1000
	const OneDayMS = 24 * 60 * 60 * 1000
	const OneHourMS = 60 * 60 * 1000
	const OneMinuteMS = 60 * 1000

	symbols := []string{
		"THETAUSDT",
		"ANTUSDT",
		"DOTUSDT",
		"CRVUSDT",
		"ONEUSDT",
		"ZECUSDT",
		"KNCUSDT",
		"IOTXUSDT",
		"SXPUSDT",
		"MANAUSDT",
		"MBOXUSDT",
		"ETCUSDT",
		"SNXUSDT",
		"SOLUSDT",
		"DASHUSDT",
		"NEOUSDT",
		"1INCHUSDT",
		"LINKUSDT",
		"LTCUSDT",
		"ADAUSDT",
		"TWTUSDT",
		"TLMUSDT",
		"DYDXUSDT",
		"VETUSDT",
		"CRVUSDT",
		"WANUSDT",
		"SFPUSDT",
		"ALGOUSDT",
		"XRPUSDT",
		"NEOUSDT",
		"XLMUSDT",
		"LINKUSDT",
		"DOTUSDT",
		"TRXUSDT",
		"SHIBUSDT",
		"ICPUSDT",
		"PUNDIXUSDT",
		"WINUSDT",
		"ZECUSDT",
		"ICPUSDT",
	}

	f, _ := os.Open("apikey.txt")
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Scan()
	apiKey := scanner.Text()

	poolState := make(poolState, len(symbols))
	poolStateChannel := make(chan poolStateForSymbol)
	weightController := bncclient.NewWeightController()
	poolKeeper := NewPoolKeeper(apiKey, weightController)

	// ====================== RUN POOL FOR EACH CRYPTO PAIR INDEPENDENTLY (IN PARALLEL ROUTINES) =======================
	for _, symbol := range symbols {
		go poolKeeper.keepSymbolPoolUpToDate(symbol, 15 * OneDayMS, poolStateChannel)
	}

	go poolKeeper.keepGeneralPoolUpToDate(poolStateChannel, &poolState)

	http.HandleFunc("/pool-status", getPoolStatus(&poolState))
	http.HandleFunc("/processed-data", getProcessedData(&poolState))

	err := http.ListenAndServe(":8088", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func getPoolStatus(poolState *poolState) http.HandlerFunc  {
	return func(writer http.ResponseWriter, request *http.Request) {
		jsonStr, err := json.Marshal(*poolState)

		if err != nil {
			fmt.Println(err.Error())
			return
		}

		writer.Header().Set("Access-Control-Allow-Origin", "*")
		fmt.Fprintf(writer, "%s", jsonStr)
		fmt.Printf("Request served: %s\n", request.Host)
	}
}

// GET request should be like this:
// /processed-data?symbols[]=ETHUSDT&symbols[]=ADAUSDT&symbols[]=LTCUSDT&timeslotDurMS=30000&numberOfTimeslots=14
func getProcessedData(poolState *poolState) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		var processedData processedData
		var statCalc statisticCalculator
		currentTimeMS := time.Now().Unix() * 1000

		// =============================== PARSE GET PARAMETERS ========================================================
		symbols := request.URL.Query()["symbols[]"]
		timeslotDurMS, _ := strconv.ParseInt(request.URL.Query().Get("timeslotDurMS"), 10, 64)
		numberOfTimeslots, _ := strconv.Atoi(request.URL.Query().Get("numberOfTimeslots"))

		// =============================== REQUEST VALIDATION ==========================================================
		for _, symbol := range symbols {
			if poolForSymbol, ok := (*poolState)[symbol]; !ok {
				processedData.AbsentSymbols = append(processedData.AbsentSymbols, symbol)
			} else {
				theOldestTimestampInPoolMS := poolForSymbol.LastDealTimestampMS - poolForSymbol.CapturedPeriodMS
				if currentTimeMS - timeslotDurMS*int64(numberOfTimeslots) < theOldestTimestampInPoolMS {
					processedData.SymbolsWithNotEnoughData = append(processedData.SymbolsWithNotEnoughData, symbol)
				}
			}
		}

		if len(processedData.AbsentSymbols) > 0 {
			processedData.IsError = true
			processedData.ErrorDescriptions = append(processedData.ErrorDescriptions, "Unknown pair(s): " + strings.Join(processedData.AbsentSymbols, ", "))
		}

		if len(processedData.SymbolsWithNotEnoughData) > 0 {
			processedData.IsError = true
			processedData.ErrorDescriptions = append(processedData.ErrorDescriptions, "Not enough data for pair(s): " + strings.Join(processedData.SymbolsWithNotEnoughData, ", "))
		}

		if processedData.IsError {
			jsonStr, _ := json.Marshal(processedData)
			writer.Header().Set("Access-Control-Allow-Origin", "*")
			fmt.Fprintf(writer, "%s", jsonStr)
			return
		}

		// =============================== REQUEST - OK, LET'S ANALYZE! ================================================
		processedData.IsError = false
		processedData.ProcessedDataBySymbol = make(map[string]analyzedDataOneSymbolByPeriods, len(symbols))

		for _, symbol := range symbols {
			processedData.ProcessedDataBySymbol[symbol] = statCalc.getStatisticData(*((*poolState)[symbol].aggTradesListRef), timeslotDurMS, numberOfTimeslots)
		}

		jsonStr, _ := json.Marshal(processedData)
		writer.Header().Set("Access-Control-Allow-Origin", "*")
		fmt.Fprintf(writer, "%s", jsonStr)
		return
	}
}

