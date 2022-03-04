package main

import (
	"errors"
	"github.com/anxp/bncclient"
	"math"
	"time"
)

type poolKeeper struct {
	apiKey string // Private field, we allow set api key only via constructor.
	weightController *bncclient.RequestWeightController
}

type poolStateForSymbol struct {
	Symbol              string
	PoolFullness        int
	LastDealTimestampMS int64
	CapturedPeriodMS 	int64
	DataReady           bool
	aggTradesListRef    *bncclient.AggTradesList
	error               error
}

// poolState used in API response in order to provide general data about pool healthy and fullness.
// Field aggTradesListRef as reference to RAW data is PRIVATE, because we don't want to make it exportable to JSON.
type poolState map[string]poolStateForSymbol

func NewPoolKeeper(apiKey string, weightController *bncclient.RequestWeightController) *poolKeeper {
	return &poolKeeper{
		apiKey: apiKey,
		weightController: weightController,
	}
}

func (pk *poolKeeper) keepGeneralPoolUpToDate(poolStateChannel <-chan poolStateForSymbol, poolState *poolState) {

	var poolStateForSymbol poolStateForSymbol

	for {
		poolStateForSymbol = <-poolStateChannel

		if poolStateForSymbol.error != nil {
			panic(poolStateForSymbol.error.Error())
		}

		(*poolState)[poolStateForSymbol.Symbol] = poolStateForSymbol
	}
}

func (pk *poolKeeper) keepSymbolPoolUpToDate(symbol string, timePeriodMS int64, actualPoolStateChannel chan<- poolStateForSymbol) {

	const RecordsCountPerRequest = 1000

	// Calculates delay between requests. It calculates dynamically between requests because it depends on market activity and currency pair.
	dynamicDelayMS := func(pool bncclient.AggTradesList) int64 {
		if len(pool) > RecordsCountPerRequest {
			lastIndex := len(pool) - 1
			return pool[lastIndex].AggTime - pool[lastIndex-RecordsCountPerRequest].AggTime
		}

		return 0
	}

	binanceClient := bncclient.NewBinanceClient(pk.apiKey, pk.weightController)

	var aggTradesPoolForSelectedPeriod bncclient.AggTradesList
	var aggTradesForRequest bncclient.AggTradesList

	// ============================== TRY TO START PROCESS AND INITIALIZE THE POOL =====================================
	aggTradesRecent, statusCode, _, err := binanceClient.GetAggregatedTrades(symbol, -1, -1, -1, RecordsCountPerRequest)

	if err != nil {
		// The program cannot be started!
		actualPoolStateChannel <- poolStateForSymbol{symbol, 0, 0, 0, false, nil, err}
		return
	}

	aggTradesPoolForSelectedPeriod = append(aggTradesPoolForSelectedPeriod, aggTradesRecent...)
	// =================================================================================================================
	expirationTimeMS := int64(0)
	allowAddEarlyRecords := true

	for {
		currentTimeMS := time.Now().Unix() * 1000

		// ========================== ADD EARLY RECORDS ONLY IF WE NEED THEM! ==========================================
		if allowAddEarlyRecords && aggTradesPoolForSelectedPeriod[0].AggTime-(currentTimeMS-timePeriodMS) > 1000 { // Only if diff between two timestamps is more than 1 sec.

			startTradeIdOldZone := aggTradesPoolForSelectedPeriod[0].AggTradeId - RecordsCountPerRequest
			aggTradesForRequest, statusCode, _, err = binanceClient.GetAggregatedTrades(symbol, startTradeIdOldZone, -1, -1, RecordsCountPerRequest)

			//TODO: Refactor this error check to anonymous function
			if statusCode == 429 {
				err = errors.New("we got status code = 429, and should immediately stop")
			}

			if err != nil {
				actualPoolStateChannel <- poolStateForSymbol{symbol, 0, 0, 0, false, nil, err}
				return
			}

			// Here we use append to effectively insert aggTradesPartialSelection into beginning of aggTradesPoolForSelectedPeriod slice:
			aggTradesPoolForSelectedPeriod = append(aggTradesForRequest, aggTradesPoolForSelectedPeriod...)
		}
		// =============================================================================================================

		// ========================== CLEAN UP OUTDATED RECORDS ========================================================
		if aggTradesPoolForSelectedPeriod[0].AggTime < (currentTimeMS - timePeriodMS) { // If at least first element of pool is out of actual range
			i := 0
			for i = 0; i < len(aggTradesPoolForSelectedPeriod); i++ {
				if aggTradesPoolForSelectedPeriod[i].AggTime >= (currentTimeMS - timePeriodMS) { // We just found the most left element from which (excluding) we cut-off outdated elements
					break
				}
			}

			allowAddEarlyRecords = false // If we cleaned outdated records at least ONCE, don't add old records EVER!
			aggTradesPoolForSelectedPeriod = aggTradesPoolForSelectedPeriod[i:]
		}
		// =============================================================================================================

		// ========================== ADD MOST RECENT RECORDS AT THE END OF POOL =======================================
		if currentTimeMS > expirationTimeMS {
			startTradeIdNewZone := aggTradesPoolForSelectedPeriod[len(aggTradesPoolForSelectedPeriod)-1].AggTradeId + 1
			aggTradesForRequest, statusCode, _, err = binanceClient.GetAggregatedTrades(symbol, startTradeIdNewZone, -1, -1, RecordsCountPerRequest)

			if statusCode == 429 {
				err = errors.New("we got status code = 429, and should immediately stop")
			}

			if err != nil {
				actualPoolStateChannel <- poolStateForSymbol{symbol, 0, 0, 0, false, nil, err}
				return
			}

			//fmt.Println("\nAdded ", len(aggTradesPartialSelection), " records to the end of the pool.")
			aggTradesPoolForSelectedPeriod = append(aggTradesPoolForSelectedPeriod, aggTradesForRequest...)

			// If we start getting less than RecordsCountPerRequest - let's leave API for some time and don't ask for new data
			if len(aggTradesForRequest) < RecordsCountPerRequest && len(aggTradesPoolForSelectedPeriod) > RecordsCountPerRequest {
				expirationTimeMS = currentTimeMS + dynamicDelayMS(aggTradesPoolForSelectedPeriod)
			}
		}
		// =============================================================================================================

		// ========================== CALCULATE POOL FULLNESS ==========================================================
		// This logic will work correctly only if pool is cleaned up from outdated early records
		// (But it is! We just did it in section "CLEAN UP OUTDATED RECORDS")
		poolFullness := int(math.Round(100 * float64(aggTradesPoolForSelectedPeriod[len(aggTradesPoolForSelectedPeriod)-1].AggTime-aggTradesPoolForSelectedPeriod[0].AggTime) / float64(timePeriodMS)))
		//==============================================================================================================

		actualPoolStateChannel <- poolStateForSymbol{
			symbol,
			poolFullness,
			aggTradesPoolForSelectedPeriod[len(aggTradesPoolForSelectedPeriod)-1].AggTime,
			aggTradesPoolForSelectedPeriod[len(aggTradesPoolForSelectedPeriod)-1].AggTime - aggTradesPoolForSelectedPeriod[0].AggTime,
			poolFullness > 99,
			&aggTradesPoolForSelectedPeriod,
			nil,
		}

		// If fullness reached, put routine to sleep for some time and DONT SPAM TO actualPoolStateChannel.
		// It's very important to keep channel free/clean/available especially if MANY routines running in parallel.
		if poolFullness >= 100 {
			time.Sleep(time.Duration(dynamicDelayMS(aggTradesPoolForSelectedPeriod)) * time.Millisecond)
		}
	}
}
