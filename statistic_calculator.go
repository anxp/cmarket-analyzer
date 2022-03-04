package main

import (
	"github.com/anxp/bncclient"
	"math"
	"time"
)

// ========================== TYPES ====================================================================================
type analyzedDataOneSymbolOnePeriod struct {
	BuyVolume             float64 // In amount of ETH for ETHUSDT pair
	SellVolume            float64 // In amount of ETH for ETHUSDT pair
	BuyVolumePercentage   int     // Calculated in % for ETH for ETHUSDT pair
	SellVolumePercentage  int     // Calculated in % for ETH for ETHUSDT pair
	PriceChangePercentage int     // Relative to price in previous timeframe (period)
	AvgPrice              float64
}

type analyzedDataOneSymbolByPeriods []analyzedDataOneSymbolOnePeriod

type statisticCalculator struct {
}
// =====================================================================================================================

/***********************************************************************************************************************
getStatisticData analyzes:
1. 	Price change dynamics for aggregated trades list (for specific symbol). Trades list data will be split for
	numberOfTimeslots periods, average price will be calculated for each period, and after that,
	price change (in %) will be calculated between current period and previous.
2. 	Buy and sell volume for each period and their percentage.
3.	Average price for each period.
4.	Total asset cost for each period.

IMPORTANT!!! We pass aggTrades by value (NOT AS POINTER) because we want to deal with COPY of pool,
to ensure it will not be changed when processing it!
***********************************************************************************************************************/
func (statCalc *statisticCalculator) getStatisticData(aggTrades bncclient.AggTradesList, timeslotDurMS int64, numberOfTimeslots int) analyzedDataOneSymbolByPeriods {

	firstIndex := 0

	currentTimeMS := time.Now().Unix() * 1000
	requestedPeriodStartTimeMS := currentTimeMS - timeslotDurMS*int64(numberOfTimeslots)

	for i, trade := range aggTrades {
		if trade.AggTime >= requestedPeriodStartTimeMS {
			firstIndex = i
			break
		}
	}

	// =================================== RESAMPLE ACCORDINGLY TO REQUESTED PERIOD ====================================
	aggTrades = aggTrades[firstIndex:]
	// =================================================================================================================

	totalDurationMS := (aggTrades)[len(aggTrades) - 1].AggTime - (aggTrades)[0].AggTime
	timeslotDurationMS := totalDurationMS / int64(numberOfTimeslots) // Timeslot Duration more accurately recalculated to fit specific pool
	timeslotStartMS := (aggTrades)[0].AggTime
	timeslotEndMS := (aggTrades)[0].AggTime + timeslotDurationMS

	analyzedData := make(analyzedDataOneSymbolByPeriods, numberOfTimeslots)

	//TODO: Check this
	//length := len(analyzedData)

	buyVolume := 0.0
	sellVolume := 0.0
	buyVolumePercentage := 0
	sellVolumePercentage := 0
	priceChangePercentage := 0
	avgPrice := 0.0
	totalAssetCost := 0.0

	numberOfCurrentPeriod := 0
	for _, trade := range aggTrades {
		if timeslotStartMS <= trade.AggTime && trade.AggTime < timeslotEndMS {
			if trade.AggIsBuyerMaker == true { // CONSIDERED AS SELL DEAL
				sellVolume += trade.AggQty
			}

			if trade.AggIsBuyerMaker == false { // CONSIDERED AS BUY DEAL
				buyVolume += trade.AggQty
			}

			totalAssetCost += trade.AggQty * trade.AggPrice

			// 90% of iterations these variables will be overwritten, but that's ok, we cannot "optimize" this part
			buyVolumePercentage = int(math.Round(100 * buyVolume / (buyVolume + sellVolume)))
			sellVolumePercentage = int(math.Round(100 * sellVolume / (buyVolume + sellVolume)))
			avgPrice = totalAssetCost / (buyVolume + sellVolume)
			if numberOfCurrentPeriod > 0 {
				priceChangePercentage = int(math.Round(100 * (avgPrice - analyzedData[numberOfCurrentPeriod-1].AvgPrice) / analyzedData[numberOfCurrentPeriod-1].AvgPrice))
			}
		}

		if trade.AggTime >= timeslotEndMS {

			// SAVE CALCULATED DATA
			analyzedData[numberOfCurrentPeriod] = analyzedDataOneSymbolOnePeriod{
				buyVolume,
				sellVolume,
				buyVolumePercentage,
				sellVolumePercentage,
				priceChangePercentage,
				avgPrice,
			}

			// SET MARKERS FOR NEXT TIMEFRAME
			timeslotStartMS = timeslotEndMS
			timeslotEndMS = timeslotStartMS + timeslotDurationMS

			// ZERO OUT TEMPORARY VARS
			buyVolume = 0.0
			sellVolume = 0.0
			buyVolumePercentage = 0
			sellVolumePercentage = 0
			priceChangePercentage = 0
			avgPrice = 0.0
			totalAssetCost = 0.0

			// SWITCH TO NEXT PERIOD
			numberOfCurrentPeriod++
		}
	}

	return analyzedData
}
