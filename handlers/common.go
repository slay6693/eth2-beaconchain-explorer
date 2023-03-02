package handlers

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"eth2-exporter/db"
	"eth2-exporter/price"
	"eth2-exporter/services"
	"eth2-exporter/types"
	"eth2-exporter/utils"
	"fmt"
	"html/template"
	"math/big"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gorilla/sessions"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

var pkeyRegex = regexp.MustCompile("[^0-9A-Fa-f]+")

func GetValidatorOnlineThresholdSlot() uint64 {
	latestProposedSlot := services.LatestProposedSlot()
	threshold := utils.Config.Chain.Config.SlotsPerEpoch * 2

	var validatorOnlineThresholdSlot uint64
	if latestProposedSlot < 1 || latestProposedSlot < threshold {
		validatorOnlineThresholdSlot = 0
	} else {
		validatorOnlineThresholdSlot = latestProposedSlot - threshold
	}

	return validatorOnlineThresholdSlot
}

// GetValidatorEarnings will return the earnings (last day, week, month and total) of selected validators
func GetValidatorEarnings(validators []uint64, currency string) (*types.ValidatorEarnings, map[uint64]*types.Validator, error) {
	validatorsPQArray := pq.Array(validators)
	latestEpoch := int64(services.LatestEpoch())
	lastDayEpoch := latestEpoch - int64(utils.EpochsPerDay())
	lastWeekEpoch := latestEpoch - int64(utils.EpochsPerDay())*7
	lastMonthEpoch := latestEpoch - int64(utils.EpochsPerDay())*31
	lastYearEpoch := latestEpoch - int64(utils.EpochsPerDay())*365

	if lastDayEpoch <= 0 {
		lastDayEpoch = 2
	}
	if lastWeekEpoch <= 0 {
		lastWeekEpoch = 2
	}
	if lastMonthEpoch <= 0 {
		lastMonthEpoch = 2
	}
	if lastYearEpoch <= 0 {
		lastYearEpoch = 2
	}

	balances := []*types.Validator{}

	err := db.ReaderDb.Select(&balances, `SELECT 
				validatorindex,
			    COALESCE(balanceactivation, 0) AS balanceactivation, 
       			activationepoch,
       			pubkey
		FROM validators WHERE validatorindex = ANY($1)`, validatorsPQArray)
	if err != nil {
		logger.Error(err)
		return nil, nil, err
	}

	balancesMap := make(map[uint64]*types.Validator, len(balances))

	for _, balance := range balances {
		balancesMap[balance.Index] = balance
	}

	latestBalances, err := db.BigtableClient.GetValidatorBalanceHistory(validators, uint64(latestEpoch), 1)
	if err != nil {
		logger.Errorf("error getting validator balance data in GetValidatorEarnings: %v", err)
		return nil, nil, err
	}
	for balanceIndex, balance := range latestBalances {
		if len(balance) == 0 || balancesMap[balanceIndex] == nil {
			continue
		}
		balancesMap[balanceIndex].Balance = balance[0].Balance
		balancesMap[balanceIndex].EffectiveBalance = balance[0].EffectiveBalance
	}

	balances1d, err := db.BigtableClient.GetValidatorBalanceHistory(validators, uint64(lastDayEpoch), 1)
	if err != nil {
		logger.Errorf("error getting validator Balance1d data in GetValidatorEarnings: %v", err)
		return nil, nil, err
	}
	for balanceIndex, balance := range balances1d {
		if len(balance) == 0 || balancesMap[balanceIndex] == nil {
			continue
		}
		balancesMap[balanceIndex].Balance1d = sql.NullInt64{
			Int64: int64(balance[0].Balance),
			Valid: true,
		}
	}

	balances7d, err := db.BigtableClient.GetValidatorBalanceHistory(validators, uint64(lastWeekEpoch), 1)
	if err != nil {
		logger.Errorf("error getting validator Balance7d data in GetValidatorEarnings: %v", err)
		return nil, nil, err
	}
	for balanceIndex, balance := range balances7d {
		if len(balance) == 0 || balancesMap[balanceIndex] == nil {
			continue
		}
		balancesMap[balanceIndex].Balance7d = sql.NullInt64{
			Int64: int64(balance[0].Balance),
			Valid: true,
		}
	}

	balances31d, err := db.BigtableClient.GetValidatorBalanceHistory(validators, uint64(lastMonthEpoch), 1)
	if err != nil {
		logger.Errorf("error getting validator Balance31d data in GetValidatorEarnings: %v", err)
		return nil, nil, err
	}
	for balanceIndex, balance := range balances31d {
		if len(balance) == 0 || balancesMap[balanceIndex] == nil {
			continue
		}
		balancesMap[balanceIndex].Balance31d = sql.NullInt64{
			Int64: int64(balance[0].Balance),
			Valid: true,
		}
	}

	balances365d, err := db.BigtableClient.GetValidatorBalanceHistory(validators, uint64(lastYearEpoch), 1)
	if err != nil {
		logger.Errorf("error getting validator Balance31d data in GetValidatorEarnings: %v", err)
		return nil, nil, err
	}
	for balanceIndex, balance := range balances365d {
		if len(balance) == 0 || balancesMap[balanceIndex] == nil {
			continue
		}
		balancesMap[balanceIndex].Balance365d = sql.NullInt64{
			Int64: int64(balance[0].Balance),
			Valid: true,
		}
	}

	deposits := []struct {
		Epoch     int64
		Amount    int64
		Publickey []byte
	}{}

	err = db.ReaderDb.Select(&deposits, `
	SELECT 
		block_slot / 32 AS epoch, 
		amount, 
		publickey 
	FROM blocks_deposits d
	INNER JOIN blocks b ON b.blockroot = d.block_root AND b.status = '1' 
	WHERE publickey IN (SELECT pubkey FROM validators WHERE validatorindex = ANY($1))`, validatorsPQArray)
	if err != nil {
		return nil, nil, err
	}

	depositsMap := make(map[string]map[int64]int64)
	for _, d := range deposits {
		if _, exists := depositsMap[fmt.Sprintf("%x", d.Publickey)]; !exists {
			depositsMap[fmt.Sprintf("%x", d.Publickey)] = make(map[int64]int64)
		}
		depositsMap[fmt.Sprintf("%x", d.Publickey)][d.Epoch] += d.Amount
	}

	withdrawals := []struct {
		Epoch          uint64
		Amount         uint64
		ValidatorIndex uint64
	}{}

	err = db.ReaderDb.Select(&withdrawals, `
	SELECT 
		w.validatorindex,
		w.block_slot / 32 AS epoch, 
		sum(w.amount) as amount
	FROM blocks_withdrawals w
	INNER JOIN blocks b ON b.blockroot = w.block_root AND b.status = '1'
	WHERE validatorindex = ANY($1)
	GROUP BY validatorindex, w.block_slot / 32
	`, validatorsPQArray)
	if err != nil {
		return nil, nil, err
	}
	withdrawalsMap := make(map[uint64]map[uint64]uint64)
	for _, w := range withdrawals {
		if _, exists := withdrawalsMap[w.ValidatorIndex]; !exists {
			withdrawalsMap[w.ValidatorIndex] = make(map[uint64]uint64)
		}
		withdrawalsMap[w.ValidatorIndex][w.Epoch] += w.Amount
	}

	var clEarningsTotal int64
	var clEarningsLastDay int64
	var clEarningsLastWeek int64
	var clEarningsLastMonth int64
	var clEarningsLastYear int64
	var totalDeposits int64
	var totalWithdrawals uint64

	for _, balance := range balancesMap {
		if int64(balance.ActivationEpoch) >= latestEpoch {
			continue
		}
		for epoch, deposit := range depositsMap[fmt.Sprintf("%x", balance.PublicKey)] {
			totalDeposits += deposit

			if epoch > int64(balance.ActivationEpoch) {
				clEarningsTotal -= deposit
			}
			if epoch > lastDayEpoch && epoch > int64(balance.ActivationEpoch) {
				clEarningsLastDay -= deposit
			}
			if epoch > lastWeekEpoch && epoch > int64(balance.ActivationEpoch) {
				clEarningsLastWeek -= deposit
			}
			if epoch > lastMonthEpoch && epoch > int64(balance.ActivationEpoch) {
				clEarningsLastMonth -= deposit
			}
			if epoch > lastYearEpoch && epoch > int64(balance.ActivationEpoch) {
				clEarningsLastYear -= deposit
			}
		}

		for epoch, withdrawal := range withdrawalsMap[balance.Index] {
			totalWithdrawals += withdrawal

			if epoch > balance.ActivationEpoch {
				clEarningsTotal += int64(withdrawal)
			}
			if epoch > uint64(lastDayEpoch) && epoch > balance.ActivationEpoch {
				clEarningsLastDay += int64(withdrawal)
			}
			if epoch > uint64(lastWeekEpoch) && epoch > balance.ActivationEpoch {
				clEarningsLastWeek += int64(withdrawal)
			}
			if epoch > uint64(lastMonthEpoch) && epoch > balance.ActivationEpoch {
				clEarningsLastMonth += int64(withdrawal)
			}
		}

		if int64(balance.ActivationEpoch) > lastDayEpoch {
			balance.Balance1d = balance.BalanceActivation
		}
		if int64(balance.ActivationEpoch) > lastWeekEpoch {
			balance.Balance7d = balance.BalanceActivation
		}
		if int64(balance.ActivationEpoch) > lastMonthEpoch {
			balance.Balance31d = balance.BalanceActivation
		}
		if int64(balance.ActivationEpoch) > lastYearEpoch {
			balance.Balance365d = balance.BalanceActivation
		}

		clEarningsTotal += int64(balance.Balance) - balance.BalanceActivation.Int64
		clEarningsLastDay += int64(balance.Balance) - balance.Balance1d.Int64
		clEarningsLastWeek += int64(balance.Balance) - balance.Balance7d.Int64
		clEarningsLastMonth += int64(balance.Balance) - balance.Balance31d.Int64
		clEarningsLastYear += int64(balance.Balance) - balance.Balance365d.Int64
	}

	if totalDeposits == 0 {
		totalDeposits = 32 * 1e9
	}

	// retrieve EL Informaion
	// get all EL blocks
	var execBlocks []types.ExecBlockProposer
	err = db.ReaderDb.Select(&execBlocks,
		`SELECT
			exec_block_number,
			slot
			FROM blocks
		WHERE proposer = ANY($1)
		AND exec_block_number IS NOT NULL
		AND exec_block_number > 0
		ORDER BY exec_block_number ASC`,
		validatorsPQArray,
	)
	if err != nil {
		return nil, nil, err
	}

	blockList := []uint64{}
	for _, slot := range execBlocks {
		blockList = append(blockList, slot.ExecBlock)
	}

	// get EL rewards
	var blocks []*types.Eth1BlockIndexed
	var relaysData map[common.Hash]types.RelaysData

	if len(blockList) > 0 {
		blocks, err = db.BigtableClient.GetBlocksIndexedMultiple(blockList, 10000)
		if err != nil {
			return nil, nil, fmt.Errorf("error retrieving blocks from bigtable: %v", err)
		}
		relaysData, err = db.GetRelayDataForIndexedBlocks(blocks)
		if err != nil {
			return nil, nil, fmt.Errorf("error retrieving mev relay data: %v", err)
		}
	}

	last1dTimestamp := time.Now().Add(-1 * 24 * time.Hour)
	last7dTimestamp := time.Now().Add(-7 * 24 * time.Hour)
	last31dTimestamp := time.Now().Add(-31 * 24 * time.Hour)
	last365dTimestamp := time.Now().Add(-365 * 24 * time.Hour)
	var elEarningsTotal int64
	var elEarningsLastDay int64
	var elEarningsLastWeek int64
	var elEarningsLastMonth int64
	var elEarningsLastYear int64
	for _, execBlock := range blocks {

		var elReward *big.Int
		relayData, ok := relaysData[common.BytesToHash(execBlock.Hash)]
		if ok {
			elReward = relayData.MevBribe.BigInt()
		} else {
			elReward = big.NewInt(0).SetBytes(execBlock.TxReward)
		}
		elReward = elReward.Div(elReward, big.NewInt(1e9))

		execBlockTs := execBlock.Time.AsTime()
		if execBlockTs.After(last1dTimestamp) {
			elEarningsLastDay += elReward.Int64()
		}
		if execBlockTs.After(last7dTimestamp) {
			elEarningsLastWeek += elReward.Int64()
		}
		if execBlockTs.After(last31dTimestamp) {
			elEarningsLastMonth += elReward.Int64()
		}
		if execBlockTs.After(last365dTimestamp) {
			elEarningsLastYear += elReward.Int64()
		}
		elEarningsTotal += elReward.Int64()
	}

	clApr7d := (((float64(clEarningsLastWeek) / 1e9) / (float64(totalDeposits) / 1e9)) * 365) / 7
	if clApr7d < float64(-1) {
		clApr7d = float64(-1)
	}

	clApr31d := (((float64(clEarningsLastMonth) / 1e9) / (float64(totalDeposits) / 1e9)) * 365) / 31
	if clApr31d < float64(-1) {
		clApr31d = float64(-1)
	}

	clApr365d := ((float64(clEarningsLastYear) / 1e9) / (float64(totalDeposits) / 1e9))
	if clApr365d < float64(-1) {
		clApr365d = float64(-1)
	}

	elApr7d := (((float64(elEarningsLastWeek) / 1e9) / (float64(totalDeposits) / 1e9)) * 365) / 7
	if elApr7d < float64(-1) {
		elApr7d = float64(-1)
	}

	elApr31d := (((float64(elEarningsLastMonth) / 1e9) / (float64(totalDeposits) / 1e9)) * 365) / 31
	if elApr31d < float64(-1) {
		elApr31d = float64(-1)
	}

	elApr365d := ((float64(elEarningsLastYear) / 1e9) / (float64(totalDeposits) / 1e9))
	if elApr365d < float64(-1) {
		elApr365d = float64(-1)
	}

	earningsLastDay := clEarningsLastDay + elEarningsLastDay
	earningsLastWeek := clEarningsLastWeek + elEarningsLastWeek
	earningsLastMonth := clEarningsLastMonth + elEarningsLastMonth

	return &types.ValidatorEarnings{
		ClIncome1d:            clEarningsLastDay,
		ClIncome7d:            clEarningsLastWeek,
		ClIncome31d:           clEarningsLastMonth,
		ElIncome1d:            elEarningsLastDay,
		ElIncome7d:            elEarningsLastWeek,
		ElIncome31d:           elEarningsLastMonth,
		ClAPR7d:               clApr7d,
		ClAPR31d:              clApr31d,
		ClAPR365d:             clApr365d,
		ElAPR7d:               elApr7d,
		ElAPR31d:              elApr31d,
		ElAPR365d:             elApr365d,
		TotalExecutionRewards: uint64(elEarningsTotal),
		TotalDeposits:         totalDeposits,
		LastDayFormatted:      utils.FormatIncome(earningsLastDay, currency),
		LastWeekFormatted:     utils.FormatIncome(earningsLastWeek, currency),
		LastMonthFormatted:    utils.FormatIncome(earningsLastMonth, currency),
		TotalFormatted:        utils.FormatIncome(clEarningsTotal, currency),
		TotalChangeFormatted:  utils.FormatIncome(clEarningsTotal+totalDeposits, currency),
		ProposalLuck:          getProposalLuck(execBlocks, len(validators)),
		ProposalEstimate:      getNextBlockEstimateTimestamp(execBlocks, len(validators)),
	}, balancesMap, nil
}

// getProposalLuck calculates the luck of a given set of proposed blocks for a certain number of validators
// given the blocks proposed by the validators and the number of validators
//
// precondition: proposedBlocks is sorted by ascending block number
func getProposalLuck(proposedBlocks []types.ExecBlockProposer, validatorsCount int) float64 {
	// Return 0 if there are no proposed blocks or no validators
	if len(proposedBlocks) == 0 || validatorsCount == 0 {
		return 0
	}
	// Timeframe constants
	fiveDays := time.Hour * 24 * 5
	oneWeek := time.Hour * 24 * 7
	oneMonth := time.Hour * 24 * 30
	sixWeeks := time.Hour * 24 * 45
	twoMonths := time.Hour * 24 * 60
	threeMonths := time.Hour * 24 * 90
	fourMonths := time.Hour * 24 * 120
	fiveMonths := time.Hour * 24 * 150

	activeValidatorsCount := *services.GetLatestStats().ActiveValidatorCount
	// Calculate the expected number of slot proposals for 30 days
	expectedSlotProposals := calcExpectedSlotProposals(oneMonth, validatorsCount, activeValidatorsCount)

	// Get the timeframe for which we should consider qualified proposals
	var proposalTimeframe time.Duration
	// Time since the first block in the proposed block slice
	timeSinceFirstBlock := time.Since(utils.SlotToTime(proposedBlocks[0].Slot))

	// Determine the appropriate timeframe based on the time since the first block and the expected slot proposals
	switch {
	case timeSinceFirstBlock < fiveDays:
		proposalTimeframe = fiveDays
	case timeSinceFirstBlock < oneWeek:
		proposalTimeframe = oneWeek
	case timeSinceFirstBlock < oneMonth:
		proposalTimeframe = oneMonth
	case timeSinceFirstBlock > fiveMonths && expectedSlotProposals <= 0.75:
		proposalTimeframe = fiveMonths
	case timeSinceFirstBlock > fourMonths && expectedSlotProposals <= 1:
		proposalTimeframe = fourMonths
	case timeSinceFirstBlock > threeMonths && expectedSlotProposals <= 1.4:
		proposalTimeframe = threeMonths
	case timeSinceFirstBlock > twoMonths && expectedSlotProposals <= 2.1:
		proposalTimeframe = twoMonths
	case timeSinceFirstBlock > sixWeeks && expectedSlotProposals <= 2.8:
		proposalTimeframe = sixWeeks
	default:
		proposalTimeframe = oneMonth
	}

	// Recalculate expected slot proposals for the new timeframe
	expectedSlotProposals = calcExpectedSlotProposals(proposalTimeframe, validatorsCount, activeValidatorsCount)
	if expectedSlotProposals == 0 {
		return 0
	}
	// Cutoff time for proposals to be considered qualified
	blockProposalCutoffTime := time.Now().Add(-proposalTimeframe)

	// Count the number of qualified proposals
	qualifiedProposalCount := 0
	for _, block := range proposedBlocks {
		if utils.SlotToTime(block.Slot).After(blockProposalCutoffTime) {
			qualifiedProposalCount++
		}
	}
	// Return the luck as the ratio of qualified proposals to expected slot proposals
	return float64(qualifiedProposalCount) / expectedSlotProposals
}

// calcExpectedSlotProposals calculates the expected number of slot proposals for a certain time frame and validator count
func calcExpectedSlotProposals(timeframe time.Duration, validatorCount int, activeValidatorsCount uint64) float64 {
	if validatorCount == 0 || activeValidatorsCount == 0 {
		return 0
	}
	slotsPerTimeframe := timeframe.Seconds() / float64(utils.Config.Chain.Config.SecondsPerSlot)
	return (slotsPerTimeframe / float64(activeValidatorsCount)) * float64(validatorCount)
}

// getNextBlockEstimateTimestamp will return the estimated timestamp of the next block proposal
// given the blocks proposed by the validators and the number of validators
//
// precondition: proposedBlocks is sorted by ascending block number
func getNextBlockEstimateTimestamp(proposedBlocks []types.ExecBlockProposer, validatorsCount int) *time.Time {
	// don't estimate if there are no proposed blocks or no validators
	if len(proposedBlocks) == 0 || validatorsCount == 0 {
		return nil
	}
	activeValidatorsCount := *services.GetLatestStats().ActiveValidatorCount
	if activeValidatorsCount == 0 {
		return nil
	}

	probability := float64(validatorsCount) / float64(activeValidatorsCount)
	// in a geometric distribution, the expected value of the number of trials needed until first success is 1/p
	// you can think of this as the average interval of blocks until you get a proposal
	expectedValue := 1 / probability

	// return the timestamp of the last proposed block plus the average interval
	nextExpectedSlot := proposedBlocks[len(proposedBlocks)-1].Slot + uint64(expectedValue)
	estimate := utils.SlotToTime(nextExpectedSlot)
	return &estimate
}

// getNextSyncEstimateTimestamp will return the estimated timestamp of the next sync committee
// given the maximum sync period the validators have peen part of and the number of validators
func getNextSyncEstimateTimestamp(maxPeriod uint64, validatorsCount int) *time.Time {
	// don't estimate if there are no validators or no sync committees
	if maxPeriod == 0 || validatorsCount == 0 {
		return nil
	}
	activeValidatorsCount := *services.GetLatestStats().ActiveValidatorCount
	if activeValidatorsCount == 0 {
		return nil
	}

	probability := (float64(utils.Config.Chain.Config.SyncCommitteeSize) / float64(activeValidatorsCount)) * float64(validatorsCount)
	// in a geometric distribution, the expected value of the number of trials needed until first success is 1/p
	// you can think of this as the average interval of sync committees until you expect to have been part of one
	expectedValue := 1 / probability

	// return the timestamp of the last sync committee plus the average interval
	nextExpectedSyncPeriod := maxPeriod + uint64(expectedValue)
	estimate := utils.EpochToTime(utils.FirstEpochOfSyncPeriod(nextExpectedSyncPeriod))
	return &estimate
}

// LatestState will return common information that about the current state of the eth2 chain
func LatestState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	currency := GetCurrency(r)
	data := services.LatestState()
	// data.Currency = currency
	data.EthPrice = price.GetEthPrice(currency)
	data.EthRoundPrice = price.GetEthRoundPrice(data.EthPrice)
	data.EthTruncPrice = utils.KFormatterEthPrice(data.EthRoundPrice)

	err := json.NewEncoder(w).Encode(data)

	if err != nil {
		logger.Errorf("error sending latest index page data: %v", err)
		http.Error(w, "Internal server error", http.StatusServiceUnavailable)
		return
	}
}

func GetCurrency(r *http.Request) string {
	if cookie, err := r.Cookie("currency"); err == nil {
		return cookie.Value
	}

	return "ETH"
}

func GetCurrencySymbol(r *http.Request) string {

	cookie, err := r.Cookie("currency")
	if err != nil {
		return "$"
	}

	switch cookie.Value {
	case "AUD":
		return "A$"
	case "CAD":
		return "C$"
	case "CNY":
		return "¥"
	case "EUR":
		return "€"
	case "GBP":
		return "£"
	case "JPY":
		return "¥"
	case "RUB":
		return "₽"
	default:
		return "$"
	}
}

func GetCurrentPrice(r *http.Request) uint64 {
	cookie, err := r.Cookie("currency")
	if err != nil {
		return price.GetEthRoundPrice(price.GetEthPrice("USD"))
	}

	if cookie.Value == "ETH" {
		return price.GetEthRoundPrice(price.GetEthPrice("USD"))
	}
	return price.GetEthRoundPrice(price.GetEthPrice(cookie.Value))
}

func GetCurrentPriceFormatted(r *http.Request) template.HTML {
	userAgent := r.Header.Get("User-Agent")
	userAgent = strings.ToLower(userAgent)
	price := GetCurrentPrice(r)
	if strings.Contains(userAgent, "android") || strings.Contains(userAgent, "iphone") || strings.Contains(userAgent, "windows phone") {
		return utils.KFormatterEthPrice(price)
	}
	return utils.FormatAddCommas(uint64(price))
}

func GetTruncCurrentPriceFormatted(r *http.Request) string {
	price := GetCurrentPrice(r)
	symbol := GetCurrencySymbol(r)
	return fmt.Sprintf("%s %s", symbol, utils.KFormatterEthPrice(price))
}

// GetValidatorIndexFrom gets the validator index from users input
func GetValidatorIndexFrom(userInput string) (pubKey []byte, validatorIndex uint64, err error) {
	validatorIndex, err = strconv.ParseUint(userInput, 10, 64)
	if err == nil {
		pubKey, err = db.GetValidatorPublicKey(validatorIndex)
		return
	}

	pubKey, err = hex.DecodeString(strings.Replace(userInput, "0x", "", -1))
	if err == nil {
		validatorIndex, err = db.GetValidatorIndex(pubKey)
		return
	}
	return
}

func DataTableStateChanges(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	user, session, err := getUserSession(r)
	if err != nil {
		logger.Errorf("error retrieving session: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	response := &types.ApiResponse{}
	response.Status = "ERROR"

	defer json.NewEncoder(w).Encode(response)

	settings := types.DataTableSaveState{}
	err = json.NewDecoder(r.Body).Decode(&settings)
	if err != nil {
		logger.Errorf("error saving data table state could not parse body: %v", err)
		response.Status = "error saving table state"
		return
	}

	// never store the page number
	settings.Start = 0

	key := settings.Key
	if len(key) == 0 {
		logger.Errorf("no key provided")
		response.Status = "error saving table state"
		return
	}

	if !user.Authenticated {
		dataTableStatePrefix := "table:state:" + utils.GetNetwork() + ":"
		key = dataTableStatePrefix + key
		count := 0
		for k := range session.Values {
			k, ok := k.(string)
			if ok && strings.HasPrefix(k, dataTableStatePrefix) {
				count += 1
			}
		}
		if count > 50 {
			_, ok := session.Values[key]
			if !ok {
				logger.Errorf("error maximum number of datatable states stored in session")
				return
			}
		}
		session.Values[key] = settings

		err := session.Save(r, w)
		if err != nil {
			logger.WithError(err).Errorf("error updating session with key: %v and value: %v", key, settings)
		}

	} else {
		err = db.SaveDataTableState(user.UserID, settings.Key, settings)
		if err != nil {
			logger.Errorf("error saving data table state could save values to db: %v", err)
			response.Status = "error saving table state"
			return
		}
	}

	response.Status = "OK"
	response.Data = ""
}

func GetDataTableState(user *types.User, session *sessions.Session, tableKey string) *types.DataTableSaveState {
	state := types.DataTableSaveState{
		Start: 0,
	}
	if user.Authenticated {
		state, err := db.GetDataTablesState(user.UserID, tableKey)
		if err != nil {
			logger.Errorf("error getting data table state from db: %v", err)
			return state
		}
		return state
	}
	stateRaw, exists := session.Values["table:state:"+utils.GetNetwork()+":"+tableKey]
	if !exists {
		return &state
	}
	state, ok := stateRaw.(types.DataTableSaveState)
	if !ok {
		logger.Errorf("error getting state from session: %+v", stateRaw)
		return &state
	}
	return &state
}

// used to handle errors constructed by Template.ExecuteTemplate correctly
func handleTemplateError(w http.ResponseWriter, r *http.Request, fileIdentifier string, functionIdentifier string, infoIdentifier string, err error) error {
	// ignore network related errors
	if err != nil && !errors.Is(err, syscall.EPIPE) && !errors.Is(err, syscall.ETIMEDOUT) {
		logger.WithFields(logrus.Fields{
			"file":       fileIdentifier,
			"function":   functionIdentifier,
			"info":       infoIdentifier,
			"error type": fmt.Sprintf("%T", err),
			"route":      r.URL.String(),
		}).WithError(err).Error("error executing template")
		http.Error(w, "Internal server error", http.StatusServiceUnavailable)
	}
	return err
}
