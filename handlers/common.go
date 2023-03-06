package handlers

import (
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
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

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

	lastDay := 0
	err := db.WriterDb.Get(&lastDay, "SELECT COALESCE(MAX(day), 0) FROM validator_stats")
	if err != nil {
		return nil, nil, err
	}

	balances := []*types.Validator{}

	balancesMap := make(map[uint64]*types.Validator, len(balances))

	for _, balance := range balances {
		balancesMap[balance.Index] = balance
	}

	latestBalances, err := db.BigtableClient.GetValidatorBalanceHistory(validators, uint64(latestEpoch), uint64(latestEpoch))
	if err != nil {
		logger.Errorf("error getting validator balance data in GetValidatorEarnings: %v", err)
		return nil, nil, err
	}

	for balanceIndex, balance := range latestBalances {
		if len(balance) == 0 {
			continue
		}

		if balancesMap[balanceIndex] == nil {
			balancesMap[balanceIndex] = &types.Validator{}
		}
		balancesMap[balanceIndex].Balance = balance[0].Balance
		balancesMap[balanceIndex].EffectiveBalance = balance[0].EffectiveBalance
	}

	type Earnings struct {
		ClEarningsTotal     int64 `db:"cl_rewards_gwei_total"`
		ClEarningsLastDay   int64 `db:"cl_rewards_gwei"`
		ClEarningsLastWeek  int64 `db:"cl_rewards_gwei_7d"`
		ClEarningsLastMonth int64 `db:"cl_rewards_gwei_31d"`
		ElEarningsTotal     int64 `db:"el_rewards_gwei_total"`
		ElEarningsLastDay   int64 `db:"el_rewards_gwei"`
		ElEarningsLastWeek  int64 `db:"el_rewards_gwei_7d"`
		ElEarningsLastMonth int64 `db:"el_rewards_gwei_31d"`
	}

	e := &Earnings{}

	err = db.ReaderDb.Get(e, `
		SELECT 
		COALESCE(SUM(cl_rewards_gwei), 0) AS cl_rewards_gwei, 
		COALESCE(SUM(cl_rewards_gwei_7d), 0) AS cl_rewards_gwei_7d, 
		COALESCE(SUM(cl_rewards_gwei_31d), 0) AS cl_rewards_gwei_31d, 
		COALESCE(SUM(cl_rewards_gwei_total), 0) AS cl_rewards_gwei_total,
		COALESCE(SUM(el_rewards_gwei), 0) AS el_rewards_gwei, 
		COALESCE(SUM(el_rewards_gwei_7d), 0) AS el_rewards_gwei_7d, 
		COALESCE(SUM(el_rewards_gwei_31d), 0) AS el_rewards_gwei_31d, 
		COALESCE(SUM(el_rewards_gwei_total), 0) AS el_rewards_gwei_total
		FROM validator_stats WHERE day = $1 AND validatorindex = ANY($2)`, lastDay, validatorsPQArray)
	if err != nil {
		return nil, nil, err
	}

	var totalDeposits int64

	err = db.ReaderDb.Get(&totalDeposits, `
	SELECT 
		COALESCE(SUM(amount), 0) 
	FROM blocks_deposits d
	INNER JOIN blocks b ON b.blockroot = d.block_root AND b.status = '1' 
	WHERE publickey IN (SELECT pubkey FROM validators WHERE validatorindex = ANY($1))`, validatorsPQArray)
	if err != nil {
		return nil, nil, err
	}

	var totalWithdrawals uint64

	err = db.ReaderDb.Get(&totalWithdrawals, `
	SELECT 
		COALESCE(sum(w.amount), 0)
	FROM blocks_withdrawals w
	INNER JOIN blocks b ON b.blockroot = w.block_root AND b.status = '1'
	WHERE validatorindex = ANY($1)
	`, validatorsPQArray)
	if err != nil {
		return nil, nil, err
	}

	earningsLastDay := e.ClEarningsLastDay + e.ElEarningsLastDay
	earningsLastWeek := e.ClEarningsLastWeek + e.ElEarningsLastWeek
	earningsLastMonth := e.ClEarningsLastMonth + e.ElEarningsLastMonth

	//TODO Add 365d earnings once it is available
	return &types.ValidatorEarnings{
		ClIncome1d:           e.ClEarningsLastDay,
		ClIncome7d:           e.ClEarningsLastWeek,
		ClIncome31d:          e.ClEarningsLastMonth,
		ClIncomeTotal:        e.ClEarningsTotal,
		ElIncome1d:           e.ElEarningsLastDay,
		ElIncome7d:           e.ElEarningsLastWeek,
		ElIncome31d:          e.ElEarningsLastMonth,
		ElIncomeTotal:        e.ElEarningsTotal,
		TotalDeposits:        totalDeposits,
		LastDayFormatted:     utils.FormatIncome(earningsLastDay, currency),
		LastWeekFormatted:    utils.FormatIncome(earningsLastWeek, currency),
		LastMonthFormatted:   utils.FormatIncome(earningsLastMonth, currency),
		TotalFormatted:       utils.FormatIncome(e.ClEarningsTotal, currency),
		TotalChangeFormatted: utils.FormatIncome(e.ClEarningsTotal+totalDeposits, currency),
	}, balancesMap, nil
}

// getProposalLuck calculates the luck of a given set of proposed blocks for a certain number of validators
// given the blocks proposed by the validators and the number of validators
//
// precondition: slots is sorted by ascending block number
func getProposalLuck(slots []uint64, validatorsCount int) float64 {
	// Return 0 if there are no proposed blocks or no validators
	if len(slots) == 0 || validatorsCount == 0 {
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
	timeSinceFirstBlock := time.Since(utils.SlotToTime(slots[0]))

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
	for _, slot := range slots {
		if utils.SlotToTime(slot).After(blockProposalCutoffTime) {
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
func getNextBlockEstimateTimestamp(slots []uint64, validatorsCount int) *time.Time {
	// don't estimate if there are no proposed blocks or no validators
	if len(slots) == 0 || validatorsCount == 0 {
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
	nextExpectedSlot := slots[len(slots)-1] + uint64(expectedValue)
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
		for k := range session.Values() {
			k, ok := k.(string)
			if ok && strings.HasPrefix(k, dataTableStatePrefix) {
				count += 1
			}
		}
		if count > 50 {
			_, ok := session.Values()[key]
			if !ok {
				logger.Errorf("error maximum number of datatable states stored in session")
				return
			}
		}
		session.Values()[key] = settings

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

func GetDataTableState(user *types.User, session *utils.CustomSession, tableKey string) *types.DataTableSaveState {
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
	stateRaw, exists := session.Values()["table:state:"+utils.GetNetwork()+":"+tableKey]
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
