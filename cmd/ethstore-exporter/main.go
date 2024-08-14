package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/gobitfly/eth2-beaconchain-explorer/db"
	"github.com/gobitfly/eth2-beaconchain-explorer/exporter"
	"github.com/gobitfly/eth2-beaconchain-explorer/metrics"
	"github.com/gobitfly/eth2-beaconchain-explorer/types"
	"github.com/gobitfly/eth2-beaconchain-explorer/utils"
	"github.com/gobitfly/eth2-beaconchain-explorer/version"

	ethstore "github.com/gobitfly/eth.store"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sirupsen/logrus"
)

func main() {
	configPath := flag.String("config", "", "Path to the config file, if empty string defaults will be used")
	bnAddress := flag.String("beacon-node-address", "", "Url of the beacon node api")
	enAddress := flag.String("execution-node-address", "", "Url of the execution node api")
	updateInterval := flag.Duration("update-intv", 0, "Update interval")
	errorInterval := flag.Duration("error-intv", 0, "Error interval")
	sleepInterval := flag.Duration("sleep-intv", 0, "Sleep interval")
	versionFlag := flag.Bool("version", false, "Show version and exit")
	dayToReexport := flag.Int64("day", -1, "Day to reexport")
	daysToReexport := flag.String("days", "", "Days to reexport")
	receiptsModeStr := flag.String("receipts-mode", "single", "single or batch")
	concurrency := flag.Int("concurrency", 1, "concurrency level to use (1 for no concurrency)")
	debugLevel := flag.Uint64("debug-level", 0, "debug level to use for eth.store calculation output")

	flag.Parse()

	if *versionFlag {
		fmt.Println(version.Version)
		fmt.Println(version.GoVersion)
		return
	}

	cfg := &types.Config{}
	err := utils.ReadConfig(cfg, *configPath)
	if err != nil {
		logrus.Fatalf("error reading config file: %v", err)
	}
	utils.Config = cfg
	logrus.WithField("config", *configPath).WithField("version", version.Version).WithField("chainName", utils.Config.Chain.ClConfig.ConfigName).Printf("starting")

	if utils.Config.Metrics.Enabled {
		go func(addr string) {
			logrus.Infof("serving metrics on %v", addr)
			if err := metrics.Serve(addr); err != nil {
				logrus.WithError(err).Fatal("Error serving metrics")
			}
		}(utils.Config.Metrics.Address)
	}

	db.MustInitDB(&types.DatabaseConfig{
		Username:     cfg.WriterDatabase.Username,
		Password:     cfg.WriterDatabase.Password,
		Name:         cfg.WriterDatabase.Name,
		Host:         cfg.WriterDatabase.Host,
		Port:         cfg.WriterDatabase.Port,
		MaxOpenConns: cfg.WriterDatabase.MaxOpenConns,
		MaxIdleConns: cfg.WriterDatabase.MaxIdleConns,
		SSL:          cfg.WriterDatabase.SSL,
	}, &types.DatabaseConfig{
		Username:     cfg.ReaderDatabase.Username,
		Password:     cfg.ReaderDatabase.Password,
		Name:         cfg.ReaderDatabase.Name,
		Host:         cfg.ReaderDatabase.Host,
		Port:         cfg.ReaderDatabase.Port,
		MaxOpenConns: cfg.ReaderDatabase.MaxOpenConns,
		MaxIdleConns: cfg.ReaderDatabase.MaxIdleConns,
		SSL:          cfg.ReaderDatabase.SSL,
	}, "pgx", "postgres")
	defer db.ReaderDb.Close()
	defer db.WriterDb.Close()

	var startDayReexport int64 = -1
	var endDayReexport int64 = -1

	if *daysToReexport != "" {
		s := strings.Split(*daysToReexport, "-")
		if len(s) < 2 {
			utils.LogFatal(nil, fmt.Sprintf("invalid 'days' flag: %s, expected something of the form 'startDay-endDay'", *daysToReexport), 0)
		}
		startDayReexport, err = strconv.ParseInt(s[0], 10, 64)
		if err != nil {
			utils.LogFatal(err, "error parsing first day of daysToExport flag to int", 0)
		}
		endDayReexport, err = strconv.ParseInt(s[1], 10, 64)
		if err != nil {
			utils.LogFatal(err, "error parsing last day of daysToExport flag to int", 0)
		}
	} else if *dayToReexport >= 0 {
		startDayReexport = *dayToReexport
		endDayReexport = *dayToReexport
	}

	receiptsMode := ethstore.RECEIPTS_MODE_SINGLE

	if *receiptsModeStr == "batch" {
		receiptsMode = ethstore.RECEIPTS_MODE_BATCH
	}

	ethstore.SetDebugLevel(*debugLevel)
	logrus.Infof("using receipts mode %s (%d)", *receiptsModeStr, receiptsMode)
	exporter.StartEthStoreExporter(*bnAddress, *enAddress, *updateInterval, *errorInterval, *sleepInterval, startDayReexport, endDayReexport, *concurrency, receiptsMode)
	logrus.Println("exiting...")
}
