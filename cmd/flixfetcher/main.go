package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/DataDog/kafka-kit/kafkazk"

	"github.com/jamiealquiza/envy"
	dd "github.com/zorkian/go-datadog-api"
)

// Config holds
// config parameters.
type Config struct {
	Client          	*dd.Client
	APIKey          	string
	AppKey          	string
	ZKAddr          	string
	OutputFilePrefix    string
	JolokiaPort     	int
	Verbose         	bool
	DryRun          	bool
}

var config = &Config{} // :(

func init() {
	flag.StringVar(&config.APIKey, "api-key", "", "Datadog API key")
	flag.StringVar(&config.AppKey, "app-key", "", "Datadog app key")
	flag.StringVar(&config.ZKAddr, "zk-addr", "localhost:2181", "ZooKeeper connect string")
	flag.IntVar(&config.JolokiaPort, "jolokia-port", 8778, "jolokia jmx port)")
	flag.StringVar(&config.OutputFilePrefix, "output-file-prefix", "flixfetcher.", "output files prefix")
	flag.BoolVar(&config.Verbose, "verbose", false, "Verbose output")
	flag.BoolVar(&config.DryRun, "dry-run", false, "Dry run mode (don't reach Zookeeper)")

	envy.Parse("FLIXFETCHER")
	flag.Parse()
}

func main() {

	config.Client = dd.NewClient(config.APIKey, config.AppKey)
	ok, err := config.Client.Validate()
	exitOnErr(err)

	if !ok {
		exitOnErr(errors.New("Invalid API or app key"))
	}
	// Init ZK client.
	var zk kafkazk.Handler
	
	zk, err = kafkazk.NewHandler(&kafkazk.Config{
		Connect: config.ZKAddr,
	})
	exitOnErr(err)

	brokerMeta, errs := zk.GetAllBrokerMeta(false)
	// If no data is returned, report and exit.
	if errs != nil && brokerMeta == nil {
		for _, e := range errs {
			fmt.Println(e)
		}
		os.Exit(1)
	}
	p := partitionMetrics(brokerMeta)
	b, _ := brokerMetrics(config,brokerMeta)

	partitions_of := config.OutputFilePrefix + "partitions.json"
	brokers_of := config.OutputFilePrefix + "brokers.json"

	if config.Verbose {
		fmt.Printf("Broker data will store at %s:\n%s\n"+
			"Partition data will store at %s:\n%s\n",
			brokers_of, b,
			partitions_of, p)
	}

	if config.DryRun {
		return
	}

	data, _ := json.MarshalIndent(p, "", " ")
	ioutil.WriteFile(partitions_of , data, 0644)

	data, _ = json.MarshalIndent(b, "", " ")
	ioutil.WriteFile(brokers_of , data, 0644)
	// Write to FILES
	fmt.Printf("\nBrokers freespace written to %s\nPartitions sizes written to %s\n", brokers_of, partitions_of)
}

func exitOnErr(e error) {
	if e != nil {
		fmt.Println(e)
		os.Exit(1)
	}
}
