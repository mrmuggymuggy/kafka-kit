package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"regexp"
	"io/ioutil"
	"encoding/json"
	"time"
	"errors"

	"github.com/swarvanusg/go_jolokia"

	"github.com/DataDog/kafka-kit/kafkazk"

	"github.com/jamiealquiza/envy"
	dd "github.com/zorkian/go-datadog-api"
)

// Config holds
// config parameters.
type Config struct {
	Client          *dd.Client
	APIKey          string
	AppKey          string
	ZKAddr          string
	OutputFilePrefix      string
	JolokiaPort     int
	Verbose         bool
	DryRun          bool
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

func partitionMetrics(brokerMeta kafkazk.BrokerMetaMap) (map[string]map[string]map[string]float64) {
	d := map[string]map[string]map[string]float64{}
	//"kafka.log:name=Size,partition=97,topic=fxt-security-audit-logs,type=Log"
	r_partition := regexp.MustCompile(`.*partition=(?P<partition>\d+)`)
	r_topic := regexp.MustCompile(`.*topic=(?P<topic>[^,]+)`)
	//r_match := regexp.MustCompile(`(?:.*?(?:(.*topic=(?P<topic>[^,]+))|(.*partition=(?P<partition>\d+)))){2}`)
	for bid := range brokerMeta {
		m := brokerMeta[bid]
		jcli := go_jolokia.NewJolokiaClient("http://" + m.Host + ":" + strconv.Itoa(config.JolokiaPort) + "/jolokia/")
		//curl -XGET "http://127.0.0.1:8778/jolokia/?p=/read/kafka.log:type=Log,name=Size,*"
		resp, err := jcli.GetAttr("kafka.log:type=Log,name=Size,*", nil, "")
		if err != nil {
			fmt.Println(err)
		}
		partitionMap := resp.(map[string]interface{})
		for key, value := range partitionMap {
			topic := r_topic.FindStringSubmatch(key)[1]
			partition := r_partition.FindStringSubmatch(key)[1]
			if _, exists := d[topic]; !exists {
				d[topic] = map[string]map[string]float64{}
			}
			d[topic][partition] = map[string]float64{}
			//
			v := value.(map[string]interface{})
			d[topic][partition]["Size"] = v["Value"].(float64)
		}
	}
	return d
}

func brokerMetrics(c *Config, brokerMeta kafkazk.BrokerMetaMap) (map[string]map[string]float64, error) {
	query_template := "max:system.disk.free{host:%s}"
	// Populate.
	d := map[string]map[string]float64{}
	for bid := range brokerMeta {
		m := brokerMeta[bid]
		query := fmt.Sprintf(query_template, m.Host)
		start := time.Now().Add(-time.Duration(3600) * time.Second).Unix()
		o, err := c.Client.QueryMetrics(start, time.Now().Unix(), query)
		if err != nil {
			return nil, err
		}
		broker_id := strconv.Itoa(bid)
		if _, exists := d[broker_id]; !exists {
			d[broker_id] = map[string]float64{}
		}
		d[broker_id]["StorageFree"] = *o[0].Points[0][1]
	}
	return d, nil
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
