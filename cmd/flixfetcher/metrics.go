package main

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/DataDog/kafka-kit/kafkazk"
	"github.com/swarvanusg/go_jolokia"
)

func partitionMetrics(brokerMeta kafkazk.BrokerMetaMap) (map[string]map[string]map[string]float64) {
	p := map[string]map[string]map[string]float64{}
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
		var partitionMap map[string]interface{}
		if resp != nil {
			partitionMap = resp.(map[string]interface{})
		}
		for key, value := range partitionMap {
			topic := r_topic.FindStringSubmatch(key)[1]
			partition := r_partition.FindStringSubmatch(key)[1]
			if _, exists := p[topic]; !exists {
				p[topic] = map[string]map[string]float64{}
			}
			p[topic][partition] = map[string]float64{}
			//
			v := value.(map[string]interface{})
			p[topic][partition]["Size"] = v["Value"].(float64)
		}
	}
	return p
}

func brokerMetrics(c *Config, brokerMeta kafkazk.BrokerMetaMap) (map[string]map[string]float64, error) {
	query_template := "sum:system.disk.free{host:%s}"
	// Populate.
	b := map[string]map[string]float64{}
	for bid := range brokerMeta {
		m := brokerMeta[bid]
		query := fmt.Sprintf(query_template, m.Host)
		start := time.Now().Add(-time.Duration(60) * time.Second).Unix()
		o, err := c.Client.QueryMetrics(start, time.Now().Unix(), query)
		if err != nil {
			return nil, err
		}
		broker_id := strconv.Itoa(bid)
		if _, exists := b[broker_id]; !exists {
			b[broker_id] = map[string]float64{}
		}
		b[broker_id]["StorageFree"] = *o[0].Points[0][1]
	}
	return b, nil
}