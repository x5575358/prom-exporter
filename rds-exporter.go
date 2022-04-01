package main

import (
	"encoding/json"
	"flag"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/cms"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
)

type Exporter struct {
	newStatusMetric map[string]*prometheus.Desc
}

type Cpu []struct {
	Timestamp  int64   `json:"timestamp"`
	UserID     string  `json:"userId,omitempty"`
	InstanceID string  `json:"instanceId"`
	Device     string  `json:"device"`
	Hostname   string  `json:"hostname"`
	IP         string  `json:"IP"`
	Sum        float64 `json:"Sum"`
	Maximum    float64 `json:"Maximum"`
	Average    float64 `json:"Average"`
	Minimum    float64 `json:"Minimum"`
	State      string  `json:"state"`
	Diskname   string  `json:"diskname"`
}

func newECSMetric(metricName string, docString string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(
		prometheus.BuildFQName("aliyun", "rds", metricName),
		docString, labels, nil,
	)
}

func newExporter() *Exporter {
	return &Exporter{
		newStatusMetric: map[string]*prometheus.Desc{
			"ConnectionUsage":              newECSMetric("ConnectionUsage", "ConnectionUsage", []string{"id"}),
			"CpuUsage":                     newECSMetric("CpuUsage", "CpuUsage", []string{"id"}),
			"DiskUsage":                    newECSMetric("DiskUsage", "DiskUsage", []string{"id"}),
			"IOPSUsage":                    newECSMetric("IOPSUsage", "IOPSUsage", []string{"id"}),
			"MemoryUsage":                  newECSMetric("MemoryUsage", "MemoryUsage", []string{"id"}),
			"MySQL_ActiveSessions":         newECSMetric("MySQL_ActiveSessions", "MySQL_ActiveSessions", []string{"id"}),
			"MySQL_ComDelete":              newECSMetric("MySQL_ComDelete", "MySQL_ComDelete", []string{"id"}),
			"MySQL_ComInsert":              newECSMetric("MySQL_ComInsert", "MySQL_ComInsert", []string{"id"}),
			"MySQL_ComInsertSelect":        newECSMetric("MySQL_ComInsertSelect", "MySQL_ComInsertSelect", []string{"id"}),
			"MySQL_ComReplace":             newECSMetric("MySQL_ComReplace", "MySQL_ComReplace", []string{"id"}),
			"MySQL_ComReplaceSelect":       newECSMetric("MySQL_ComReplaceSelect", "MySQL_ComReplaceSelect", []string{"id"}),
			"MySQL_ComSelect":              newECSMetric("MySQL_ComSelect", "MySQL_ComSelect", []string{"id"}),
			"MySQL_ComUpdate":              newECSMetric("MySQL_ComUpdate", "MySQL_ComUpdate", []string{"id"}),
			"MySQL_QPS":                    newECSMetric("MySQL_QPS", "MySQL_QPS", []string{"id"}),
			"MySQL_TPS":                    newECSMetric("MySQL_TPS", "MySQL_TPS", []string{"id"}),
			"MySQL_NetworkInNew":           newECSMetric("MySQL_NetworkInNew", "MySQL_NetworkInNew", []string{"id"}),
			"MySQL_NetworkOutNew":          newECSMetric("MySQL_NetworkOutNew", "MySQL_NetworkOutNew", []string{"id"}),
			"MySQL_IbufDirtyRatio":         newECSMetric("MySQL_IbufDirtyRatio", "MySQL_IbufDirtyRatio", []string{"id"}),
			"MySQL_IbufUseRatio":           newECSMetric("MySQL_IbufUseRatio", "MySQL_IbufUseRatio", []string{"id"}),
			"MySQL_InnoDBDataRead":         newECSMetric("MySQL_InnoDBDataRead", "MySQL_InnoDBDataRead", []string{"id"}),
			"MySQL_InnoDBDataWritten":      newECSMetric("MySQL_InnoDBDataWritten", "MySQL_InnoDBDataWritten", []string{"id"}),
			"MySQL_TempDiskTableCreates":   newECSMetric("MySQL_TempDiskTableCreates", "MySQL_TempDiskTableCreates", []string{"id"}),
			"MySQL_InnoDBRowUpdate":        newECSMetric("MySQL_InnoDBRowUpdate", "MySQL_InnoDBRowUpdate", []string{"id"}),
			"MySQL_InnoDBRowInsert":        newECSMetric("MySQL_InnoDBRowInsert", "MySQL_InnoDBRowInsert", []string{"id"}),
			"MySQL_InnoDBRowDelete":        newECSMetric("MySQL_InnoDBRowDelete", "MySQL_InnoDBRowDelete", []string{"id"}),
			"MySQL_InnoDBRowRead":          newECSMetric("MySQL_InnoDBRowRead", "MySQL_InnoDBRowRead", []string{"id"}),
			"MySQL_InnoDBLogFsync":         newECSMetric("MySQL_InnoDBLogFsync", "MySQL_InnoDBLogFsync", []string{"id"}),
			"MySQL_InnoDBLogWrites":        newECSMetric("MySQL_InnoDBLogWrites", "MySQL_InnoDBLogWrites", []string{"id"}),
			"MySQL_InnoDBLogWriteRequests": newECSMetric("MySQL_InnoDBLogWriteRequests", "MySQL_InnoDBLogWriteRequests", []string{"id"}),
		},
	}
}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range e.newStatusMetric {
		ch <- m
	}
}

func (e Exporter) Collect(ch chan<- prometheus.Metric) {
	for metric, desc := range e.newStatusMetric {
		request := cms.CreateDescribeMetricLastRequest()
		request.Scheme = "https"
		request.MetricName = metric
		request.Namespace = "acs_rds_dashboard"
		request.AcceptFormat = "json"
		client, _ := cms.NewClientWithAccessKey("cn-hangzhou", "secretid", "secretkey")
		response, err := client.DescribeMetricLast(request)
		if err != nil {
			continue
		}
		var user Cpu
		json.Unmarshal([]byte(string(response.Datapoints)), &user)
		for _, value := range user {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value.Average, value.InstanceID)
		}
	}
}

var (
	listenAddress   = flag.String("telemetry.address", ":8024", "Address on which to expose metrics.")
	metricsEndpoint = flag.String("telemetry.endpoint", "/metrics", "Path under which to expose metrics.")
	//insecure        = flag.Bool("insecure", true, "Ignore server certificate if using https")
)

func main() {
	flag.Parse()
	exporter := newExporter()
	prometheus.MustRegister(exporter)
	prometheus.Unregister(prometheus.NewGoCollector())

	http.Handle(*metricsEndpoint, promhttp.Handler())
	log.Fatal(http.ListenAndServe(*listenAddress, nil))

}
