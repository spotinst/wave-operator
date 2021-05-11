package client

type GaugeValue struct {
	Value int64 `json:"value"`
}

type CounterValue struct {
	Count int64 `json:"count"`
}

type Metrics struct {
	Gauges   map[string]GaugeValue   `json:"gauges"`
	Counters map[string]CounterValue `json:"counters"`
}
