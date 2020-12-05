package client

type Environment struct {
	SparkProperties [][]string `json:"sparkProperties"`
}

type Application struct {
	Id       string    `json:"id"`
	Name     string    `json:"name"`
	Attempts []Attempt `json:"attempts"`
}

type Attempt struct {
	StartTimeEpoch   int64  `json:"startTimeEpoch"`
	EndTimeEpoch     int64  `json:"endTimeEpoch"`
	LastUpdatedEpoch int64  `json:"lastUpdatedEpoch"`
	Duration         int64  `json:"duration"`
	SparkUser        string `json:"sparkUser"`
	Completed        bool   `json:"completed"`
	AppSparkVersion  string `json:"appSparkVersion"`
}

type Stage struct {
	StageId         int   `json:"stageId"`
	AttemptId       int   `json:"attemptId"`
	InputBytes      int64 `json:"inputBytes"`
	OutputBytes     int64 `json:"outputBytes"`
	ExecutorCpuTime int64 `json:"executorCpuTime"`
}
