package client

// Environment is the Spark API representation of a Spark application's environment
type Environment struct {
	SparkProperties [][]string `json:"sparkProperties"`
}

// Application is the Spark API representation of a Spark application
type Application struct {
	ID       string    `json:"id"`
	Name     string    `json:"name"`
	Attempts []Attempt `json:"attempts"`
}

// Attempt is the Spark API representation of a Spark application attempt
type Attempt struct {
	StartTimeEpoch   int64  `json:"startTimeEpoch"`
	EndTimeEpoch     int64  `json:"endTimeEpoch"`
	LastUpdatedEpoch int64  `json:"lastUpdatedEpoch"`
	Duration         int64  `json:"duration"`
	SparkUser        string `json:"sparkUser"`
	Completed        bool   `json:"completed"`
	AppSparkVersion  string `json:"appSparkVersion"`
}

// Stage is the Spark API representation of a Spark application stage
type Stage struct {
	Status          string `json:"status"`
	StageID         int    `json:"stageID"`
	AttemptID       int    `json:"attemptID"`
	InputBytes      int64  `json:"inputBytes"`
	OutputBytes     int64  `json:"outputBytes"`
	ExecutorCpuTime int64  `json:"executorCpuTime"`
}

// Executor is the Spark API representation of a Spark executor
type Executor struct {
	ID                string                     `json:"id"`
	IsActive          bool                       `json:"isActive"`
	AddTime           string                     `json:"addTime"`
	RemoveTime        string                     `json:"removeTime"`
	RemoveReason      string                     `json:"removeReason"`
	RddBlocks         int64                      `json:"rddBlocks"`
	MemoryUsed        int64                      `json:"memoryUsed"`
	DiskUsed          int64                      `json:"diskUsed"`
	TotalCores        int64                      `json:"totalCores"`
	MaxTasks          int64                      `json:"maxTasks"`
	ActiveTasks       int64                      `json:"activeTasks"`
	FailedTasks       int64                      `json:"failedTasks"`
	CompletedTasks    int64                      `json:"completedTasks"`
	TotalTasks        int64                      `json:"totalTasks"`
	TotalDuration     int64                      `json:"totalDuration"`
	TotalGCTime       int64                      `json:"totalGCTime"`
	TotalInputBytes   int64                      `json:"totalInputBytes"`
	TotalShuffleRead  int64                      `json:"totalShuffleRead"`
	TotalShuffleWrite int64                      `json:"totalShuffleWrite"`
	IsBlacklisted     bool                       `json:"isBlacklisted"`
	MaxMemory         int64                      `json:"maxMemory"`
	MemoryMetrics     ExecutorMemoryMetrics      `json:"memoryMetrics"`
	PeakMemoryMetrics *ExecutorPeakMemoryMetrics `json:"peakMemoryMetrics"`
}

// ExecutorMemoryMetrics holds the current values of an executor's memory metrics
type ExecutorMemoryMetrics struct {
	UsedOnHeapStorageMemory   int64 `json:"usedOnHeapStorageMemory"`
	UsedOffHeapStorageMemory  int64 `json:"usedOffHeapStorageMemory"`
	TotalOnHeapStorageMemory  int64 `json:"totalOnHeapStorageMemory"`
	TotalOffHeapStorageMemory int64 `json:"totalOffHeapStorageMemory"`
}

// ExecutorPeakMemoryMetrics holds the peak values of an executor's memory and GC metrics
type ExecutorPeakMemoryMetrics struct {
	JVMHeapMemory              int64 `json:"JVMHeapMemory"`
	JVMOffHeapMemory           int64 `json:"JVMOffHeapMemory"`
	OnHeapExecutionMemory      int64 `json:"OnHeapExecutionMemory"`
	OffHeapExecutionMemory     int64 `json:"OffHeapExecutionMemory"`
	OnHeapStorageMemory        int64 `json:"OnHeapStorageMemory"`
	OffHeapStorageMemory       int64 `json:"OffHeapStorageMemory"`
	OnHeapUnifiedMemory        int64 `json:"OnHeapUnifiedMemory"`
	OffHeapUnifiedMemory       int64 `json:"OffHeapUnifiedMemory"`
	DirectPoolMemory           int64 `json:"DirectPoolMemory"`
	MappedPoolMemory           int64 `json:"MappedPoolMemory"`
	ProcessTreeJVMVMemory      int64 `json:"ProcessTreeJVMVMemory"`
	ProcessTreeJVMRSSMemory    int64 `json:"ProcessTreeJVMRSSMemory"`
	ProcessTreePythonVMemory   int64 `json:"ProcessTreePythonVMemory"`
	ProcessTreePythonRSSMemory int64 `json:"ProcessTreePythonRSSMemory"`
	ProcessTreeOtherVMemory    int64 `json:"ProcessTreeOtherVMemory"`
	ProcessTreeOtherRSSMemory  int64 `json:"ProcessTreeOtherRSSMemory"`
	MinorGCCount               int64 `json:"MinorGCCount"`
	MinorGCTime                int64 `json:"MinorGCTime"`
	MajorGCCount               int64 `json:"MajorGCCount"`
	MajorGCTime                int64 `json:"MajorGCTime"`
}

// StreamingStatistics holds Spark Streaming statistics
type StreamingStatistics struct {
	StartTime         string `json:"startTime"`
	BatchDuration     int64  `json:"batchDuration"`
	AvgProcessingTime int64  `json:"avgProcessingTime"`
}
