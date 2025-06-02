package domain

type RunnerMsg struct {
	Id     string `json:"id"`
	Action string `json:"action"`
}

type VideoFrame struct {
	ScenarioId string `json:"scenario_id"`
	Data       []byte `json:"data"`
}
