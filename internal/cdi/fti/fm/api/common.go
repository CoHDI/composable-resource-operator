package api

type Condition struct {
	Condition []ConditionItem `json:"condition"`
}

type ConditionItem struct {
	Column   string `json:"column"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
