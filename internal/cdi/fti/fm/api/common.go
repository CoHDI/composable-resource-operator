/**
 * (C) Copyright 2025 The CoHDI Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package api

import "encoding/json"

type Condition struct {
	Condition []ConditionItem `json:"condition"`
}

type ConditionItem struct {
	Column   string `json:"column"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

type ErrorBody struct {
	Status int         `json:"status"`
	Detail ErrorDetail `json:"detail"`
}

type ErrorDetail struct {
	Code    string          `json:"code"`
	Message json.RawMessage `json:"message"`
	Data    map[string]any  `json:"data"`
}
