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

type MachineData struct {
	Data Data `json:"data"`
}

type Data struct {
	TenantUUID string  `json:"tenant_uuid"`
	Cluster    Cluster `json:"cluster"`
}

type Cluster struct {
	ClusterUUID string  `json:"cluster_uuid"`
	Machine     Machine `json:"machine"`
}

type Machine struct {
	UUID          string         `json:"uuid"`
	Name          string         `json:"name"`
	Status        string         `json:"status"`
	StatusReason  string         `json:"status_reason"`
	ResourceSpecs []ResourceSpec `json:"resspecs"`
}

type ResourceSpec struct {
	SpecUUID    string   `json:"spec_uuid"`
	Type        string   `json:"type"`
	Selector    Selector `json:"selector"`
	MinCount    int      `json:"min_resspec_count"`
	MaxCount    int      `json:"max_resspec_count"`
	DeviceCount int      `json:"device_count"`
	Devices     []Device `json:"devices"`
}

type Selector struct {
	Version    string     `json:"version"`
	Expression Expression `json:"expression"`
}

type Expression struct {
	Conditions []Condition `json:"conditions"`
}

type Condition struct {
	Column   string `json:"column"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

type Device struct {
	DeviceUUID   string       `json:"device_id"`
	Status       string       `json:"status"`
	StatusReason string       `json:"status_reason"`
	Detail       DeviceDetail `json:"detail"`
}

type DeviceDetail struct {
	FabricUUID       string               `json:"fabric_uuid"`
	FabricID         int                  `json:"fabric_id"`
	ResourceUUID     string               `json:"res_uuid"`
	FabricGID        string               `json:"fabr_gid"`
	ResourceType     string               `json:"res_type"`
	ResourceName     string               `json:"res_name"`
	ResourceStatus   string               `json:"res_status"`
	ResourceOPStatus string               `json:"res_op_status"`
	ResourceSpec     []DeviceResourceSpec `json:"resspecs"`
	TenantID         string               `json:"tenant_uuid"`
	MachineID        string               `json:"mach_uuid"`
}

type DeviceResourceSpec struct {
	ResourceSpecUUID string `json:"resspec_uuid"`
	ProductName      string `json:"productname"`
	Model            string `json:"model"`
	Vendor           string `json:"vendor"`
	Removable        bool   `json:"removable"`
}

type ErrorBody struct {
	Status int         `json:"status"`
	Detail ErrorDetail `json:"detail"`
}

type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
