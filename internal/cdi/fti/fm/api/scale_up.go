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

type ScaleUpBody struct {
	Tenants ScaleUpTenants `json:"tenants"`
}

type ScaleUpTenants struct {
	TenantUUID string               `json:"tenant_uuid"`
	Machines   []ScaleUpMachineItem `json:"machines"`
}

type ScaleUpMachineItem struct {
	MachineUUID string                `json:"mach_uuid"`
	Resources   []ScaleUpResourceItem `json:"resources"`
}

type ScaleUpResourceItem struct {
	ResourceSpecs []ScaleUpResourceSpecItem `json:"res_specs"`
}

type ScaleUpResourceSpecItem struct {
	Type string    `json:"res_type"`
	Spec Condition `json:"res_spec"`
	Num  int       `json:"res_num"`
}

type ScaleUpResponse struct {
	Data ScaleUpResponseData `json:"data"`
}

type ScaleUpResponseData struct {
	Machines []ScaleUpResponseMachineItem `json:"machines"`
}

type ScaleUpResponseMachineItem struct {
	FabricUUID  string                        `json:"fabric_uuid"`
	FabricID    int                           `json:"fabric_id"`
	MachineUUID string                        `json:"mach_uuid"`
	MachineID   int                           `json:"mach_id"`
	MachineName string                        `json:"mach_name"`
	TenantUUID  string                        `json:"tenant_uuid"`
	Resources   []ScaleUpResponseResourceItem `json:"resources"`
}

type ScaleUpResponseResourceItem struct {
	ResourceUUID string    `json:"res_uuid"`
	ResourceName string    `json:"res_name"`
	Type         string    `json:"res_type"`
	Status       int       `json:"res_status"`
	OptionStatus string    `json:"res_op_status"`
	SerialNum    string    `json:"res_serial_num"`
	Spec         Condition `json:"res_spec"`
}
