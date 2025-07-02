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
