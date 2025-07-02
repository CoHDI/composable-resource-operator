package api

type GetMachineResponse struct {
	Data GetMachineData `json:"data"`
}

type GetMachineData struct {
	Machines []GetMachineItem `json:"machines"`
}

type GetMachineItem struct {
	FabricUUID   string               `json:"fabric_uuid"`
	FabricID     int                  `json:"fabric_id"`
	MachineUUID  string               `json:"mach_uuid"`
	MachineID    int                  `json:"mach_id"`
	MachineName  string               `json:"mach_name"`
	TenantUUID   string               `json:"tenant_uuid"`
	Status       int                  `json:"mach_status"`
	StatusDetail string               `json:"mach_status_detail"`
	Resources    []GetMachineResource `json:"resources"`
}

type GetMachineResource struct {
	ResourceUUID string    `json:"res_uuid"`
	ResourceName string    `json:"res_name"`
	Type         string    `json:"res_type"`
	Status       int       `json:"res_status"`
	OptionStatus string    `json:"res_op_status"`
	SerialNum    string    `json:"res_serial_num"`
	Spec         Condition `json:"res_spec"`
}
