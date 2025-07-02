package api

type ScaleDownBody struct {
	Tenants ScaleDownTenants `json:"tenants"`
}

type ScaleDownTenants struct {
	TenantUUID string                 `json:"tenant_uuid"`
	Machines   []ScaleDownMachineItem `json:"machines"`
}

type ScaleDownMachineItem struct {
	MachineUUID string                  `json:"mach_uuid"`
	Resources   []ScaleDownResourceItem `json:"resources"`
}

type ScaleDownResourceItem struct {
	ResourceSpecs []ScaleDownResourceSpecItem `json:"res_specs"`
}

type ScaleDownResourceSpecItem struct {
	Type         string `json:"res_type"`
	ResourceUUID string `json:"res_uuid"`
}
