package sentinel

// SlaveInstance 从节点实例
type SlaveInstance struct {
	ID     string
	Addr   string
	Master *MasterInstance
	Flags  []string
	State  string
	Offset int64
	Lag    int64
}

// NewSlaveInstance 创建新的从节点实例
func NewSlaveInstance(id, addr string) *SlaveInstance {
	return &SlaveInstance{
		ID:    id,
		Addr:  addr,
		Flags: []string{"slave"},
		State: "online",
	}
}
