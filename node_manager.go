package dtask

func RegisterNode() {
}

type NodeManager struct {
	model IModel
}

func (n *NodeManager) Register(addr string) error {
	n.model.RegisterNode(addr)
	return nil
}
