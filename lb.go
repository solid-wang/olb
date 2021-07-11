package main

type LbAllocator struct {
	Address   string
	backedIPs []string
	Port      []string
}

func (l LbAllocator) CreateLB() bool {
	// todo call olb create api
	return true
}

func (l LbAllocator) DeleteLB() bool {
	// todo get olb api

	// todo delete instance, if exist

	return true
}

func (l LbAllocator) UpdateLB() bool {
	// todo get olb api

	// todo update instance, if exist

	return true
}
