package test

import (
	"sync"
)

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]TestDriver)
)

type TestDriver interface {
	Open()
}

type TestClient interface {
	Connect()
}

func Register(name string, driver TestDriver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic("sql: Register driver is nil")
	}
	if _, dup := drivers[name]; dup {
		panic("sql: Register called twice for driver " + name)
	}
	drivers[name] = driver
}
