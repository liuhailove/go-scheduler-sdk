package gs

import (
	"fmt"
	"testing"
)

func TestRoundBalancer(t *testing.T) {
	balancer := new(RoundBalancer)
	services := []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"}
	for i := 0; i < 100; i++ {
		fmt.Println(balancer.Balance(services))
	}
}
