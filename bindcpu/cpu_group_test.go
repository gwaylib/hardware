package bindcpu

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestCPUGroup(t *testing.T) {
	l3Cpus, err := UseL3AllCPU(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(strings.Join(l3Cpus, ","))
}
