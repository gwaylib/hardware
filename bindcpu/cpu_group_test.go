package bindcpu

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestCPUGroup(t *testing.T) {
	ctx := context.TODO()
	l3Cpu, err := UseL3CPU(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(strings.Join(l3Cpu, ","))

	l3Cpus, err := UseL3AllCPU(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(strings.Join(l3Cpus, ","))
}

func TestCpuAllocate(t *testing.T) {
	ctx := context.TODO()
	groupLen := CpuGroupL3Thread(ctx)
	for i := 0; i < 30; i++ {
		key, val := SyncAllocateCpu(ctx, 0, groupLen, false)
		fmt.Printf("%d, key: %+v, val: %+v\n", groupLen, *key, *val)
	}
}
