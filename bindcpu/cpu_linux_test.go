//go:build linux
// +build linux

package bindcpu

import (
	"fmt"
	"testing"

	"github.com/shirou/gopsutil/cpu"
)

func TestGetCacheID(t *testing.T) {
	infos, err := cpu.Info() //总体信息
	if err != nil {
		t.Fatal(err)
	}
	// lscpu -p=CPU,SOCKET,CORE,CACHE
	fmt.Println("CPU,SOCKET,CORE,CACHE ")
	for _, info := range infos {
		l0Cache, err := GetCacheID(int(info.CPU), 0)
		if err != nil {
			t.Fatal(err)
		}
		l1Cache, err := GetCacheID(int(info.CPU), 1)
		if err != nil {
			t.Fatal(err)
		}
		l2Cache, err := GetCacheID(int(info.CPU), 2)
		if err != nil {
			t.Fatal(err)
		}
		l3Cache, err := GetCacheID(int(info.CPU), 3)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("%d,%s,%s,%d:%d:%d:%d\n", info.CPU, info.PhysicalID, info.CoreID, l0Cache, l1Cache, l2Cache, l3Cache)
	}
}
