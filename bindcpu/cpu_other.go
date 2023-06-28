//go:build !linux
// +build !linux

package bindcpu

func GetCacheID(cpuid, level int) (int, error) {
	return 0, nil
}
