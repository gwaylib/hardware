//go:build linux
// +build linux

package bindcpu

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/gwaylib/errors"
)

func GetCacheID(cpuid, level int) (int, error) {
	id, err := ioutil.ReadFile(fmt.Sprintf("/sys/devices/system/cpu/cpu%d/cache/index%d/id", cpuid, level))
	if err != nil {
		return 0, errors.As(err)
	}
	return strconv.Atoi(strings.TrimSpace(string(id)))
}
