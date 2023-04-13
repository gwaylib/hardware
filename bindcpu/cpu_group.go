package bindcpu

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"net"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gwaylib/errors"
	"github.com/gwaylib/log"
	"golang.org/x/sys/unix"
)

type CpuInfo struct {
	Index   int
	Socket  int
	Core    int
	L3Index int
}

type CpuCoreGroup []CpuInfo
type CpuL3Group []CpuCoreGroup
type CpuSocketGroup []CpuL3Group

type CpuGroupInfo struct {
	MaxCoreThreads int
	MaxSocketL3Num int
	MaxL3Cores     int
	Group          []CpuSocketGroup
}

// return total cores and group detail
func groupCpuBySocket(ctx context.Context) (*CpuGroupInfo, error) {
	out, err := exec.CommandContext(ctx, "lscpu", "-p=CPU,SOCKET,CORE,CACHE").CombinedOutput()
	if err != nil {
		return nil, errors.As(err)
	}
	r := csv.NewReader(bytes.NewReader(out))
	r.Comma = ','
	r.Comment = '#'
	records, err := r.ReadAll()
	if err != nil {
		return nil, errors.As(err)
	}

	groupInfo := &CpuGroupInfo{
		Group: []CpuSocketGroup{},
	}
	for _, r := range records {
		if len(r) != 4 {
			return nil, errors.New("error cpu format").As(records)
		}
		cpuIndex, err := strconv.Atoi(r[0])
		if err != nil {
			return nil, errors.As(err)
		}
		socket, err := strconv.Atoi(r[1])
		if err != nil {
			return nil, errors.As(err)
		}
		core, err := strconv.Atoi(r[2])
		if err != nil {
			return nil, errors.As(err)
		}
		caches := strings.Split(r[3], ":")
		if len(caches) < 4 {
			return nil, errors.New("error cache format").As(records)
		}
		l3Index, err := strconv.Atoi(caches[3])
		if err != nil {
			return nil, errors.New("error cache format").As(records)
		}
		cpuInfo := CpuInfo{
			Index:   cpuIndex,
			Socket:  socket,
			Core:    core,
			L3Index: l3Index,
		}

		// select socket group
		socketL3Group := CpuSocketGroup{}
		if socket < len(groupInfo.Group) {
			socketL3Group = groupInfo.Group[socket]
		} else {
			groupInfo.Group = append(groupInfo.Group, socketL3Group)
		}

		// select l3 group
		var l3Group CpuL3Group
		var l3GroupIdx = -1
	l3GroupSearch:
		for i, l3g := range socketL3Group {
			for _, coreg := range l3g {
				for _, info := range coreg {
					if info.L3Index == l3Index {
						l3Group = l3g
						l3GroupIdx = i
						break l3GroupSearch
					}
				}
			}
		}
		if l3Group == nil {
			l3Group = CpuL3Group{}
			socketL3Group = append(socketL3Group, l3Group)
			l3GroupIdx = len(socketL3Group) - 1
			groupInfo.Group[socket] = socketL3Group
		}
		if len(socketL3Group) > groupInfo.MaxSocketL3Num {
			groupInfo.MaxSocketL3Num = len(socketL3Group)
		}

		var coreGroup = CpuCoreGroup{}
		var coreGroupIdx = -1
	coreGroupSearch:
		for i, coreg := range l3Group {
			for _, info := range coreg {
				if info.Core == core {
					coreGroup = coreg
					coreGroupIdx = i
					break coreGroupSearch
				}
			}
		}
		coreGroup = append(coreGroup, cpuInfo)
		if coreGroupIdx < 0 {
			l3Group = append(l3Group, coreGroup)
		} else {
			l3Group[coreGroupIdx] = coreGroup
		}
		socketL3Group[l3GroupIdx] = l3Group
		groupInfo.Group[socket] = socketL3Group
		// update max threads of each core
		if len(coreGroup) > groupInfo.MaxCoreThreads {
			groupInfo.MaxCoreThreads = len(coreGroup)
		}
		if len(l3Group) > groupInfo.MaxL3Cores {
			groupInfo.MaxL3Cores = len(l3Group)
		}
	}
	return groupInfo, nil
}

type CpuAllocateKey struct {
	CoreIndex int
	L3Index   int
}

func (ak *CpuAllocateKey) Key() string {
	return fmt.Sprintf("%d_%d", ak.CoreIndex, ak.L3Index)
}

type CpuAllocateVal struct {
	cpus []int

	allocated bool
	unixConn  net.Conn
}

func NewCpuAllocateVal(cpus []int, unixConn net.Conn) *CpuAllocateVal {
	return &CpuAllocateVal{
		cpus:     cpus,
		unixConn: unixConn,
	}
}

func (val *CpuAllocateVal) CpuStr() []string {
	result := make([]string, len(val.cpus))
	for i, c := range val.cpus {
		result[i] = strconv.Itoa(c)
	}
	return result
}

var (
	cpuSocketGroups *CpuGroupInfo
	cpuGroupSort    [][][]int                     // [core_thread][l3_core][cpu_id]
	cpuAllocatePool = map[string]CpuAllocateVal{} // map[socket_core_l3cache]]bool
	cpuSetLock      = sync.Mutex{}
)

// sort result by thread:
// [0]socket1_core1_thread1,socket2_core1_thread1,socket1_core2_thread1,cocket2_core2_thread1 ...
// [1]socket1_core1_thread2,socket2_core1_thread2,socket1_core2_thread2,cocket2_core2_thread2 ...
// [..]
func sortAllCpu(cpuGroup *CpuGroupInfo) [][][]int {
	result := [][][]int{}
	socketNum := len(cpuGroup.Group)
	for coreIndex := 0; coreIndex < cpuGroup.MaxCoreThreads; coreIndex++ {
		thread := [][]int{}
		for l3Index := 0; l3Index < cpuGroup.MaxSocketL3Num; l3Index++ {
			for socketIndex := 0; socketIndex < socketNum; socketIndex++ {
				socketL3Group := cpuGroup.Group[socketIndex]
				socketL3GroupLen := len(socketL3Group)
				if l3Index >= socketL3GroupLen {
					continue
				}

				l3Group := socketL3Group[l3Index]
				l3GroupLen := len(l3Group)
				if coreIndex >= l3GroupLen {
					continue
				}

				cpus := []int{}
				for _, l3Cores := range l3Group {
					if coreIndex < len(l3Cores) {
						cpus = append(cpus, l3Cores[coreIndex].Index)
					}
				}
				thread = append(thread, cpus)
			}
		}
		result = append(result, thread)
	}
	return result
}

func initCpuGroup(ctx context.Context) error {
	cpuSetLock.Lock()
	defer cpuSetLock.Unlock()
	if cpuSocketGroups == nil {
		groupInfo, err := groupCpuBySocket(ctx)
		if err != nil {
			return errors.As(err)
		}
		cpuGroupSort = sortAllCpu(groupInfo)
		cpuSocketGroups = groupInfo

		//log.Infof("Init cpu group: %+v", cpuGroupSort)
	}
	return nil
}

func CpuThreadNum(ctx context.Context) int {
	if err := initCpuGroup(ctx); err != nil {
		return 0
	}
	cpuSetLock.Lock()
	defer cpuSetLock.Unlock()
	return cpuSocketGroups.MaxCoreThreads
}

func CpuSocketNum(ctx context.Context) int {
	if err := initCpuGroup(ctx); err != nil {
		return 0
	}
	cpuSetLock.Lock()
	defer cpuSetLock.Unlock()
	return len(cpuSocketGroups.Group)
}
func CpuGroupL3Thread(ctx context.Context) int {
	if err := initCpuGroup(ctx); err != nil {
		return 0
	}
	cpuSetLock.Lock()
	defer cpuSetLock.Unlock()
	return len(cpuGroupSort[0])
}

func BindCpuConn(key *CpuAllocateKey, val *CpuAllocateVal) {
	cpuSetLock.Lock()
	defer cpuSetLock.Unlock()
	cpuAllocatePool[key.Key()] = *val
}
func CloseCpuConn(key *CpuAllocateKey) {
	cpuSetLock.Lock()
	defer cpuSetLock.Unlock()

	aKey := key.Key()
	aVal, ok := cpuAllocatePool[aKey]
	if !ok {
		return
	}
	if aVal.unixConn != nil {
		aVal.unixConn.Close()
	}
	aVal.unixConn = nil
	cpuAllocatePool[aKey] = aVal
}

func useL3CPU(ctx context.Context, l3Index int) ([]string, error) {
	// need initCpuGroup by caller

	result := []string{}
	for i := 0; i < len(cpuGroupSort); i++ {
		ak := &CpuAllocateKey{
			CoreIndex: i,
			L3Index:   l3Index,
		}
		aKey := ak.Key()
		aVal, ok := cpuAllocatePool[aKey]
		if !ok {
			aVal = CpuAllocateVal{
				cpus: cpuGroupSort[i][l3Index],
			}
		}
		// new allocation
		aVal.allocated = true
		cpuAllocatePool[aKey] = aVal

		for _, cpu := range aVal.cpus {
			result = append(result, strconv.Itoa(cpu))
		}
	}
	return result, nil
}

func UseL3AllCPU(ctx context.Context) ([]string, error) {
	if err := initCpuGroup(ctx); err != nil {
		return nil, errors.As(err)
	}
	cpuSetLock.Lock()
	defer cpuSetLock.Unlock()

	result := []string{}
	l3Num := len(cpuGroupSort[0])
	for i := 0; i < l3Num; i++ {
		r, err := useL3CPU(ctx, i)
		if err != nil {
			return nil, errors.As(err, i)
		}
		result = append(result, r...)
	}
	return result, nil
}

func UseL3CPU(ctx context.Context, l3Index int) ([]string, error) {
	if err := initCpuGroup(ctx); err != nil {
		return nil, errors.As(err)
	}

	cpuSetLock.Lock()
	if l3Index >= len(cpuGroupSort[0]) {
		cpuSetLock.Unlock()
		//panic(fmt.Sprintf("l3 id out of range :%d,%d", l3Index, len(cpuGroupSort[0])))
		return UseL3AllCPU(ctx)
	}
	defer cpuSetLock.Unlock()

	return useL3CPU(ctx, l3Index)
}

func OrderAllCpu(ctx context.Context, coreIndex, coreLength int, desc bool) (CpuGroupInfo, string, error) {
	if err := initCpuGroup(ctx); err != nil {
		return CpuGroupInfo{}, "", errors.As(err)
	}

	cpuSetLock.Lock()
	defer cpuSetLock.Unlock()

	if coreIndex < 0 {
		return CpuGroupInfo{}, "", errors.New("need 'index' more than zero")
	}
	end := coreIndex + coreLength
	if end > len(cpuGroupSort[0]) {
		return CpuGroupInfo{}, "", errors.New("'index+length' is over than cpu group length")
	}

	buf := bytes.NewBuffer([]byte{})
	if desc {
		start := coreIndex - 1
		for i := 0; i < len(cpuGroupSort); i++ {
			for j := end - 1; j > start; j-- {
				for _, cpu := range cpuGroupSort[i][j] {
					buf.WriteString(fmt.Sprintf(",%d", cpu))
				}
			}
		}
	} else {
		for i := 0; i < len(cpuGroupSort); i++ {
			for j := coreIndex; j < end; j++ {
				for _, cpu := range cpuGroupSort[i][j] {
					buf.WriteString(fmt.Sprintf(",%d", cpu))
				}
			}
		}
	}
	result := buf.Bytes()
	if len(result) == 0 {
		return CpuGroupInfo{}, "", errors.New("no cpu found")
	}
	return *cpuSocketGroups, string(buf.Bytes()[1:]), nil
}

func AllocateCpu(ctx context.Context, l3Index, l3Length int, desc bool) (*CpuAllocateKey, *CpuAllocateVal, error) {
	if err := initCpuGroup(ctx); err != nil {
		return nil, nil, errors.As(err)
	}

	cpuSetLock.Lock()
	defer cpuSetLock.Unlock()
	if l3Index < 0 {
		return nil, nil, errors.New("need 'index' more than zero")
	}
	end := l3Index + l3Length
	if end > len(cpuGroupSort[0]) {
		return nil, nil, errors.New("'index+length' is over than cpu group length")
	}

	if desc {
		for i := 0; i < len(cpuGroupSort); i++ {
			for j := l3Index; j < end; j++ {
				ak := &CpuAllocateKey{
					CoreIndex: i,
					L3Index:   j,
				}
				aKey := ak.Key()
				aVal, ok := cpuAllocatePool[aKey] // aVal is a copy
				if !ok {
					aVal = CpuAllocateVal{
						cpus: cpuGroupSort[i][j],
					}
				}
				if aVal.allocated {
					continue
				}
				// new allocation
				aVal.allocated = true
				cpuAllocatePool[aKey] = aVal
				return ak, &aVal, nil
			}
		}
	} else {
		start := l3Index - 1
		for i := 0; i < len(cpuGroupSort); i++ {
			for j := end - 1; j > start; j-- {
				ak := &CpuAllocateKey{
					CoreIndex: i,
					L3Index:   j,
				}
				aKey := ak.Key()
				aVal, ok := cpuAllocatePool[aKey] // aVal is a copy
				if !ok {
					aVal = CpuAllocateVal{
						cpus: cpuGroupSort[i][j],
					}
				}
				if aVal.allocated {
					continue
				}
				// new allocation
				aVal.allocated = true
				cpuAllocatePool[aKey] = aVal
				return ak, &aVal, nil
			}
		}
	}

	return nil, nil, errors.New("allocate cpu failed").As(len(cpuAllocatePool))
}

func SyncAllocateCpu(ctx context.Context, coreIndex, coreLength int, desc bool) (*CpuAllocateKey, *CpuAllocateVal) {
	for {
		key, group, err := AllocateCpu(ctx, coreIndex, coreLength, desc)
		if err != nil {
			log.Warn("allocate cpu failed, retry 1s later", errors.As(err))
			time.Sleep(1e9)
			continue
		}
		return key, group
	}
}

func ReturnCpu(key *CpuAllocateKey) error {
	cpuSetLock.Lock()
	defer cpuSetLock.Unlock()
	if key == nil {
		return errors.New("key is nil")
	}
	aKey := key.Key()
	aVal, ok := cpuAllocatePool[aKey]
	if !ok {
		return nil
	}
	aVal.allocated = false
	cpuAllocatePool[aKey] = aVal
	return nil
}

// pin golang thread
// TODO: no effect now
func BindCpu(pid int, cpus []int) error {
	// set cpu affinity
	cpuSet := unix.CPUSet{}
	for _, cpu := range cpus {
		cpuSet.Set(cpu)
	}
	// https://github.com/golang/go/issues/11243
	if err := unix.SchedSetaffinity(pid, &cpuSet); err != nil {
		return errors.As(err)
	}
	return nil
}

// pin golang thread
// TODO: no effect now
func BindCpuStr(pid int, cpus []string) error {
	bind := make([]int, len(cpus))
	for i, c := range cpus {
		ci, err := strconv.Atoi(c)
		if err != nil {
			return errors.As(err, cpus, i)
		}
		bind[i] = ci
	}
	return BindCpu(pid, bind)
}

// pin golang thread
// TODO: no effect now
func pinToCPU(cpus []int) error {
	return nil

	runtime.LockOSThread()
	return BindCpu(0, cpus)
}
