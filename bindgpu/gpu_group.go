package bindgpu

import (
	"context"
	"encoding/xml"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gwaylib/errors"
	"github.com/gwaylib/hardware/bindcpu"
	"github.com/gwaylib/log"
)

var (
	P2L3Num   = 1
	P2CardNum = 1 // update when gpu init
)

func InitGpuGroup(ctx context.Context) {
	initGpuGroup(ctx)
	l3Num, err := strconv.Atoi(os.Getenv("BIND_GPU_CPU_L3_NUM"))
	if err == nil {
		P2L3Num = l3Num
	}
	// prepare the cpus for p2
	// two l3 group for one card
	// the 0 index of l3 group is mixed for ap and p1
	for i := 0; i < P2CardNum; i++ {
		for j := 0; j < P2L3Num; j++ {
			bindcpu.UseL3CPU(context.TODO(), (i*P2L3Num)+j+1)
		}
	}
}

type GpuPci struct {
	PciBus   string `xml:"pci_bus"`
	PciBusID string `xml:"pci_bus_id"`
	// TODO: more infomation
}

func (p *GpuPci) ParseBusId() (int, error) {
	val, err := strconv.ParseInt(p.PciBus, 16, 32)
	if err != nil {
		return 0, err
	}
	return int(val), nil
}

type GpuInfo struct {
	unixLock sync.Mutex `xml:"-"` // fix by bind
	unixConn []net.Conn `xml:"-"` // fix by bind

	UUID string `xml:"uuid"`
	Pci  GpuPci `xml:"pci"`
	// TODO: more infomation
}

func (info *GpuInfo) close(idx int) error {
	if len(info.unixConn) <= idx {
		return nil
	}
	if info.unixConn[idx] != nil {
		info.unixConn[idx].Close()
	}
	info.unixConn[idx] = nil
	return nil
}

func (info *GpuInfo) Close(idx int) error {
	info.unixLock.Lock()
	defer info.unixLock.Unlock()
	return info.close(idx)
}

func (info *GpuInfo) setConn(idx int, conn net.Conn) {
	if info.unixConn == nil {
		info.unixConn = make([]net.Conn, 1)
	}
	info.close(idx)
	info.unixConn[idx] = conn
}

func (info *GpuInfo) SetConn(idx int, conn net.Conn) {
	info.unixLock.Lock()
	defer info.unixLock.Unlock()
	info.setConn(idx, conn)
	return
}

// the return maybe is nil
func (info *GpuInfo) getConn(idx int) net.Conn {
	if len(info.unixConn) <= idx {
		return nil
	}
	return info.unixConn[idx]
}

func (info *GpuInfo) GetConn(idx int) net.Conn {
	info.unixLock.Lock()
	defer info.unixLock.Unlock()
	return info.getConn(idx)
}

func (info *GpuInfo) UniqueID() string {
	uid := strings.Replace(info.UUID, "GPU-", "", 1)
	if len(uid) == 0 {
		return "nogpu"
	}
	return uid

}

type GpuXml struct {
	XMLName xml.Name  `xml:"nvidia_smi_log"`
	Gpu     []GpuInfo `xml:"gpu"`
	// TODO: more infomation
}

func GroupGpu(ctx context.Context) ([]GpuInfo, error) {
	input, err := exec.CommandContext(ctx, "nvidia-smi", "-q", "-x").CombinedOutput()
	if err != nil {
		return nil, errors.As(err)
	}
	output := GpuXml{}
	if err := xml.Unmarshal(input, &output); err != nil {
		return nil, errors.As(err)
	}
	return output.Gpu, nil
}

var (
	gpuGroup []GpuInfo
	gpuKeys  = map[string]string{}
	gpuLock  = sync.Mutex{}
)

type GpuAllocateKey struct {
	Index  int
	Thread int
}

func (g *GpuAllocateKey) Key() string {
	return fmt.Sprintf("%d_%d", g.Index, g.Thread)
}

func initGpuGroup(ctx context.Context) error {
	gpuLock.Lock()
	defer gpuLock.Unlock()
	if gpuGroup == nil {
		group, err := GroupGpu(ctx)
		if err != nil {
			return errors.As(err)
		}
		gpuGroup = group
		P2CardNum = len(group)
		//log.Infof("Init gpu pool:%+v", gpuGroup)
	}

	return nil
}

func init() {
	initGpuGroup(context.TODO())
}

// TODO: limit call frequency
func hasGPU(ctx context.Context) bool {
	gpuLock.Lock()
	defer gpuLock.Unlock()
	gpus, err := GroupGpu(ctx)
	if err != nil {
		return false
	}
	return len(gpus) > 0
}

func needGPU() bool {
	return os.Getenv("BELLMAN_NO_GPU") != "1" && os.Getenv("FIL_PROOFS_GPU_MODE") == "force"
}

func allocateGPU(ctx context.Context) (*GpuAllocateKey, *GpuInfo, error) {
	if err := initGpuGroup(ctx); err != nil {
		log.Warn(errors.As(err))
		//return nil, nil, errors.As(err)
	}

	gpuLock.Lock()
	defer gpuLock.Unlock()
	maxThread := 1
	for i := 0; i < maxThread; i++ {
		// range is a copy
		for j, gpuInfo := range gpuGroup {
			ak := &GpuAllocateKey{
				Index:  j,
				Thread: i,
			}
			key := ak.Key()
			using, _ := gpuKeys[key]
			if len(using) > 0 {
				continue
			}
			gpuKeys[key] = gpuInfo.UniqueID()
			return ak, &gpuGroup[j], nil
		}
	}
	if len(gpuGroup) == 0 || !needGPU() {
		// using cpu when no gpu hardware
		return &GpuAllocateKey{}, &GpuInfo{}, nil
	}
	return nil, nil, errors.New("allocate gpu failed").As(gpuKeys, gpuGroup)
}

func syncAllocateGPU(ctx context.Context) (*GpuAllocateKey, *GpuInfo) {
	for {
		ak, group, err := allocateGPU(ctx)
		if err != nil {
			log.Warn("allocate gpu failed, retry 1s later. ", errors.As(err))
			time.Sleep(1e9)
			continue
		}
		return ak, group
	}
}

func returnGPU(key *GpuAllocateKey) {
	gpuLock.Lock()
	defer gpuLock.Unlock()
	if key == nil {
		return
	}
	gpuKeys[key.Key()] = ""
}

func AssertGPU(ctx context.Context) {
	// assert gpu for release mode
	// only the develop mode don't need gpu
	if needGPU() && !hasGPU(ctx) {
		log.Fatalf("os exit by gpu not found(BELLMAN_NO_GPU=%s, FIL_PROOFS_GPU_MODE=%s)", os.Getenv("BELLMAN_NO_GPU"), os.Getenv("FIL_PROOFS_GPU_MODE"))
		time.Sleep(3e9)
		os.Exit(-1)
	}
}

// bind gpu, if 'NEPTUNE_DEFAULT_GPU_IDX' is set
func BindGPU(ctx context.Context) {
	defaultGPUEnv := os.Getenv("NEPTUNE_DEFAULT_GPU")
	if len(defaultGPUEnv) > 0 {
		// alreay set
		return
	}

	envIdxStr := os.Getenv("NEPTUNE_DEFAULT_GPU_IDX")
	if len(envIdxStr) == 0 {
		// GPU_IDX not set, using default
		return
	}

	envIdx, err := strconv.Atoi(envIdxStr)
	if err != nil {
		log.Warn(errors.As(err, envIdxStr))
		return
	}
	if err := initGpuGroup(ctx); err != nil {
		log.Warn(errors.As(err, envIdxStr))
		return
	}

	gpuLock.Lock()
	defer gpuLock.Unlock()
	if len(gpuGroup) <= envIdx {
		log.Warn(errors.New("NEPTUNE_DEFAULT_GPU_IDX is out of gpu pool"))
		return
	}
	gpuInfo := gpuGroup[envIdx]

	os.Setenv("NEPTUNE_DEFAULT_GPU", gpuInfo.UniqueID())
	// https://filecoinproject.slack.com/archives/CR98WERRN/p1672367336271889
	if len(os.Getenv("NVIDIA_VISIBLE_DEVICES")) == 0 {
		os.Setenv("NVIDIA_VISIBLE_DEVICES", envIdxStr)
	}
	if len(os.Getenv("CUDA_VISIBLE_DEVICES")) == 0 {
		os.Setenv("CUDA_VISIBLE_DEVICES", envIdxStr)
	}
}
