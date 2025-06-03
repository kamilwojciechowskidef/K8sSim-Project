package scheduler

import (
	"fmt"
	"strconv"

	// Volcano API, skąd bierzemy NodeInfo i Resource
	api "volcano.sh/volcano/pkg/scheduler/api"
	// PodSpec z loadera (dzień 2)
	workload "volcano.sh/volcano/demos/k8ssim-PW/internal/workload"
)

// BinPack to prosta heurystyka „First-Fit” bazująca na zasobach CPU i pamięci.
type BinPack struct {
	Nodes []*api.NodeInfo
}

// NewBinPack inicjalizuje scheduler z listą węzłów (NodeInfo).
func NewBinPack(nodes []*api.NodeInfo) *BinPack {
	return &BinPack{
		Nodes: nodes,
	}
}

// Schedule próbuje umieścić PodSpec p na pierwszym węźle, który ma
// wystarczająco wolnych zasobów (CPU i pamięć).
// Ponieważ p.CPU i p.Memory są typu string, musimy je sparsować na float64.
func (b *BinPack) Schedule(p *workload.PodSpec) (*api.NodeInfo, error) {
	// Parsowanie zapotrzebowania CPU (string → float64)
	cpuReq, err := strconv.ParseFloat(p.CPU, 64)
	if err != nil {
		return nil, fmt.Errorf("BinPack: niepoprawna wartość CPU dla poda %s: %v", p.Image, err)
	}
	// Parsowanie zapotrzebowania pamięci (string → float64)
	memReq, err := strconv.ParseFloat(p.Memory, 64)
	if err != nil {
		return nil, fmt.Errorf("BinPack: niepoprawna wartość Memory dla poda %s: %v", p.Image, err)
	}

	for _, node := range b.Nodes {
		// Obliczamy dostępne zasoby na tym węźle:
		availableCPU := node.Allocatable.MilliCPU - node.Used.MilliCPU
		availableMem := node.Allocatable.Memory - node.Used.Memory

		// Sprawdzamy, czy PodSpec p zmieści się na tym węźle:
		if cpuReq <= availableCPU && memReq <= availableMem {
			// „Zajmujemy” zasoby (aktualizujemy stan node.Used):
			node.Used.MilliCPU += cpuReq
			node.Used.Memory += memReq
			return node, nil
		}
	}
	return nil, fmt.Errorf("BinPack: brak węzła z wolnymi zasobami dla poda %s", p.Image)
}

// ResetResourceUsage zeruje wartość node.Used.* we wszystkich węzłach,
// co pozwala na „start z czystymi zasobami” przy kolejnym eksperymencie.
func (b *BinPack) ResetResourceUsage() {
	for _, node := range b.Nodes {
		node.Used.MilliCPU = 0
		node.Used.Memory = 0
	}
}
