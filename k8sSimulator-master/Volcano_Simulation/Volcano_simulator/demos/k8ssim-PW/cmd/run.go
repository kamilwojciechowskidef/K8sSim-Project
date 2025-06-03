package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	// Volcano API → NodeInfo, Resource
	api "volcano.sh/volcano/pkg/scheduler/api"
	// Loader z dnia 2 → PodSpec, Workload
	workload "volcano.sh/volcano/demos/k8ssim-PW/internal/workload"
	// Nasz scheduler BinPack
	"volcano.sh/volcano/demos/k8ssim-PW/internal/scheduler"
)

// PodResult to struktura używana do zapisania wyników (CSV).
type PodResult struct {
	PodName       string
	AssignTimeMs  int64 // czas przydziału
	ReleaseTimeMs int64 // czas zwolnienia (po Lifetime)
	NodeName      string
}

// simEvent pozwala nam kolejkować zdarzenia w kolejności czasowej.
type eventType int

const (
	assignEvent eventType = iota
	releaseEvent
)

type simEvent struct {
	timeMs      int64 // moment zdarzenia (ms od startu symulacji)
	podSpec     *workload.PodSpec
	podName     string
	nodeIndex   int       // indeks węzła w slice nodes, gdy węzeł został przydzielony
	resultIndex int       // indeks w results[], żeby potem wstawić ReleaseTimeMs
	evType      eventType // assignEvent lub releaseEvent

	paired *simEvent // wskaźnik na odpowiadające zdarzenie (assign ↔ release)
}

// indexOf zwraca indeks elementu target w slice nodes.
// Jeśli nie znajdzie, zwraca -1.
func indexOf(nodes []*api.NodeInfo, target *api.NodeInfo) int {
	for i, n := range nodes {
		if n == target {
			return i
		}
	}
	return -1
}

// exportMetricsCSV zapisuje slice PodResult do pliku CSV pod ścieżką path.
func exportMetricsCSV(results []PodResult, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// Nagłówek (dodaliśmy kolumnę release_time_ms)
	if err := w.Write([]string{"pod_name", "assign_time_ms", "release_time_ms", "node_name"}); err != nil {
		return err
	}

	for _, r := range results {
		rec := []string{
			r.PodName,
			fmt.Sprintf("%d", r.AssignTimeMs),
			fmt.Sprintf("%d", r.ReleaseTimeMs),
			r.NodeName,
		}
		w.Write(rec)
	}
	return nil
}

func main() {
	// 1. Parsowanie flag: --workload=<ścieżka> oraz --nodes=<liczba>
	var workloadPath string
	var numNodes int
	flag.StringVar(&workloadPath, "workload", "", "Ścieżka do pliku YAML z workloadem (np. demos/k8ssim-PW/workloads/spread.yaml)")
	flag.IntVar(&numNodes, "nodes", 5, "Liczba węzłów w klastrze (domyślnie 5)")
	flag.Parse()

	if workloadPath == "" {
		fmt.Println("Użycie: go run cmd/run.go --workload=<ścieżka> [--nodes=N]")
		os.Exit(1)
	}

	// 2. Wczytanie workloadu (loader dnia 2)
	wl, err := workload.LoadWorkload(workloadPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Błąd wczytywania workloadu: %v\n", err)
		os.Exit(1)
	}

	// 3. Utworzenie listy węzłów (NodeInfo) z przykładowymi zasobami
	nodes := make([]*api.NodeInfo, numNodes)
	for i := 0; i < numNodes; i++ {
		alloc := api.NewResource(nil)
		alloc.MilliCPU = 1000 // 1000 milicores
		alloc.Memory = 2048   // 2048 MiB

		used := api.EmptyResource()

		nodeInfo := &api.NodeInfo{
			Name:        fmt.Sprintf("node-%d", i),
			Node:        nil,
			Allocatable: alloc,
			Used:        used,
			Idle:        alloc.Clone(),
			Capability:  alloc.Clone(),
			Releasing:   api.EmptyResource(),
			Pipelined:   api.EmptyResource(),
		}
		nodes[i] = nodeInfo
	}

	// Utworzenie BinPack i wyzerowanie użycia zasobów
	binpack := scheduler.NewBinPack(nodes)
	binpack.ResetResourceUsage()

	// 4. Budowanie listy eventów (assign + release), w parach
	var results []PodResult
	var events []simEvent

	for _, ps := range wl.Pods {
		for i := 0; i < ps.Replicas; i++ {
			// 4.1. Tworzymy oddzielną kopię PodSpec:
			psCopy := ps
			podName := fmt.Sprintf("%s-%d", psCopy.Image, i)

			// 4.2. assignEvent – moment, w którym próbujemy przydzielić
			submitMs := psCopy.SubmitAt.Milliseconds()
			evAssign := simEvent{
				timeMs:      submitMs,
				podSpec:     &psCopy,
				podName:     podName,
				nodeIndex:   -1,
				resultIndex: -1,
				evType:      assignEvent,
				paired:      nil,
			}
			events = append(events, evAssign)

			// 4.3. releaseEvent – moment, w którym pod kończy Lifetime
			releaseMs := submitMs + psCopy.Lifetime.Milliseconds()
			evRelease := simEvent{
				timeMs:      releaseMs,
				podSpec:     &psCopy,
				podName:     podName,
				nodeIndex:   -1,
				resultIndex: -1,
				evType:      releaseEvent,
				paired:      nil,
			}
			events = append(events, evRelease)

			// 4.4. Łączymy oba wydarzenia nawzajem wskaźnikami
			assignIdx := len(events) - 2  // indeks evAssign
			releaseIdx := len(events) - 1 // indeks evRelease
			events[assignIdx].paired = &events[releaseIdx]
			events[releaseIdx].paired = &events[assignIdx]
		}
	}

	// 5. Sortowanie eventów rosnąco po timeMs
	sort.Slice(events, func(i, j int) bool {
		return events[i].timeMs < events[j].timeMs
	})

	// 6. Przetwarzanie eventów w kolejności czasowej
	for idx := range events {
		ev := &events[idx]
		switch ev.evType {
		case assignEvent:
			// — DEBUG (opcjonalnie) —
			fmt.Printf("[DEBUG-ASSIGN] Próba assignEvent dla %s (submit_ms=%d)\n", ev.podName, ev.timeMs)

			chosenNode, err := binpack.Schedule(ev.podSpec)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[DEBUG-ASSIGN] node NOT found dla %s: %v\n", ev.podName, err)
				continue
			}
			fmt.Printf("[DEBUG-ASSIGN] Przydzieliłem %s do węzła %s\n", ev.podName, chosenNode.Name)

			// Dodanie rekordu do results i zapis indeksu
			resultIdx := len(results)
			results = append(results, PodResult{
				PodName:       ev.podName,
				AssignTimeMs:  ev.timeMs,
				ReleaseTimeMs: 0,
				NodeName:      chosenNode.Name,
			})
			ev.nodeIndex = indexOf(nodes, chosenNode)
			ev.resultIndex = resultIdx
			fmt.Printf("[DEBUG-ASSIGN] Ustawiono ev.nodeIndex=%d, ev.resultIndex=%d dla %s\n",
				ev.nodeIndex, ev.resultIndex, ev.podName)

			// **Kluczowe**: przekazujemy tę wartość do odpowiadającego releaseEvent
			if ev.paired != nil {
				ev.paired.nodeIndex = ev.nodeIndex
				ev.paired.resultIndex = ev.resultIndex
			}

		case releaseEvent:
			// Debug: czy trafiamy do releaseEvent?
			if ev.nodeIndex < 0 || ev.resultIndex < 0 || ev.resultIndex >= len(results) {
				fmt.Printf("[DEBUG-RELEASE] Pomijam releaseEvent dla %s: nodeIndex=%d, resultIndex=%d\n",
					ev.podName, ev.nodeIndex, ev.resultIndex)
				continue
			}
			fmt.Printf("[DEBUG-RELEASE] Wykonuję releaseEvent dla %s: timeMs=%d, nodeIndex=%d, resultIndex=%d\n",
				ev.podName, ev.timeMs, ev.nodeIndex, ev.resultIndex)

			cpuReq, err1 := strconv.ParseFloat(ev.podSpec.CPU, 64)
			memReq, err2 := strconv.ParseFloat(ev.podSpec.Memory, 64)
			if err1 != nil || err2 != nil {
				continue
			}
			nodeToFree := nodes[ev.nodeIndex]
			nodeToFree.Used.MilliCPU -= cpuReq
			nodeToFree.Used.Memory -= memReq

			// Nadpisujemy ReleaseTimeMs
			results[ev.resultIndex].ReleaseTimeMs = ev.timeMs
		}
	}

	// 7. Zapis wyników do CSV
	csvPath := fmt.Sprintf("metrics/%s_binpack.csv", filepath.Base(workloadPath))
	if err := exportMetricsCSV(results, csvPath); err != nil {
		fmt.Fprintf(os.Stderr, "Błąd zapisu CSV: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Symulacja ukończona – wyniki zapisane w %s\n", csvPath)
}
