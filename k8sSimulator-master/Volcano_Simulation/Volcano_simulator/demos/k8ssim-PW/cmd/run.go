package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	// Volcano API → NodeInfo
	api "volcano.sh/volcano/pkg/scheduler/api"
	// Loader z dnia 2 → PodSpec, Workload
	workload "volcano.sh/volcano/demos/k8ssim-PW/internal/workload"
	// Nasz scheduler binpack
	"volcano.sh/volcano/demos/k8ssim-PW/internal/scheduler"
)

// PodResult to struktura używana do zapisania wyników (CSV).
type PodResult struct {
	PodName      string
	AssignTimeMs int64 // czas przydziału: ms od startu symulacji
	NodeName     string
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

	// Nagłówek
	if err := w.Write([]string{"pod_name", "assign_time_ms", "node_name"}); err != nil {
		return err
	}

	for _, r := range results {
		rec := []string{
			r.PodName,
			fmt.Sprintf("%d", r.AssignTimeMs),
			r.NodeName,
		}
		w.Write(rec)
	}
	return nil
}

func main() {
	// 1. Parsowanie flag: --workload=<ścieżka_do_yaml> oraz --nodes=<ilość_węzłów>
	var workloadPath string
	var numNodes int
	flag.StringVar(&workloadPath, "workload", "", "Ścieżka do pliku YAML z workloadem (np. demos/k8ssim-PW/workloads/burst.yaml)")
	flag.IntVar(&numNodes, "nodes", 5, "Liczba węzłów w klastrze (domyślnie 5)")
	flag.Parse()

	if workloadPath == "" {
		fmt.Println("Użycie: go run cmd/run.go --workload=<ścieżka> [--nodes=N]")
		os.Exit(1)
	}

	// 2. Wczytanie workloadu (działamy loaderem z dnia 2)
	wl, err := workload.LoadWorkload(workloadPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Błąd wczytywania workloadu: %v\n", err)
		os.Exit(1)
	}

	// 3. Utworzenie listy węzłów (NodeInfo), każdy z przydzielonymi, przykładowymi zasobami
	//    (np. 1000 milicores CPU i 2048 MiB pamięci). Każdy węzeł to *api.NodeInfo.
	nodes := make([]*api.NodeInfo, numNodes)
	for i := 0; i < numNodes; i++ {
		// Tworzymy "pusty" Resource, a następnie wypełniamy pola na podstawie
		// przyjętych założeń. Możemy skorzystać z konstruktora NewResource(nil)
		// i nadpisać pola Manualnie, albo zbudować ręcznie.
		alloc := api.NewResource(nil)
		alloc.MilliCPU = 1000 // 1000 milicores
		alloc.Memory = 2048   // 2048 MiB

		used := api.EmptyResource()

		// NodeInfo wymaga podania obiektu *v1.Node, ale do naszej lekkiej symulacji
		// wystarczy nadać mu nil i ręcznie przypisać pola Allocatable/Used:
		nodeInfo := &api.NodeInfo{
			Name:        fmt.Sprintf("node-%d", i),
			Node:        nil, // nie operujemy na prawdziwym obiekcie K8s
			Allocatable: alloc,
			Used:        used,
			// Pozostałe pola wykorzystamy tylko do schedulera, więc ustawiamy:
			Idle:       alloc.Clone(), // po starcie wszystkie zasoby są wolne
			Capability: alloc.Clone(),
			Releasing:  api.EmptyResource(),
			Pipelined:  api.EmptyResource(),
		}
		nodes[i] = nodeInfo
	}

	// 4. Utworzenie instancji BinPack i zresetowanie stanu zasobów (na wszelki wypadek)
	binpack := scheduler.NewBinPack(nodes)
	binpack.ResetResourceUsage()

	// 5. „Symulacja”:
	//    Dla każdego PodSpec w wl.Pods wykonujemy tyle instancji, ile wskazuje Replicas.
	//    Każdy instancja otrzymuje unikalną nazwę (image + indeks) oraz mierzymy czas
	//    przydziału w milisekundach od momentu startu programu.
	startTime := time.Now()
	var results []PodResult

	for _, ps := range wl.Pods {
		for i := 0; i < ps.Replicas; i++ {
			// Generujemy unikalną nazwę: image-n
			podName := fmt.Sprintf("%s-%d", ps.Image, i)

			// Ile już minęło od startu w ms:
			elapsed := time.Since(startTime)
			assignMs := elapsed.Milliseconds()

			// Próba przydziału:
			node, err := binpack.Schedule(&ps)
			if err != nil {
				// Brak dostępnych zasobów: wypisz ostrzeżenie i pomiń ten pod
				fmt.Fprintf(os.Stderr, "UWAGA: nie udało się przydzielić poda %s: %v\n", podName, err)
				continue
			}

			// Zapisujemy wynik:
			results = append(results, PodResult{
				PodName:      podName,
				AssignTimeMs: assignMs,
				NodeName:     node.Name,
			})
		}
	}

	// 6. Zapis wyników do CSV
	csvPath := fmt.Sprintf("metrics/%s_binpack.csv", filepath.Base(workloadPath))
	if err := exportMetricsCSV(results, csvPath); err != nil {
		fmt.Fprintf(os.Stderr, "Błąd zapisu CSV: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Symulacja ukończona – wyniki zapisane w %s\n", csvPath)
}
