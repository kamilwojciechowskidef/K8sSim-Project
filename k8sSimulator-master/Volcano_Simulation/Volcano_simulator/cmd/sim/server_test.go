// file: Volcano_Simulation/Volcano_simulator/cmd/sim/server_test.go
package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"testing"
	"time"
)

// Ten typ musi odpowiadać dokładnie temu, co oczekuje funkcja reset w main.go:
type workloadPayload struct {
	Period   string `json:"period"`
	Nodes    string `json:"nodes"`
	Workload string `json:"workload"`
}

// Przykładowe "minimalne" YAML-e, które wyślemy w teście.
// Oczywiście w praktyce zamień je na realne pliki/foldery.
var exampleNodesYAML = `
cluster:
  - node:
      metadata:
        name: "n1"
      status:
        allocatable:
          cpu: "2000m"
          memory: "4Gi"
        capacity:
          cpu: "2000m"
          memory: "4Gi"
      spec:
        taints: []
    CtnCreationTime: 2
    CtnCreationExtraTime: 0.5
    CtnCreationTimeInterval: 1
    CalculationSpeed: 1
    MinimumSpeed: -1
    SlowSpeedThreshold: -1
`
var exampleJobsYAML = `
jobs:
  - metadata:
      name: "job1"
      namespace: "default"
      labels:
        sub-time: "0"
    spec:
      tasks:
        - name: "job1-task0"
          namespace: "default"
          containers:
            - name: "c0"
              image: "busybox"
              resources:
                requests:
                  cpu: "500m"
                  memory: "128Mi"
                limits:
                  cpu: "500m"
                  memory: "128Mi"
          labels: {}
          annotations: {}
          Command:
            - "sleep"
            - "10"
`

func TestSimulationEndpoints(t *testing.T) {
	baseURL := "http://localhost:8006"

	// 1) Przygotuj payload JSON do /reset
	pl := workloadPayload{
		Period:   "1",
		Nodes:    exampleNodesYAML,
		Workload: exampleJobsYAML,
	}
	bodyBytes, err := json.Marshal(pl)
	if err != nil {
		t.Fatalf("nie udało się zserializować workloadu: %v", err)
	}

	// 2) Wyślij POST /reset
	resp, err := http.Post(baseURL+"/reset", "application/json", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("błąd w żądaniu POST /reset: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("oczekiwano 200 OK z /reset, otrzymano %d", resp.StatusCode)
	}
	data, _ := ioutil.ReadAll(resp.Body)
	t.Logf("Odpowiedź /reset: %s", string(data))

	// Krótka pauza, by serwer zdążył zainicjować stan
	time.Sleep(200 * time.Millisecond)

	// 3) Wyślij pusty (lub dowolny) POST /step, aby załadować konfigurację scheduler_conf
	schedulerConf := []byte(`{"conf": ""}`)
	stepResp, err := http.Post(baseURL+"/step", "application/json", bytes.NewReader(schedulerConf))
	if err != nil {
		t.Fatalf("błąd w żądaniu POST /step: %v", err)
	}
	defer stepResp.Body.Close()
	if stepResp.StatusCode != http.StatusOK {
		t.Fatalf("oczekiwano 200 OK z /step, otrzymano %d", stepResp.StatusCode)
	}
	stepData, _ := ioutil.ReadAll(stepResp.Body)
	t.Logf("Odpowiedź /step: %s", string(stepData))

	// 4) Wyślij POST /stepResult, żeby pobrać aktualny stan klastra/stack
	resResult, err := http.Post(baseURL+"/stepResult", "application/json", nil)
	if err != nil {
		t.Fatalf("błąd w żądaniu POST /stepResult: %v", err)
	}
	defer resResult.Body.Close()
	if resResult.StatusCode != http.StatusOK {
		t.Fatalf("oczekiwano 200 OK z /stepResult, otrzymano %d", resResult.StatusCode)
	}
	resultData, _ := ioutil.ReadAll(resResult.Body)
	t.Logf("Odpowiedź /stepResult: %s", string(resultData))

	// 5) Możesz także zdeserializować JSON do struktury simulator.Info
	//    i zweryfikować, czy NotCompletion lub Jobs są zgodne z oczekiwaniami.
	//    Na przykład:
	//      var info simulator.Info
	//      if err := json.Unmarshal(resultData, &info); err != nil { t.Fatalf("błąd JSON: %v", err) }
	//      if info.NotCompletion { t.Log("Wciąż są taski do zrobienia") }
}
