package workload

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// PodSpec odzwierciedla pojedynczy wpis w sekcji „pods:” workloadu YAML.
type PodSpec struct {
	Replicas int           `yaml:"replicas"`
	Image    string        `yaml:"image"`
	CPU      string        `yaml:"cpu"`
	Memory   string        `yaml:"memory"`
	Lifetime time.Duration `yaml:"lifetime"`
	SubmitAt time.Duration `yaml:"submit_at"`
}

// Workload mapuje całą strukturę pliku YAML.
type Workload struct {
	Name   string        `yaml:"workload_name"`
	Length time.Duration `yaml:"simulation_length"`
	Pods   []PodSpec     `yaml:"pods"`
}

// LoadWorkload wczytuje plik YAML i zwraca wypełnioną strukturę Workload.
func LoadWorkload(path string) (*Workload, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var wl Workload
	if err := yaml.Unmarshal(raw, &wl); err != nil {
		return nil, err
	}
	return &wl, nil
}
