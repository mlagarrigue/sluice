package audit

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// Azure, read from what its Instance Metadata Service answered at
// http://169.254.169.254/metadata/instance, saved to a file: the audit
// never calls it. What the comparison needs from it is what makes two
// machines comparable — the size, the region, the zone — published for
// each machine so that a reader can check they were the same.

// Azure is one virtual machine as its metadata describes it.
type Azure struct {
	Path        string
	VMSize      string
	Location    string
	Zone        string
	Environment string
}

func (a *Azure) Name() string { return "azure" }

func (a *Azure) Settings() []Setting {
	orNone := func(v string) string {
		if v == "" {
			return "none"
		}
		return v
	}
	return []Setting{
		{"host azure vm size", a.VMSize},
		{"host azure location", a.Location},
		{"host azure zone", orNone(a.Zone)},
	}
}

func openAzure(path string) (*Azure, error) {
	b, err := os.ReadFile(path) //nolint:gosec // G304: the path is the caller's
	if err != nil {
		return nil, err
	}
	var doc struct {
		Compute *struct {
			VMSize        string `json:"vmSize"`
			Location      string `json:"location"`
			Zone          string `json:"zone"`
			AzEnvironment string `json:"azEnvironment"`
		} `json:"compute"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("%s: not the metadata service's JSON: %w", path, err)
	}
	if doc.Compute == nil || doc.Compute.VMSize == "" {
		return nil, fmt.Errorf("%s: %w", path, errors.New("no compute.vmSize: want the answer of /metadata/instance"))
	}
	c := doc.Compute
	return &Azure{Path: path, VMSize: c.VMSize, Location: c.Location, Zone: c.Zone, Environment: c.AzEnvironment}, nil
}
