/*
Copyright 2026 The karpenter-provider-clever-cloud Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package pricing resolves the CKE flavor catalog from Clever Cloud's public,
// unauthenticated HTTP API. It combines the live available-flavor list (per
// topology) and the live per-resource rates with the static sizing seed to
// produce a freshly priced []instancetype.Flavor.
//
// The API exposes flavor NAMES and per-resource RATES but never per-flavor
// cpu/memory sizing, so the sizing comes from instancetype.SizingByName; a
// flavor the API offers but the seed does not know is skipped (it cannot be
// priced or sized).
package pricing

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
)

const (
	// DefaultBaseURL is the public Clever Cloud API root.
	DefaultBaseURL = "https://api.clever-cloud.com"
	// DefaultTopology is the CKE topology whose available-flavor list is used
	// (DISTRIBUTED exposes the full 2XS…XL set).
	DefaultTopology = "DISTRIBUTED"

	productPath     = "/v4/kubernetes-product"
	priceSystemPath = "/v4/billing/price-system"

	serviceVCPU = "kubernetes.node.vcpu"
	serviceRAM  = "kubernetes.node.ram"

	httpTimeout = 15 * time.Second

	// maxResponseBytes caps how much of a response body the decoder reads.
	// Live bodies are a few hundred bytes to ~100 KiB; 1 MiB leaves an order
	// of magnitude of headroom while bounding a misbehaving endpoint.
	maxResponseBytes = 1 << 20

	// rateBoundFactor bounds how far a live rate may drift from the compiled
	// default before the refresh is rejected: an order of magnitude either
	// way accommodates any plausible repricing while catching unit changes
	// and tiered-plan misreads. Deliberately NOT a delta against the
	// last-known-good rate — that state resets on every pod restart, and a
	// delta rule would permanently reject a legitimate larger change with no
	// recovery short of a new release.
	rateBoundFactor = 10
)

// kubernetesProduct mirrors the fields we consume from GET /v4/kubernetes-product.
type kubernetesProduct struct {
	Topologies []productTopology `json:"topologies"`
}

type productTopology struct {
	Topology         string   `json:"topology"`
	AvailableFlavors []string `json:"availableFlavors"`
}

// priceSystem mirrors the fields we consume from
// GET /v4/billing/price-system?zone_id=<region>.
type priceSystem struct {
	ZoneID    string          `json:"zone_id"`
	Currency  string          `json:"currency"`
	Countable []countableItem `json:"countable"`
}

type countableItem struct {
	Service      string       `json:"service"`
	DataUnit     string       `json:"data_unit"`
	DataQuantity dataQuantity `json:"data_quantity_for_price"`
	PricePlans   []pricePlan  `json:"price_plans"`
}

type dataQuantity struct {
	Quantity int64 `json:"quantity"`
}

type pricePlan struct {
	Price float64 `json:"price"`
}

// rates holds the live per-resource prices: EUR per vCPU/hour and EUR per
// nominal GB of RAM/hour (the ram countable is priced per 1e9 B = 1 nominal GB,
// so the value multiplies instancetype nominal GB directly).
type rates struct {
	vcpu float64
	ram  float64
}

// Options configures a pricing Provider. BaseURL is the API root used for both
// endpoints; ProductURL and PriceSystemURL optionally override the full URL of
// an individual endpoint (each defaults to BaseURL + its standard path), so the
// two public APIs can be pointed at different hosts, a proxy, or a mock.
type Options struct {
	BaseURL        string
	ProductURL     string
	PriceSystemURL string
	Region         string
	Topology       string
}

// Provider fetches live flavor availability and rates and resolves them into a
// freshly priced instancetype.Flavor catalog.
type Provider struct {
	productURL     string
	priceSystemURL string
	region         string
	topology       string
	client         *http.Client
}

// NewProvider builds a pricing Provider. Empty Options fields fall back to the
// package defaults (BaseURL, Topology) and the standard endpoint paths; Region
// is the price-system zone_id.
func NewProvider(opts Options) *Provider {
	base := opts.BaseURL
	if base == "" {
		base = DefaultBaseURL
	}
	base = strings.TrimRight(base, "/")
	topology := opts.Topology
	if topology == "" {
		topology = DefaultTopology
	}
	productURL := opts.ProductURL
	if productURL == "" {
		productURL = base + productPath
	}
	priceSystemURL := opts.PriceSystemURL
	if priceSystemURL == "" {
		priceSystemURL = base + priceSystemPath
	}
	return &Provider{
		productURL:     productURL,
		priceSystemURL: priceSystemURL,
		region:         opts.Region,
		topology:       topology,
		client:         &http.Client{Timeout: httpTimeout},
	}
}

// Resolve fetches the live available-flavor list and per-resource rates and
// returns a freshly priced catalog. Flavors offered by the API but absent from
// the static sizing seed are skipped with a warning. Any fetch/parse failure,
// a missing rate, an unknown topology, or an empty result returns an error so
// the caller keeps its last-known-good catalog.
func (p *Provider) Resolve(ctx context.Context) ([]instancetype.Flavor, error) {
	logger := log.FromContext(ctx).WithName("pricing")

	var product kubernetesProduct
	if err := p.getJSON(ctx, p.productURL, nil, &product); err != nil {
		return nil, err
	}
	available, err := flavorsForTopology(&product, p.topology)
	if err != nil {
		return nil, err
	}
	if len(available) == 0 {
		return nil, fmt.Errorf("topology %q exposes no available flavors", p.topology)
	}

	var ps priceSystem
	if err := p.getJSON(ctx, p.priceSystemURL, url.Values{"zone_id": {p.region}}, &ps); err != nil {
		return nil, err
	}
	r, err := extractRates(&ps)
	if err != nil {
		return nil, err
	}

	out := make([]instancetype.Flavor, 0, len(available))
	for _, name := range available {
		seed, ok := instancetype.SizingByName[name]
		if !ok {
			logger.Info("skipping flavor without a static sizing seed; cannot price", "flavor", name)
			continue
		}
		out = append(out, instancetype.Flavor{
			Name:        name,
			CPU:         seed.CPU,
			MemoryKi:    seed.MemoryKi,
			PriceHourly: instancetype.ComputePrice(seed.CPU, seed.NominalGB, r.vcpu, r.ram),
		})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no available flavor had a sizing seed; refusing an empty catalog")
	}
	return out, nil
}

func (p *Provider) getJSON(ctx context.Context, rawURL string, query url.Values, out any) error {
	u := rawURL
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("building request for %s: %w", rawURL, err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", rawURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: unexpected status %d", rawURL, resp.StatusCode)
	}
	// A truncated read fails the decode, which routes to last-known-good.
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(out); err != nil {
		return fmt.Errorf("decoding %s: %w", rawURL, err)
	}
	return nil
}

func flavorsForTopology(product *kubernetesProduct, topology string) ([]string, error) {
	for _, t := range product.Topologies {
		if t.Topology == topology {
			return t.AvailableFlavors, nil
		}
	}
	return nil, fmt.Errorf("topology %q not found in kubernetes-product", topology)
}

// extractRates pulls and validates the two node rates. Every check that
// fails returns an error so the caller keeps its last-known-good catalogue:
// this data feeds consolidation and provisioning decisions, and a
// wrong-but-parseable payload is worse than a stale one.
func extractRates(ps *priceSystem) (rates, error) {
	if ps.Currency != "EUR" {
		return rates{}, fmt.Errorf("price-system currency %q, expected EUR: prices would be mislabeled", ps.Currency)
	}
	var r rates
	var haveVCPU, haveRAM bool
	for _, c := range ps.Countable {
		switch c.Service {
		case serviceVCPU:
			if err := validateCountable(c, "vcpu", 1); err != nil {
				return rates{}, err
			}
			r.vcpu, haveVCPU = c.PricePlans[0].Price, true
		case serviceRAM:
			// The pricing formula multiplies this rate by nominal GB, which
			// only holds while the countable is priced per 1e9 bytes.
			if err := validateCountable(c, "B", 1_000_000_000); err != nil {
				return rates{}, err
			}
			r.ram, haveRAM = c.PricePlans[0].Price, true
		}
	}
	if !haveVCPU || !haveRAM {
		return r, fmt.Errorf("price-system missing rate (vcpu=%t, ram=%t)", haveVCPU, haveRAM)
	}
	if err := checkRateBound("vcpu", r.vcpu, instancetype.DefaultVCPURate); err != nil {
		return rates{}, err
	}
	if err := checkRateBound("ram", r.ram, instancetype.DefaultRAMRate); err != nil {
		return rates{}, err
	}
	return r, nil
}

func validateCountable(c countableItem, wantUnit string, wantQuantity int64) error {
	if len(c.PricePlans) != 1 {
		// Tiered plans whose FIRST tier is free are the platform norm on
		// other services; blindly reading PricePlans[0] would price nodes at
		// zero the day the node services converge on that style.
		return fmt.Errorf("service %s has %d price plans, expected exactly 1: tiered pricing is not understood", c.Service, len(c.PricePlans))
	}
	if c.DataUnit != wantUnit || c.DataQuantity.Quantity != wantQuantity {
		return fmt.Errorf("service %s is priced per %d %q, expected per %d %q: the price formula would be wrong",
			c.Service, c.DataQuantity.Quantity, c.DataUnit, wantQuantity, wantUnit)
	}
	if price := c.PricePlans[0].Price; price <= 0 {
		return fmt.Errorf("service %s has non-positive rate %v", c.Service, price)
	}
	return nil
}

func checkRateBound(name string, rate, defaultRate float64) error {
	if rate < defaultRate/rateBoundFactor || rate > defaultRate*rateBoundFactor {
		return fmt.Errorf("%s rate %v is outside the plausibility band [%v, %v]",
			name, rate, defaultRate/rateBoundFactor, defaultRate*rateBoundFactor)
	}
	return nil
}
