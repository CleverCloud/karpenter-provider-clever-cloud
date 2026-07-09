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

package pricing_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/instancetype"
	"github.com/CleverCloud/karpenter-provider-clever-cloud/pkg/providers/pricing"
)

// productJSON is the canned /v4/kubernetes-product response. extraFlavor, when
// non-empty, is appended to the DISTRIBUTED topology to exercise the skip path.
func productJSON(extraFlavor string) string {
	distributed := `"2XS","XS","S","M","L","XL"`
	if extraFlavor != "" {
		distributed += `,"` + extraFlavor + `"`
	}
	return `{
      "topologies": [
        {"topology": "ALL_IN_ONE", "availableFlavors": ["S","M","L","XL"]},
        {"topology": "DISTRIBUTED", "availableFlavors": [` + distributed + `]}
      ],
      "versions": {"available": ["1.36"], "default": "1.36"}
    }`
}

// priceSystemJSON is the canned /v4/billing/price-system response with the live
// public rates that reproduce the documented catalog prices exactly.
const priceSystemJSON = `{
  "zone_id": "par",
  "currency": "EUR",
  "countable": [
    {"service": "kubernetes.node.vcpu", "data_unit": "vcpu", "data_quantity_for_price": {"quantity": 1}, "price_plans": [{"price": 0.00277777777}]},
    {"service": "kubernetes.node.ram", "data_unit": "B", "data_quantity_for_price": {"quantity": 1000000000}, "price_plans": [{"price": 0.00555555555}]},
    {"service": "kubernetes.controlPlane.distributed.vcpu", "price_plans": [{"price": 0.01}]}
  ]
}`

type handlerConfig struct {
	productStatus int
	productBody   string
	priceStatus   int
	priceBody     string
	wantZoneID    string
	t             *testing.T
}

func newProvider(baseURL, topology string) *pricing.Provider {
	return pricing.NewProvider(pricing.Options{BaseURL: baseURL, Region: "par", Topology: topology})
}

func newServer(cfg handlerConfig) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v4/kubernetes-product":
			if cfg.productStatus != 0 {
				w.WriteHeader(cfg.productStatus)
			}
			_, _ = w.Write([]byte(cfg.productBody))
		case "/v4/billing/price-system":
			if cfg.wantZoneID != "" {
				if got := r.URL.Query().Get("zone_id"); got != cfg.wantZoneID {
					cfg.t.Errorf("price-system zone_id = %q, want %q", got, cfg.wantZoneID)
				}
			}
			if cfg.priceStatus != 0 {
				w.WriteHeader(cfg.priceStatus)
			}
			_, _ = w.Write([]byte(cfg.priceBody))
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestResolveReproducesDefaultPrices(t *testing.T) {
	srv := newServer(handlerConfig{
		productBody: productJSON(""),
		priceBody:   priceSystemJSON,
		wantZoneID:  "par",
		t:           t,
	})
	defer srv.Close()

	flavors, err := newProvider(srv.URL, "DISTRIBUTED").Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(flavors) != len(instancetype.DefaultFlavors) {
		t.Fatalf("expected %d flavors, got %d", len(instancetype.DefaultFlavors), len(flavors))
	}
	got := map[string]instancetype.Flavor{}
	for _, f := range flavors {
		got[f.Name] = f
	}
	for _, want := range instancetype.DefaultFlavors {
		f, ok := got[want.Name]
		if !ok {
			t.Errorf("flavor %s missing from resolved catalog", want.Name)
			continue
		}
		if f.PriceHourly != want.PriceHourly {
			t.Errorf("flavor %s: price %v, want %v", want.Name, f.PriceHourly, want.PriceHourly)
		}
		if f.CPU != want.CPU || f.MemoryKi != want.MemoryKi {
			t.Errorf("flavor %s: sizing %d/%d, want %d/%d", want.Name, f.CPU, f.MemoryKi, want.CPU, want.MemoryKi)
		}
	}
}

func TestResolveSkipsUnknownFlavor(t *testing.T) {
	srv := newServer(handlerConfig{
		productBody: productJSON("3XL"), // 3XL has no sizing seed
		priceBody:   priceSystemJSON,
		t:           t,
	})
	defer srv.Close()

	flavors, err := newProvider(srv.URL, "DISTRIBUTED").Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	for _, f := range flavors {
		if f.Name == "3XL" {
			t.Fatalf("unknown flavor 3XL must be skipped, got %+v", f)
		}
	}
	if len(flavors) != len(instancetype.DefaultFlavors) {
		t.Errorf("expected the %d seeded flavors, got %d", len(instancetype.DefaultFlavors), len(flavors))
	}
}

func TestResolveUsesConfiguredTopology(t *testing.T) {
	srv := newServer(handlerConfig{productBody: productJSON(""), priceBody: priceSystemJSON, t: t})
	defer srv.Close()

	// ALL_IN_ONE omits 2XS and XS.
	flavors, err := newProvider(srv.URL, "ALL_IN_ONE").Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	for _, f := range flavors {
		if f.Name == "2XS" || f.Name == "XS" {
			t.Errorf("ALL_IN_ONE must not expose %s", f.Name)
		}
	}
	if len(flavors) != 4 {
		t.Errorf("expected 4 ALL_IN_ONE flavors, got %d", len(flavors))
	}
}

// vcpuPlan is the vcpu countable's price_plans fragment in priceSystemJSON;
// priceSystemVariant swaps it for a variant to exercise one validation rule.
const vcpuPlan = `"price_plans": [{"price": 0.00277777777}]`

func priceSystemVariant(replacement, original string) string {
	return strings.Replace(priceSystemJSON, original, replacement, 1)
}

func TestResolveErrors(t *testing.T) {
	cases := map[string]handlerConfig{
		"product 500":        {productStatus: http.StatusInternalServerError, productBody: "", priceBody: priceSystemJSON},
		"price 500":          {productBody: productJSON(""), priceStatus: http.StatusInternalServerError, priceBody: ""},
		"malformed product":  {productBody: "{not json", priceBody: priceSystemJSON},
		"rates missing":      {productBody: productJSON(""), priceBody: `{"zone_id":"par","currency":"EUR","countable":[]}`},
		"empty availability": {productBody: `{"topologies":[{"topology":"DISTRIBUTED","availableFlavors":[]}]}`, priceBody: priceSystemJSON},
		"unknown topology":   {productBody: `{"topologies":[{"topology":"ALL_IN_ONE","availableFlavors":["S"]}]}`, priceBody: priceSystemJSON},
		// Validation of bad-but-parseable payloads: every rejection keeps the
		// last-known-good catalogue via the same error path.
		"zero rate":          {productBody: productJSON(""), priceBody: priceSystemVariant(`"price_plans": [{"price": 0}]`, vcpuPlan)},
		"negative rate":      {productBody: productJSON(""), priceBody: priceSystemVariant(`"price_plans": [{"price": -0.002}]`, vcpuPlan)},
		"absurdly high rate": {productBody: productJSON(""), priceBody: priceSystemVariant(`"price_plans": [{"price": 2.77}]`, vcpuPlan)},
		"absurdly low rate":  {productBody: productJSON(""), priceBody: priceSystemVariant(`"price_plans": [{"price": 0.0000027}]`, vcpuPlan)},
		"tiered price plans": {productBody: productJSON(""), priceBody: priceSystemVariant(`"price_plans": [{"price": 0}, {"price": 0.00277777777}]`, vcpuPlan)},
		"wrong currency":     {productBody: productJSON(""), priceBody: strings.Replace(priceSystemJSON, `"currency": "EUR"`, `"currency": "USD"`, 1)},
		"wrong ram unit":     {productBody: productJSON(""), priceBody: strings.Replace(priceSystemJSON, `"data_unit": "B"`, `"data_unit": "MiB"`, 1)},
		"wrong ram quantity": {productBody: productJSON(""), priceBody: strings.Replace(priceSystemJSON, `{"quantity": 1000000000}`, `{"quantity": 1000000}`, 1)},
		"oversized body":     {productBody: productJSON(""), priceBody: `{"padding": "` + strings.Repeat("x", 2<<20) + `"}`},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			cfg.t = t
			srv := newServer(cfg)
			defer srv.Close()

			topology := "DISTRIBUTED"
			flavors, err := newProvider(srv.URL, topology).Resolve(context.Background())
			if err == nil {
				t.Fatalf("expected error, got %d flavors", len(flavors))
			}
			if flavors != nil {
				t.Errorf("expected nil catalog on error, got %v", flavors)
			}
		})
	}
}

func TestResolveUsesPerEndpointURLOverrides(t *testing.T) {
	// The two endpoints are served under non-standard paths; the default
	// /v4/... paths return 500. Resolve must succeed only by honouring the
	// per-endpoint overrides.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/proxy/product":
			_, _ = w.Write([]byte(productJSON("")))
		case "/proxy/price":
			if got := r.URL.Query().Get("zone_id"); got != "par" {
				t.Errorf("price-system zone_id = %q, want par", got)
			}
			_, _ = w.Write([]byte(priceSystemJSON))
		default:
			http.Error(w, "default path must not be used", http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	p := pricing.NewProvider(pricing.Options{
		BaseURL:        srv.URL,
		ProductURL:     srv.URL + "/proxy/product",
		PriceSystemURL: srv.URL + "/proxy/price",
		Region:         "par",
		Topology:       "DISTRIBUTED",
	})
	flavors, err := p.Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(flavors) != len(instancetype.DefaultFlavors) {
		t.Errorf("expected %d flavors via overridden endpoints, got %d", len(instancetype.DefaultFlavors), len(flavors))
	}
}

func TestResolveAllUnknownFlavorsErrors(t *testing.T) {
	// A topology whose flavors all lack a sizing seed must error rather than
	// publish an empty catalog.
	srv := newServer(handlerConfig{
		productBody: `{"topologies":[{"topology":"DISTRIBUTED","availableFlavors":["3XL","4XL"]}]}`,
		priceBody:   priceSystemJSON,
		t:           t,
	})
	defer srv.Close()

	if _, err := newProvider(srv.URL, "DISTRIBUTED").Resolve(context.Background()); err == nil {
		t.Fatal("expected error when no available flavor has a sizing seed")
	}
}
