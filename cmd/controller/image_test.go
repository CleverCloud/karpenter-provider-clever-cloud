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

package main

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

var (
	goDirective = regexp.MustCompile(`(?m)^go (\S+)$`)
	// Dockerfile instructions are case-insensitive and FROM may carry flags
	// (--platform=$BUILDPLATFORM to cross-build); an optional @digest suffix
	// is tolerated so pinning the builder by digest does not touch this test.
	builderImage = regexp.MustCompile(`(?mi)^FROM[ \t]+(?:--\S+[ \t]+)*golang:([^\s@]+)(?:@\S+)?[ \t]+AS[ \t]+builder[ \t]*$`)
)

// builderGoVersion returns the golang image tag of the Dockerfile's builder
// stage.
func builderGoVersion(dockerfile []byte) (string, bool) {
	m := builderImage.FindSubmatch(dockerfile)
	if m == nil {
		return "", false
	}
	return string(m[1]), true
}

// TestImageBuilderMatchesGoDirective pins the release image's toolchain to the
// go directive. CI installs exactly that release (setup-go reads go.mod) and
// govulncheck scans against it, but the shipped binary is built by the
// Dockerfile's builder stage, whose golang image sets GOTOOLCHAIN=local: its
// tag alone picks the standard library that ships. A builder older than the
// go directive only fails `make image` once a release tag is pushed, and
// `go get` raises the go directive by itself when a dependency needs a newer
// release — so a Dependabot bump could pass every PR check and still break
// the next release. Fail here instead, where both CI and the release gate run.
func TestImageBuilderMatchesGoDirective(t *testing.T) {
	root := filepath.Join("..", "..")
	gomod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	dockerfile, err := os.ReadFile(filepath.Join(root, "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}

	goVersion := goDirective.FindSubmatch(gomod)
	if goVersion == nil {
		t.Fatal("go.mod has no go directive")
	}
	builder, ok := builderGoVersion(dockerfile)
	if !ok {
		t.Fatal("found no `FROM [--flags] golang:<version>[@digest] AS builder` stage in the Dockerfile; " +
			"the builder must be the official golang image pinned to the go directive's release")
	}
	if want := string(goVersion[1]); builder != want {
		t.Errorf("Dockerfile builds with golang:%s but go.mod pins go %s; bump both to the same patch release", builder, want)
	}
}

func TestBuilderGoVersion(t *testing.T) {
	for _, tc := range []struct {
		name, dockerfile, want string
		ok                     bool
	}{
		{name: "plain", dockerfile: "FROM golang:1.26.8 AS builder\n", want: "1.26.8", ok: true},
		{name: "digest", dockerfile: "FROM golang:1.26.8@sha256:0123abcd AS builder\n", want: "1.26.8", ok: true},
		{name: "platform flag", dockerfile: "FROM --platform=$BUILDPLATFORM golang:1.26.8 AS builder\n", want: "1.26.8", ok: true},
		{name: "lowercase", dockerfile: "from golang:1.26.8 as builder\n", want: "1.26.8", ok: true},
		{name: "floating tag is reported, not hidden", dockerfile: "FROM golang:1.26 AS builder\n", want: "1.26", ok: true},
		{name: "later stage", dockerfile: "# comment\nARG X\nFROM golang:1.26.8 AS builder\nFROM scratch\n", want: "1.26.8", ok: true},
		{name: "other builder image", dockerfile: "FROM debian:13 AS builder\n", ok: false},
		{name: "unnamed stage", dockerfile: "FROM golang:1.26.8\n", ok: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := builderGoVersion([]byte(tc.dockerfile))
			if ok != tc.ok || got != tc.want {
				t.Errorf("builderGoVersion(%q) = %q, %v; want %q, %v", tc.dockerfile, got, ok, tc.want, tc.ok)
			}
		})
	}
}
