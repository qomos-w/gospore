// Package integration hosts gospore's end-to-end test programs.
//
// Per ARCHITECTURE.md §3 this directory is the home of cross-package
// integration tests — exercising App boot, actor tree spawn, real
// Invoke + Watch flows, and Phase-2 transport scenarios. Unit tests
// live next to the package they test; integration's role is the
// system-shape interaction tests that no single package can host on
// its own.
//
// This file is the package marker only; real *_test.go files land
// here as the corresponding feature surfaces graduate from skeleton
// to implementation.
package integration
