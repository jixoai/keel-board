package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/store"
)

// rebuild replaces each named tenant's projection with a replay of its durable
// log, prints the digest, and fails when a report isn't consistent. It never
// writes or deletes an event, so a log is not changed by a rebuild.
//
// The projection a running server serves is in its process; rebuild it there
// with POST /v1/board/admin/projection/rebuild. This command is the cold,
// offline equivalent: it proves every tenant replays and hashes the same.
func rebuild(args []string) error {
	flags := flag.NewFlagSet("board rebuild", flag.ExitOnError)
	dsn := flags.String("dsn", "", "PostgreSQL DSN of the board's log (required)")
	var tenants stringList
	flags.Var(&tenants, "tenant", "a tenant to rebuild (repeatable; default: every tenant in the log)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*dsn) == "" {
		return errors.New("board rebuild needs --dsn; a running server rebuilds through POST /v1/board/admin/projection/rebuild")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	events, closeStore, _, err := openStore(ctx, *dsn)
	if err != nil {
		return err
	}
	defer closeStore()

	selected := []string(tenants)
	if len(selected) == 0 {
		lister, ok := events.(store.TenantStore)
		if !ok {
			return errors.New("board rebuild: this store can't list its tenants; pass --tenant")
		}
		selected, err = lister.Tenants(ctx)
		if err != nil {
			return fmt.Errorf("board rebuild: listing tenants: %w", err)
		}
	}
	reports, err := rebuildLog(ctx, events, selected)
	failed := printReports(reports)
	if err != nil {
		// Print what was rebuilt before the failure: an operator needs the
		// per-tenant digests even when a later tenant's log is broken.
		return err
	}
	if failed > 0 {
		return fmt.Errorf("board rebuild: %d tenant(s) are not consistent; events are intact, so restore the log or head row and retry", failed)
	}
	return nil
}

// printReports writes each tenant's result and returns how many were not
// consistent.
func printReports(reports []log.ProjectionReport) int {
	failed := 0
	for _, report := range reports {
		fmt.Printf("%s: head=%d digest=%s consistent=%t\n", report.Tenant, report.LogHead, report.Digest, report.Consistent)
		for _, problem := range report.Problems {
			fmt.Printf("  - %s\n", problem)
		}
		if len(report.Problems) > 0 {
			failed++
		}
	}
	return failed
}

// rebuildLog rebuilds each tenant on its own board, so one tenant's
// projection is released before the next is loaded. It stops at the first
// failure and returns what it has already rebuilt, including a partial report
// the failing tenant produced.
func rebuildLog(ctx context.Context, events store.EventStore, tenants []string) ([]log.ProjectionReport, error) {
	reports := make([]log.ProjectionReport, 0, len(tenants))
	for _, tenant := range tenants {
		report, err := log.New(log.Options{Store: events}).Rebuild(ctx, tenant)
		if report.Tenant != "" {
			reports = append(reports, report)
		}
		if err != nil {
			return reports, fmt.Errorf("board rebuild: %s: %w", tenant, err)
		}
	}
	return reports, nil
}

// stringList is a repeatable string flag.
type stringList []string

// String implements flag.Value.
func (s *stringList) String() string { return strings.Join(*s, ",") }

// Set implements flag.Value.
func (s *stringList) Set(value string) error {
	if strings.TrimSpace(value) == "" {
		return errors.New("an empty value")
	}
	*s = append(*s, value)
	return nil
}
