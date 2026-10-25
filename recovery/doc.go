// Package recovery supplies reports, acceptance values and application-files hooks
// for gquorum permanent-majority-loss recovery. Construct the configuration handle
// through Node.Recovery or standalone gquorum.NewRecovery with NodeOptions.
// The application owns
// maintenance, operator authorization and business validation; Node owns machines.
//
// Recovery requires one complete survivor and creates a fresh lineage. A survivor
// may omit acknowledged writes. Backups stay inside configured roots. Rollback,
// arbitrary source merging and recovery from injected stores are not supported.
package recovery
