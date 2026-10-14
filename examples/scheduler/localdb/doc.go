// Package localdb persists one scheduler replica using SQLite and XORM.
// Machine implements the public gquorum state-machine contracts. Only Apply and
// Restore change application data; the main package cannot access its database.
// Explicit schema DDL, row constraints and identity markers protect recovery.
package localdb
