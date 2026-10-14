// Package raftlog adapts a caller logger to the upstream Raft logging contract.
package raftlog

import (
	"fmt"
	"log/slog"
)

// Logger adapts slog to the upstream Raft logger interface.
type Logger struct{ Log *slog.Logger }

// Debug logs concatenated arguments at debug level.
func (l Logger) Debug(v ...any) { l.Log.Debug(fmt.Sprint(v...)) }

// Debugf logs formatted arguments at debug level.
func (l Logger) Debugf(f string, v ...any) { l.Log.Debug(fmt.Sprintf(f, v...)) }

// Info logs concatenated arguments at info level.
func (l Logger) Info(v ...any) { l.Log.Info(fmt.Sprint(v...)) }

// Infof logs formatted arguments at info level.
func (l Logger) Infof(f string, v ...any) { l.Log.Info(fmt.Sprintf(f, v...)) }

// Warning logs concatenated arguments at warning level.
func (l Logger) Warning(v ...any) { l.Log.Warn(fmt.Sprint(v...)) }

// Warningf logs formatted arguments at warning level.
func (l Logger) Warningf(f string, v ...any) {
	l.Log.Warn(fmt.Sprintf(f, v...))
}

// Error logs concatenated arguments at error level.
func (l Logger) Error(v ...any) { l.Log.Error(fmt.Sprint(v...)) }

// Errorf logs formatted arguments at error level.
func (l Logger) Errorf(f string, v ...any) { l.Log.Error(fmt.Sprintf(f, v...)) }

// Fatal panics with concatenated arguments to surface a fatal Raft invariant
// failure.
func (Logger) Fatal(v ...any) { panic(fmt.Sprint(v...)) }

// Fatalf panics with formatted arguments to surface a fatal Raft invariant
// failure.
func (Logger) Fatalf(f string, v ...any) { panic(fmt.Sprintf(f, v...)) }

// Panic panics with concatenated arguments.
func (Logger) Panic(v ...any) { panic(fmt.Sprint(v...)) }

// Panicf panics with formatted arguments.
func (Logger) Panicf(f string, v ...any) { panic(fmt.Sprintf(f, v...)) }
