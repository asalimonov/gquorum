// Package raftlog adapts a caller logger to the upstream Raft logging contract.
package raftlog

import (
	"fmt"
	"log/slog"
)

type Logger struct{ Log *slog.Logger }

func (l Logger) Debug(v ...any)              { l.Log.Debug(fmt.Sprint(v...)) }
func (l Logger) Debugf(f string, v ...any)   { l.Log.Debug(fmt.Sprintf(f, v...)) }
func (l Logger) Info(v ...any)               { l.Log.Info(fmt.Sprint(v...)) }
func (l Logger) Infof(f string, v ...any)    { l.Log.Info(fmt.Sprintf(f, v...)) }
func (l Logger) Warning(v ...any)            { l.Log.Warn(fmt.Sprint(v...)) }
func (l Logger) Warningf(f string, v ...any) { l.Log.Warn(fmt.Sprintf(f, v...)) }
func (l Logger) Error(v ...any)              { l.Log.Error(fmt.Sprint(v...)) }
func (l Logger) Errorf(f string, v ...any)   { l.Log.Error(fmt.Sprintf(f, v...)) }
func (_ Logger) Fatal(v ...any)              { panic(fmt.Sprint(v...)) }
func (_ Logger) Fatalf(f string, v ...any)   { panic(fmt.Sprintf(f, v...)) }
func (_ Logger) Panic(v ...any)              { panic(fmt.Sprint(v...)) }
func (_ Logger) Panicf(f string, v ...any)   { panic(fmt.Sprintf(f, v...)) }
