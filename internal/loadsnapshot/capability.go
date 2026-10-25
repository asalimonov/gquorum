package loadsnapshot

import (
	"reflect"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
)

// RequireTransport checks capability without calling application methods.
// Constructor enforcement is enabled only after adapter migration (task 15).
func RequireTransport(transport load.Transport) (snapshot.Transport, error) {
	if isNilTransport(transport) {
		return nil, &snapshot.TransferError{Reason: snapshot.ReasonUnsupported}
	}
	capable, ok := transport.(snapshot.Transport)
	if !ok {
		return nil, &snapshot.TransferError{Reason: snapshot.ReasonUnsupported}
	}
	return capable, nil
}

func isNilTransport(transport any) bool {
	if transport == nil {
		return true
	}
	value := reflect.ValueOf(transport)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
