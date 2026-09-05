package nilguard

import "reflect"

// Interface normalizes a value destined for an interface field: when v is a
// non-nil interface wrapping a nil pointer/func/map/slice/chan (the Go
// typed-nil trap), it returns the zero value so `iface == nil` guards fire.
func Interface[T any](v T) T {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		if rv.IsNil() {
			var zero T
			return zero
		}
	}
	return v
}
