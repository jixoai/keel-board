package automation

import "reflect"

// deepCopyValue detaches a value from its original: maps, slices, pointers,
// interfaces, and exported struct fields are copied recursively. It is used
// for the read-only values an action receives; an action must not be able to
// mutate a delivery's event, message, or watcher through a shared pointer.
func deepCopyValue(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.New(value.Type()).Elem()
		out.Set(deepCopyValue(value.Elem()))
		return out
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.New(value.Type().Elem())
		out.Elem().Set(deepCopyValue(value.Elem()))
		return out
	case reflect.Struct:
		out := reflect.New(value.Type()).Elem()
		out.Set(value)
		for index := 0; index < value.NumField(); index++ {
			field := out.Field(index)
			if !field.CanSet() || !value.Field(index).CanInterface() {
				continue
			}
			field.Set(deepCopyValue(value.Field(index)))
		}
		return out
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.MakeMapWithSize(value.Type(), value.Len())
		iter := value.MapRange()
		for iter.Next() {
			out.SetMapIndex(deepCopyValue(iter.Key()), deepCopyValue(iter.Value()))
		}
		return out
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for index := 0; index < value.Len(); index++ {
			out.Index(index).Set(deepCopyValue(value.Index(index)))
		}
		return out
	case reflect.Array:
		out := reflect.New(value.Type()).Elem()
		for index := 0; index < value.Len(); index++ {
			out.Index(index).Set(deepCopyValue(value.Index(index)))
		}
		return out
	default:
		return value
	}
}

func deepCopy[T any](value T) T {
	return deepCopyValue(reflect.ValueOf(value)).Interface().(T)
}
