package model

import (
	"reflect"
	"strings"
)

func indirectShape(input reflect.Type) reflect.Type {
	shape := input
	for shape != nil && shape.Kind() == reflect.Pointer {
		shape = shape.Elem()
	}
	return shape
}
func arrayItem(input reflect.Type) reflect.Type {
	shape := indirectShape(input)
	if shape != nil && (shape.Kind() == reflect.Slice || shape.Kind() == reflect.Array) {
		return shape.Elem()
	}
	return nil
}

// JSON field spelling is exact. encoding/json's case-insensitive fallback would
// otherwise accept unknown aliases and two spellings for the same schema field.
func objectField(input reflect.Type, name string) (reflect.Type, bool) {
	shape := indirectShape(input)
	if shape == nil || shape.Kind() == reflect.Interface {
		return nil, true
	}
	if shape.Kind() == reflect.Map {
		return shape.Elem(), true
	}
	if shape.Kind() != reflect.Struct {
		return nil, false
	}
	if field, ok := directStructField(shape, name); ok {
		return field, true
	}
	return embeddedStructField(shape, name)
}
func directStructField(shape reflect.Type, name string) (reflect.Type, bool) {
	for i := range shape.NumField() {
		field := shape.Field(i)
		if field.PkgPath != "" {
			continue
		}
		tag := strings.SplitN(field.Tag.Get("json"), ",", 2)[0]
		if tag == "-" || field.Anonymous && tag == "" {
			continue
		}
		if tag == "" {
			tag = field.Name
		}
		if tag == name {
			return field.Type, true
		}
	}
	return nil, false
}
func embeddedStructField(shape reflect.Type, name string) (reflect.Type, bool) {
	for i := range shape.NumField() {
		field := shape.Field(i)
		if !field.Anonymous || field.PkgPath != "" || strings.SplitN(field.Tag.Get("json"), ",", 2)[0] != "" {
			continue
		}
		if child, ok := objectField(field.Type, name); ok {
			return child, true
		}
	}
	return nil, false
}
