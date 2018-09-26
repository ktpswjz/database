package mysql

import (
	"fmt"
	"reflect"
)

type field struct {
	name          string
	value         interface{}
	address       interface{}
	autoIncrement bool
	primaryKey    bool
	filter        string
	order         string
}

func (s *field) Name() string {
	return s.name
}

func (s *field) Value() interface{} {
	return s.value
}

func (s *field) Address() interface{} {
	return s.address
}

func (s *field) AutoIncrement() bool {
	return s.autoIncrement
}

func (s *field) PrimaryKey() bool {
	return s.primaryKey
}

func (s *field) Filter() string {
	return s.filter
}

func (s *field) Order() string {
	return s.order
}

func (s *field) ValueEmpty() bool {
	if s.value == nil {
		return true
	}
	v := reflect.ValueOf(s.value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.Interface, reflect.Slice:
		if v.IsNil() {
			return true
		}
	}

	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return true
		}
		v = v.Elem()
	}

	ev := fmt.Sprint(v)
	if len(ev) == 0 {
		return true
	}

	return false
}
