package feishusdk

import (
	"reflect"
	"strings"
)

// MapAppValue 将包名映射为更易读的 App 名称。
func MapAppValue(app string) string {
	switch strings.TrimSpace(app) {
	case "com.smile.gifmaker", "com.jiangjia.gif":
		return "快手"
	case "com.tencent.mm", "com.tencent.xin":
		return "微信"
	default:
		return strings.TrimSpace(app)
	}
}

// StructFieldMap 提取结构体中的 string 字段映射，常用于 Feishu 列名映射。
func StructFieldMap(schema any) map[string]string {
	val := reflect.ValueOf(schema)
	if val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil
	}
	typ := val.Type()
	result := make(map[string]string, val.NumField())
	for i := 0; i < val.NumField(); i++ {
		fieldVal := val.Field(i)
		if fieldVal.Kind() != reflect.String {
			continue
		}
		raw := strings.TrimSpace(fieldVal.String())
		if raw == "" {
			continue
		}
		result[typ.Field(i).Name] = raw
	}
	return result
}
