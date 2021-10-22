package elastic

import (
	"errors"
	"github.com/xxxmicro/base/domain/model"
	breflect2 "github.com/xxxmicro/base/domain/repository/elastic/reflect"
	breflect "github.com/xxxmicro/base/reflect"
	"reflect"
)

func getModelInfo(m model.Model) (index string, idRefValue reflect.Value, err error) {
	idRefValue, err = breflect.GetStructField(m, "ID")
	if err != nil {
		return
	}

	ms, err := breflect2.GetStructInfo(m, nil)
	if err != nil {
		return index, idRefValue, err
	}
	index = TheNamingStrategy.Table(ms.Name)
	return
}

func getModelInfoAndCheckID(m model.Model) (index string, idRefValue reflect.Value, err error) {
	index, idRefValue, err = getModelInfo(m)
	if err != nil {
		return
	}

	if idRefValue.String() == "" {
		err = errors.New("ID is empty")
	}
	return
}

func buildPageSearch(pageQuery *model.PageQuery) map[string]interface{} {
	query := buildQuery(pageQuery.Filters)
	search := map[string]interface{}{
		"query": query,
		"from":  (pageQuery.PageNo - 1) * pageQuery.PageSize,
		"size":  pageQuery.PageSize,
	}

	sort := buildSort(pageQuery.Sort)
	if sort != nil {
		search["sort"] = sort
	}

	return search
}

// todo 多条件排序好像有问题
func buildSort(sortSpecs []*model.SortSpec) []map[string]string {
	var sorts []map[string]string

	for _, spec := range sortSpecs {
		sort := map[string]string{
			spec.Property: string(spec.Type),
		}
		sorts = append(sorts, sort)
	}

	return sorts
}

func buildQuery(filters map[string]interface{}) map[string]map[string]interface{} {
	var musts []interface{}

	for column, v := range filters {
		must := buildMust(column, v)
		musts = append(musts, must)
	}

	query := map[string]map[string]interface{}{
		"bool": {
			"must": musts,
		},
	}

	return query
}

func buildMust(column string, value interface{}) interface{} {
	switch value.(type) {
	case string:
		must := map[string]map[string]map[string]interface{}{
			"match_phrase": {
				column + ".keyword": {
					"query": value,
				},
			},
		}
		return must
	case map[string]interface{}:
		return buildRange(column, value.(map[string]interface{}))
	default:
		must := map[string]map[string]map[string]interface{}{
			"match_phrase": {
				column: {
					"query": value,
				},
			},
		}
		return must
	}
}

func buildRange(column string, filters map[string]interface{}) interface{} {
	filter := make(map[string]interface{})

	// 每个条件的话， 支持 gt gte lt lte eq  like ne in 这几个即可
	for k, v := range filters {
		filterType := model.FilterType(k)
		switch filterType {
		case model.FilterType_ES_GT_FILTER:
			filter["gt"] = v
		case model.FilterType_ES_GTE_FILTER:
			filter["gte"] = v
		case model.FilterType_ES_LT_FILTER:
			filter["lt"] = v
		case model.FilterType_ES_LTE_FILTER:
			filter["lte"] = v
		case model.FilterType_ES_EQ:
			must := map[string]map[string]map[string]interface{}{
				"match_phrase": {
					column + ".keyword": {
						"query": v,
					},
				},
			}
			return must
		case model.FilterType_ES_NE:
			switch v.(type) {
			case string:
				must := map[string]map[string][]map[string]map[string]map[string]interface{}{
					"bool": {
						"must_not": []map[string]map[string]map[string]interface{}{{
							"match_phrase": {
								column + ".keyword": {
									"query": v,
								},
							},
						}},
					},
				}
				return must
			default:
				must := map[string]map[string][]map[string]map[string]map[string]interface{}{
					"bool": {
						"must_not": []map[string]map[string]map[string]interface{}{{
							"match_phrase": {
								column: {
									"query": v,
								},
							},
						}},
					},
				}
				return must
			}
		case model.FilterType_ES_IN:
			must := map[string]map[string]interface{}{
				"terms": {
					column: v,
				},
			}
			return must
		case model.FilterType_ES_LIKE:
			must := map[string]map[string]map[string]interface{}{
				"match_phrase": {
					column: {
						"query": v,
					},
				},
			}
			return must
		default:

		}
	}

	rangeFilter := map[string]map[string]map[string]interface{}{
		"range": {
			column: filter,
		}}

	return rangeFilter
}

// 这里要在 query.Filters 加入游标的条件
// 要在 query.CursorSort 也加入游标的条件
/*
type CursorQuery struct {
	Filters    map[string]interface{} 	`json:"filters"`    // 筛选条件
	Cursor     interface{}            	`json:"cursor"`     // 游标值
	CursorSort *SortSpec              	`json:"cursorSort"` // 游标字段&排序
	Size       int                  	`json:"size"`       // 数据量
	Direction  byte                   	`json:"direction"`  // 查询方向 0：游标前；1：游标后
}
*/
func buildCursorSort(property string, direction byte) map[string]string {
	if direction == uint8(0) {
		return map[string]string{property: "asc"}
	}
	return map[string]string{property: "desc"}
}

func buildCursorSearch(cursorQuery *model.CursorQuery) map[string]interface{} {

	{
		filterDirection := "LT_FILTER"
		if cursorQuery.Direction == uint8(1) {
			filterDirection = "GT_FILTER"
		}
		cursorQuery.Filters[cursorQuery.CursorSort.Property] = map[string]interface{}{
			filterDirection: cursorQuery.Cursor,
		}
	}

	query := buildQuery(cursorQuery.Filters)
	sort := buildCursorSort(cursorQuery.CursorSort.Property, cursorQuery.Direction)

	search := map[string]interface{}{
		"query": query,
		"size":  cursorQuery.Size,
		"sort":  sort,
	}
	return search

}
