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

func buildSearch(pageQuery *model.PageQuery) map[string]interface{} {
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
								column+".keyword": {
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
