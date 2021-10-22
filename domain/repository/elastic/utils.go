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

/*

{
  "query": {
    "bool": {
      "must": [
        {
          "match_phrase": {
            "name.keyword": {
              "query": "吕布"
            }
          }
        },
        {
          "match_phrase": {
            "age": {
              "query": 29
            }
          }
        },
        {
          "range": {
            "age": {
              "gte": 10
            }
          }
        }
      ]
    }
  },
  "from": 0,
  "size": 20,
  "sort": [
    {
      "age": "asc"
    }
  ]
}

*/

func getSort(sortSpecs []*model.SortSpec) []map[string]string {
	var sorts []map[string]string
	for _, spec := range sortSpecs {

		sort := map[string]string{
			spec.Property: string(spec.Type),
		}
		sorts = append(sorts, sort)
	}
	return sorts
}
