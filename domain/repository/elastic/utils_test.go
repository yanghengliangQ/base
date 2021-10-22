package elastic

import (
	"encoding/json"
	"fmt"
	"github.com/xxxmicro/base/domain/model"
	"testing"
)

func TestGetSort(t *testing.T) {
	sortSpecs := []*model.SortSpec{&model.SortSpec{Property: "name", Type: "desc"}, &model.SortSpec{Property: "age", Type: "asc"}}

	sorts := buildSort(sortSpecs)

	t.Log(sorts)

}

func TestGetQuery(t *testing.T) {
	pageQuery := &model.PageQuery{
		Filters: map[string]interface{}{
			"name": "吕布",
			"age": map[string]interface{}{
				"GT_FILTER": 22,
			},
		},
		PageSize: 10,
		PageNo:   1,
	}

	queryMap := buildQuery(pageQuery.Filters)
	str, err := json.Marshal(queryMap)

	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("map to json  :   ", string(str))

}

func TestGetSearch(t *testing.T) {
	pageQuery := &model.PageQuery{
		Filters: map[string]interface{}{
			"name": map[string]interface{}{
				"NE": "2",
			},
		},
		PageSize: 10,
		PageNo:   1,
		Sort: []*model.SortSpec{{
			Property:   "age",
			Type:       "asc",
			IgnoreCase: false,
		}},
	}

	searchMap := buildSearch(pageQuery)
	str, err := json.Marshal(searchMap)

	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("map to json  :   ", string(str))

}
