package elastic

import (
	"encoding/json"
	"fmt"
	"github.com/xxxmicro/base/domain/model"
	"testing"
	"time"
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

func TestBuildPageSearch(t *testing.T) {
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

	searchMap := buildPageSearch(pageQuery)
	str, err := json.Marshal(searchMap)

	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("map to json  :   ", string(str))

}

func TestBuildCursorSearch(t *testing.T) {
	h, _ := time.ParseDuration("1s")
	t1 := time.Now().Add(h)
	cursor := t1.UnixNano() / 1e6

	cursorQuery := &model.CursorQuery{
		Filters: map[string]interface{}{
			"name": map[string]interface{}{
				"NE": "2",
			},
		},
		CursorSort: &model.SortSpec{
			Property: "ctime",
		},
		Cursor: cursor,
		Size: 10,
	}

	searchMap := buildCursorSearch(cursorQuery)
	str, err := json.Marshal(searchMap)

	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("map to json  :   ", string(str))

}