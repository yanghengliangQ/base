package elastic

import (
	"github.com/xxxmicro/base/domain/model"
	"testing"
)


func TestGetSort(t *testing.T) {
	sortSpecs := []*model.SortSpec{&model.SortSpec{Property: "name", Type: "desc"},&model.SortSpec{Property: "age", Type: "asc"}}

	sorts := getSort(sortSpecs)

	t.Log(sorts)

}