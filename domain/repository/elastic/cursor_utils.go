package elastic

import "github.com/xxxmicro/base/domain/model"

func buildCursorSearch(cursorQuery *model.CursorQuery) map[string]interface{} {

	{
		filterDirection := model.FilterType_ES_LT_FILTER
		if cursorQuery.Direction == 1 {
			filterDirection = model.FilterType_ES_GT_FILTER
		}
		cursorQuery.Filters[cursorQuery.CursorSort.Property] = map[string]interface{}{
			string(filterDirection): cursorQuery.Cursor,
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

func buildCursorSort(property string, direction byte) map[string]string {
	if direction == 0 {
		return map[string]string{property: "desc"}
	}
	return map[string]string{property: "asc"}
}
