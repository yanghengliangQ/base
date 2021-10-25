package elastic

import "github.com/xxxmicro/base/domain/model"

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
