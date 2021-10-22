package model

type CursorQuery struct {
	Filters    map[string]interface{} 	`json:"filters"`    // 筛选条件
	Cursor     interface{}            	`json:"cursor"`     // 游标值
	CursorSort *SortSpec              	`json:"cursorSort"` // 游标字段&排序
	Size       int                  	`json:"size"`       // 数据量
	Direction  byte                   	`json:"direction"`  // 查询方向 0：游标前；1：游标后
}

type CursorList struct {
	Extra CursorExtra			`json:"extra"`
	Items   interface{} 		`json:"items"`   // 数据列表指针
}

type CursorExtra struct {
	Direction byte        		`json:"direction"` // 查询方向 0：游标前；1：游标后
	Size      int       		`json:"size"`      // 数据量
	HasMore   bool        		`json:"hasMore"`   // 是否有更多数据
	MaxCursor interface{} 		`json:"maxCursor"` // 结果集中的起始游标值
	MinCursor interface{} 		`json:"minCursor"` // 结果集中的结束游标值
}

/*
1、游标和查询方向、筛选条件作为查询的条件， 优先以游标排序， 如果还有其他排序方式往后加
2、取出数据量对应条数据
3、返回数据中除了查询的数据本身，需要包含一些基本信息
 */