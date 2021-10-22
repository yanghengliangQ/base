package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	elasticsearch6 "github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"github.com/xxxmicro/base/domain/model"
	"github.com/xxxmicro/base/domain/repository"
	breflect "github.com/xxxmicro/base/reflect"
	"gopkg.in/mgo.v2/bson"
)

type BaseRepository struct {
	DB *elasticsearch6.Client
}

func NewBaseRepository(db *elasticsearch6.Client) repository.BaseRepository {
	return &BaseRepository{db}
}

// todo 时间要加时区

func (r *BaseRepository) Create(c context.Context, m model.Model) error {
	index, idRefValue, err := getModelInfo(m)
	if err != nil {
		return err
	}

	if idRefValue.String() == "" {
		idRefValue.SetString(bson.NewObjectId().Hex())
	}

	jsonBody, err := json.Marshal(m)
	if err != nil {
		return err
	}

	req := esapi.CreateRequest{
		Index:        index,
		DocumentType: index,
		DocumentID:   idRefValue.String(),
		Body:         bytes.NewReader(jsonBody),
	}
	res, err := req.Do(c, r.DB)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	fmt.Println(res.String())
	return nil
}

func (r *BaseRepository) Exists(c context.Context, index string, documentID string) (bool, error) {
	req := esapi.ExistsRequest{
		Index:        index,
		DocumentType: index,
		DocumentID:   documentID,
	}

	res, err := req.Do(c, r.DB)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		return true, nil
	}
	return false, nil
}

func (r *BaseRepository) Upsert(c context.Context, m model.Model) (*repository.ChangeInfo, error) {
	index, idRefValue, err := getModelInfo(m)
	if err != nil {
		return nil, err
	}

	if idRefValue.String() == "" {
		idRefValue.SetString(bson.NewObjectId().Hex())
		err = r.Create(c, m)
		if err != nil {
			return nil, err
		}
		change := &repository.ChangeInfo{
			UpsertedId: idRefValue.String(),
		}
		return change, nil
	}

	exist, err := r.Exists(c, index, idRefValue.String())
	if err != nil {
		return nil, err
	}

	if !exist {
		err = r.Create(c, m)
		if err != nil {
			return nil, err
		}
		change := &repository.ChangeInfo{
			UpsertedId: idRefValue.String(),
		}
		return change, nil
	}

	reqBody := map[string]interface{}{
		"doc": m,
	}
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	req := esapi.UpdateRequest{
		Index:        index,
		DocumentType: index,
		DocumentID:   idRefValue.String(),
		Body:         bytes.NewReader(jsonBody),
	}
	res, err := req.Do(c, r.DB)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	change := &repository.ChangeInfo{
		UpsertedId: idRefValue.String(),
	}
	return change, nil
}

func (r *BaseRepository) Update(c context.Context, m model.Model, data interface{}) error {
	index, idRefValue, err := getModelInfoAndCheckID(m)
	if err != nil {
		return err
	}

	reqBody := map[string]interface{}{
		"doc": data,
	}
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	req := esapi.UpdateRequest{
		Index:        index,
		DocumentType: index,
		DocumentID:   idRefValue.String(),
		Body:         bytes.NewReader(jsonBody),
	}
	res, err := req.Do(c, r.DB)
	if err != nil {
		return err
	}
	res.Body.Close()

	if res.StatusCode != 200 {
		return errors.New(res.Status())
	}

	return nil
}

func (r *BaseRepository) FindOne(c context.Context, m model.Model) error {
	index, idRefValue, err := getModelInfoAndCheckID(m)
	if err != nil {
		return err
	}

	req := esapi.GetRequest{
		Index:        index,
		DocumentType: index,
		DocumentID:   idRefValue.String(),
		FilterPath:   []string{"_source"},
	}
	res, err := req.Do(c, r.DB)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	fmt.Println(res.String())
	var respData map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&respData)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		return errors.New("not found")
	}

	return breflect.CastStruct(respData["_source"], m)
}

func (r *BaseRepository) Delete(c context.Context, m model.Model) error {
	index, idRefValue, err := getModelInfoAndCheckID(m)
	if err != nil {
		return err
	}

	req := esapi.DeleteRequest{
		Index:        index,
		DocumentType: index,
		DocumentID:   idRefValue.String(),
	}
	res, err := req.Do(c, r.DB)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		return nil
	}

	var respData map[string]interface{}
	if err = json.NewDecoder(res.Body).Decode(&respData); err != nil {
		return err
	}

	return errors.New(respData["result"].(string))
}

type PageResult struct {
	Hits struct {
		Total int `json:"total"`
		Hits  []struct {
			Source interface{} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

// Page 多个条件 and , 每个条件的话， 支持 gt gte lt lte eq  like ne in 这几个即可
func (r *BaseRepository) Page(c context.Context, m model.Model, query *model.PageQuery, resultPtr interface{}) (total int, pageCount int, err error) {
	index, _, err := getModelInfo(m)
	if err != nil {
		return 0, 0, err
	}

	queryMap := buildPageSearch(query)
	jsonBody, err := json.Marshal(queryMap)
	if err != nil {
		return 0, 0, err
	}

	req := esapi.SearchRequest{
		Index:        []string{index},
		DocumentType: []string{index},
		Body:         bytes.NewReader(jsonBody),
		FilterPath:   []string{"hits.hits._source", "hits.total"},
	}
	res, err := req.Do(c, r.DB)
	if err != nil {
		return 0, 0, err
	}
	defer res.Body.Close()

	// todo 数据处理待优化
	var respData PageResult
	err = json.NewDecoder(res.Body).Decode(&respData)
	if err != nil {
		return 0, 0, err
	}

	if res.StatusCode != 200 {
		// todo 看怎么拿到详情
		return 0, 0, errors.New("not found")
	}

	total = respData.Hits.Total

	var sources []interface{}
	for _, v := range respData.Hits.Hits {
		sources = append(sources, v.Source)
	}

	b, err := json.Marshal(sources)
	if err != nil {
		return 0, 0, err
	}

	pageCount = len(sources)
	err = json.Unmarshal(b, resultPtr)

	return
}

func (r *BaseRepository) Cursor(c context.Context, query *model.CursorQuery, m model.Model, resultPtr interface{}) (extra *model.CursorExtra, err error) {
	index, _, err := getModelInfo(m)
	if err != nil {
		return nil, err
	}

	queryMap := buildCursorSearch(query)
	jsonBody, err := json.Marshal(queryMap)
	if err != nil {
		return nil, err
	}

	req := esapi.SearchRequest{
		Index:        []string{index},
		DocumentType: []string{index},
		Body:         bytes.NewReader(jsonBody),
		FilterPath:   []string{"hits.hits._source", "hits.total"},
	}
	res, err := req.Do(c, r.DB)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	// todo 数据处理待优化
	var respData PageResult
	err = json.NewDecoder(res.Body).Decode(&respData)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		// todo 看怎么拿到详情
		return nil, errors.New("not found")
	}

	// todo 这里需要填充
	extra = &model.CursorExtra{
		Direction: 0,
		Size:      0,
		HasMore:   false,
		MaxCursor: 0,
		MinCursor: 0,
	}

	var sources []interface{}
	for _, v := range respData.Hits.Hits {
		sources = append(sources, v.Source)
	}

	b, err := json.Marshal(sources)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(b, resultPtr)

	return
}
