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

func (r *BaseRepository) Create(c context.Context, m model.Model) error {

	index, idRefValue, err := getModelInfo(m)
	if err != nil {
		return err
	}

	if idRefValue.String() == "" {
		idRefValue.SetString(bson.NewObjectId().Hex())
	}

	jsonBody, _ := json.Marshal(m)

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
	jsonBody, _ := json.Marshal(reqBody)

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
	jsonBody, _ := json.Marshal(reqBody)

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
	}
	res, err := req.Do(c, r.DB)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	var respData map[string]interface{}
	if err = json.NewDecoder(res.Body).Decode(&respData); err != nil {
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

// Page 多个条件 and , 每个条件的话， 支持 gt gte lt lte eq  like ne in 这几个即可
func (r *BaseRepository) Page(c context.Context, m model.Model, query *model.PageQuery, resultPtr interface{}) (total int, pageCount int, err error) {

	return
}

func (r *BaseRepository) Cursor(c context.Context, query *model.CursorQuery, m model.Model, resultPtr interface{}) (extra *model.CursorExtra, err error) {

	return
}
