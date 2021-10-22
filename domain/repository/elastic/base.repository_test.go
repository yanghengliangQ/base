package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	elasticsearch6 "github.com/elastic/go-elasticsearch/v6"
	"github.com/micro/go-micro/v2/config"
	"github.com/micro/go-micro/v2/config/source/memory"
	"github.com/stretchr/testify/assert"
	"github.com/xxxmicro/base/database/elastic"
	"github.com/xxxmicro/base/domain/model"
	"gopkg.in/mgo.v2/bson"
	"log"
	"testing"
	"time"
)

type User struct {
	ID    string    `json:"id"`
	Name  string    `json:"name"`
	Age   int       `json:"age"`
	Ctime time.Time `json:"ctime"`
	Mtime time.Time `json:"mtime"`
	Dtime time.Time `json:"dtime"`
}

func (u *User) Unique() interface{} {
	return bson.M{
		"_id": u.ID,
	}
}

type UimTest struct {
	ID    string    `json:"id"`
	Name  string    `json:"name"`
	Ctime time.Time `json:"ctime"`
	Mtime time.Time `json:"mtime"`
	Dtime time.Time `json:"dtime"`
}

func (u *UimTest) Unique() interface{} {
	return bson.M{
		"_id": u.ID,
	}
}

func getConfig() (config.Config, error) {
	config, err := config.NewConfig()
	if err != nil {
		return nil, err
	}

	data := []byte(`{
		"elastic": {
			"addresses":[
				"http://127.0.0.1:9200"
			],
			"username":"elastic",
			"password":"123456"
		}
	}`)
	source := memory.NewSource(memory.WithJSON(data))

	err = config.Load(source)
	if err != nil {
		return nil, err
	}

	return config, nil
}

func getDB(config config.Config) (*elasticsearch6.Client, error) {
	db, err := elastic.NewElasticProvider(config)
	if err != nil {
		log.Panic("数据库连接失败")
		return nil, err
	}

	return db, nil
}

func TestBaseRepository_Create(t *testing.T) {
	cfg, err := getConfig()
	if err != nil {
		t.Fatal(err)
		return
	}

	db, err := getDB(cfg)
	if err != nil {
		t.Fatal(err)
		return
	}

	userRepo := NewBaseRepository(db)
	now := time.Now()
	user := &User{
		ID: bson.NewObjectId().Hex(),
		Name: "吕布",
		Age: 28,
	}
	user.Ctime = now
	err = userRepo.Create(context.Background(), user)
	if err != nil {
		log.Fatal("插入记录失败")
		return
	}
}

func TestBaseRepository_Upsert(t *testing.T) {

	cfg, err := getConfig()
	if err != nil {
		t.Fatal(err)
		return
	}

	db, err := getDB(cfg)
	if err != nil {
		t.Fatal(err)
		return
	}

	userRepo := NewBaseRepository(db)
	now := time.Now()
	// 不指定id
	user := &User{
		Name: "吕布",
		Age: 29,
	}
	user.Ctime = now
	change, err := userRepo.Upsert(context.Background(), user)
	if err != nil {
		log.Fatal("插入记录失败")
		return
	}
	t.Log(change)

	// 不存在的id
	user2 := &User{
		ID: bson.NewObjectId().Hex(),
		Name: "吕布2",
		Age: 29,
	}
	user2.Ctime = now
	change, err = userRepo.Upsert(context.Background(), user2)
	if err != nil {
		log.Fatal("插入记录失败")
		return
	}
	t.Log(change)

	// 指定id
	user3 := &User{
		ID: "61713ec431cc5f524088ec09",
		Name: "吕布3",
		Age: 29,
	}
	user3.Ctime = now
	change, err = userRepo.Upsert(context.Background(), user3)
	if err != nil {
		log.Fatal("插入记录失败")
		return
	}
	t.Log(change)

}

func TestBaseRepository_Update(t *testing.T) {

	cfg, err := getConfig()
	if err != nil {
		t.Fatal(err)
		return
	}

	db, err := getDB(cfg)
	if err != nil {
		t.Fatal(err)
		return
	}

	userRepo := NewBaseRepository(db)

	data := map[string]interface{}{
		"name": "孙悟空",
	}

	// 指定存在的id
	user := &User{
		ID: "61713ec431cc5f524088ec09",
	}
	err = userRepo.Update(context.Background(), user, data)
	if err != nil {
		t.Fatal(err)
	}

	// 指定不存在的id
	user2 := &User{
		ID: "61713ec431cc5524088ec09",
	}
	err = userRepo.Update(context.Background(), user2, data)
	if err != nil {
		t.Fatal(err)
	}

}

func TestBaseRepository_FindOne(t *testing.T) {

	cfg, err := getConfig()
	if err != nil {
		t.Fatal(err)
		return
	}

	db, err := getDB(cfg)
	if err != nil {
		t.Fatal(err)
		return
	}

	userRepo := NewBaseRepository(db)

	// 指定存在的id
	user := &User{
		ID: "61713ec431cc5f524088ec09",
	}
	err = userRepo.FindOne(context.Background(), user)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(user.Name)

	// 指定不存在的id
	user2 := &User{
		ID: "61713ec43524088ec09",
	}
	err = userRepo.FindOne(context.Background(), user2)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(user2)
}

func TestBaseRepository_Delete(t *testing.T) {

	cfg, err := getConfig()
	if err != nil {
		t.Fatal(err)
		return
	}

	db, err := getDB(cfg)
	if err != nil {
		t.Fatal(err)
		return
	}

	userRepo := NewBaseRepository(db)

	// 指定存在的id
	user := &User{
		ID: "61713ec431cc5f524088ec09",
	}
	err = userRepo.Delete(context.Background(), user)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(user)

	// 指定不存在的id
	user2 := &User{
		ID: "61713ec43524088ec09",
	}
	err = userRepo.Delete(context.Background(), user2)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(user2)
}

func TestBaseRepository_Page(t *testing.T) {
	cfg, err := getConfig()
	if err != nil {
		t.Fatal(err)
		return
	}

	db, err := getDB(cfg)
	if err != nil {
		t.Fatal(err)
		return
	}

	userRepo := NewBaseRepository(db)

	pageQuery := &model.PageQuery{
		Filters: map[string]interface{}{
			"name": "赵云",
			"age": map[string]interface{}{
				"GT": 22,
			},
		},
		PageSize: 10,
		PageNo: 1,
	}

	items := make([]*User, 0)
	total, pageCount, err := userRepo.Page(context.Background(), &User{}, pageQuery, &items)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("total = ", total)
	t.Log("pageCount = ", pageCount)

}


func TestCrud(t *testing.T) {
	assert := assert.New(t)

	config, err := getConfig()
	if err != nil {
		t.Fatal(err)
		return
	}

	db, err := getDB(config)
	if err != nil {
		t.Fatal(err)
		return
	}

	userRepo := NewBaseRepository(db)

	user1 := &User{
		ID:   string(bson.NewObjectId()),
		Name: "吕布",
		Age:  28,
	}

	user2 := &User{
		ID:   string(bson.NewObjectId()),
		Name: "貂蝉",
		Age:  21,
	}

	{
		now := time.Now()
		user1.Ctime = now
		user2.Mtime = now
		err := userRepo.Create(context.Background(), user1)
		assert.NoError(err)
		if err != nil {
			log.Fatal("插入记录失败")
			return
		}

		time.Sleep(time.Second * 3)

		now = time.Now()
		user2.Ctime = now
		user2.Mtime = now
		err = userRepo.Create(context.Background(), user2)
		assert.NoError(err)
		if err != nil {
			log.Fatal("插入记录失败")
			return
		}

		log.Print("插入记录成功")
	}

	user3 := &User{
		ID:   string(bson.NewObjectId()),
		Name: "关羽",
		Age:  38,
	}
	{
		change, err := userRepo.Upsert(context.Background(), user3)
		assert.NoError(err)
		t.Logf("change: %v", change)
	}

	{
		data := map[string]interface{}{
			"name": "赵云",
		}
		err := userRepo.Update(context.Background(), &User{}, data)
		if !assert.Error(err) {
			t.Fatal(err)
		}

		err = userRepo.Update(context.Background(), user1, data)
		if !assert.NoError(err) {
			t.Fatal(err)
		}
		log.Print("选择更新成功")
	}

	{
		findUser := &User{ID: user2.ID}
		err := userRepo.FindOne(context.Background(), findUser)
		assert.NoError(err)
		if err != nil {
			log.Print("查找记录失败")
			t.Fatal(err)
			return
		}
		t.Log(findUser)
		log.Print("找到对应记录")
	}

	{
		pageQuery := &model.PageQuery{
			Filters: map[string]interface{}{
				"name": "赵云",
				"age": map[string]interface{}{
					"GT": 22,
				},
			},
			PageSize: 10,
			PageNo:   1,
		}

		items := make([]*User, 0)
		total, pageCount, err := userRepo.Page(context.Background(), &User{}, pageQuery, &items)
		if assert.Error(err) {
			t.Fatal(err)
		}

		if assert.Equal(1, total) {
			log.Print("翻页查询总数正确")
		} else {
			log.Print(fmt.Sprintf("翻页查询总数错误, 期望1, 返回%d", total))
		}

		if assert.Equal(1, pageCount) {
			log.Print("翻页查询页数正确")
		} else {
			log.Print(fmt.Sprintf("翻页查询页数错误, 期望1, 返回%d", pageCount))
		}

		b, _ := json.Marshal(items)
		s := string(b)
		t.Log(s)
	}

	{
		h, _ := time.ParseDuration("1s")
		t1 := user1.Ctime.Add(h)
		cursor := t1.UnixNano() / 1e6

		cursorQuery := &model.CursorQuery{
			Filters: map[string]interface{}{},
			CursorSort: &model.SortSpec{
				Property: "ctime",
			},
			Cursor: cursor,
			Size:   10,
		}

		items := make([]*User, 0)
		extra, err := userRepo.Cursor(context.Background(), cursorQuery, &User{}, &items)
		if assert.Error(err) {
			t.Fatal(err)
		}

		if assert.Equal(2, len(items)) {
			log.Print("游标查询正确")
		} else {
			log.Print(fmt.Sprintf("游标查询错误 期望1条, 实际返回%d条", len(items)))
		}

		b, _ := json.Marshal(extra)
		s := string(b)
		t.Log(s)
	}

	{
		err := userRepo.Delete(context.Background(), &User{ID: user1.ID})
		assert.NoError(err)
		log.Print("删除记录成功")

		items := make([]*User, 0)
		total, pageCount, err := userRepo.Page(context.Background(), &User{}, &model.PageQuery{
			Filters:  map[string]interface{}{},
			PageSize: 10,
			PageNo:   1,
		}, &items)
		assert.NoError(err)
		assert.Equal(1, total)
		assert.Equal(1, len(items))

		err = userRepo.Delete(context.Background(), &User{ID: user2.ID})
		err = userRepo.Delete(context.Background(), &User{ID: user3.ID})
		log.Print("删除记录成功")

		items = make([]*User, 0)
		total, pageCount, err = userRepo.Page(context.Background(), &User{}, &model.PageQuery{
			Filters:  map[string]interface{}{},
			PageSize: 10,
			PageNo:   1,
		}, &items)
		assert.NoError(err)
		assert.Equal(0, total)
		assert.Equal(0, pageCount)

		log.Print("翻页核对成功")
	}
}
