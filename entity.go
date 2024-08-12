package mysql

import (
	"context"
	"errors"

	"github.com/go-qbit/model"
	mysql "github.com/go-qbit/storage-mysql"
)

var (
	ErrNotFound = errors.New("not found")
)

type Fabric struct {
	table  *mysql.BaseModel
	field  string
	filter FilterFunc
}

type FilterFunc func(id interface{}) model.IExpression

type entity struct {
	table  *mysql.BaseModel
	field  string
	filter model.IExpression
	id     interface{}
}

func New(table *mysql.BaseModel, field string, filter FilterFunc) *Fabric {
	return &Fabric{
		table:  table,
		field:  field,
		filter: filter,
	}
}

func (f *Fabric) Get(id interface{}) *entity {
	return &entity{
		table:  f.table,
		field:  f.field,
		filter: f.filter(id),
		id:     id,
	}
}

func (e *entity) StartAction(ctx context.Context) (context.Context, error) {
	return e.table.GetDb().StartTransaction(ctx)
}

func (e *entity) GetState(ctx context.Context) (uint64, error) {
	data, err := e.table.GetAll(ctx, []string{e.field}, model.GetAllOptions{
		Filter:    e.filter,
		Limit:     1,
		ForUpdate: true,
	})
	if err != nil {
		return 0, err
	}

	if data.Len() == 0 {
		return 0, ErrNotFound
	}

	return data.Data()[0][0].(uint64), nil
}

func (e *entity) SetState(ctx context.Context, newState uint64, params ...interface{}) error {
	return e.table.Edit(ctx, e.filter, map[string]interface{}{
		e.field: newState,
	})
}

func (e *entity) EndAction(ctx context.Context, err error) error {
	if err != nil {
		_, _ = e.table.GetDb().Rollback(ctx)
		return err
	}

	_, err = e.table.GetDb().Commit(ctx)
	return err
}

func (e *entity) GetId() interface{} {
	return e.id
}

func (e *entity) GetTx() interface{} {
	panic("not implemented")
}
