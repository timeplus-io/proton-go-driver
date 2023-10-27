package proton

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/timeplus-io/proton-go-driver/v2/lib/column"
	"github.com/timeplus-io/proton-go-driver/v2/lib/driver"
	"github.com/timeplus-io/proton-go-driver/v2/lib/proto"
)

func (c *connect) prepareStreamingBuffer(ctx context.Context, query string, release func(*connect, error)) (*streamingBuffer, error) {
	query = splitInsertRe.Split(query, -1)[0]
	if !strings.HasSuffix(strings.TrimSpace(strings.ToUpper(query)), "VALUES") {
		query += " VALUES"
	}
	options := queryOptions(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		c.conn.SetDeadline(deadline)
		defer c.conn.SetDeadline(time.Time{})
	}
	if err := c.sendQuery(query, &options); err != nil {
		release(c, err)
		return nil, err
	}
	var (
		onProcess  = options.onProcess()
		block, err = c.firstBlock(ctx, onProcess)
	)
	if err != nil {
		release(c, err)
		return nil, err
	}
	return &streamingBuffer{
		ctx:   ctx,
		conn:  c,
		block: block,
		release: func(err error) {
			release(c, err)
		},
		onProcess: onProcess,
		done:      make(chan struct{}),
	}, nil
}

type streamingBuffer struct {
	err       error
	ctx       context.Context
	conn      *connect
	sent      bool
	block     *proto.Block
	release   func(error)
	onProcess *onProcess
	once      sync.Once
	done      chan struct{}
}

func (b *streamingBuffer) Append(v ...interface{}) error {
	if b.sent {
		return ErrStreamingBufferClosed
	}
	if err := b.block.Append(v...); err != nil {
		b.release(err)
		return err
	}
	return nil
}

func (b *streamingBuffer) AppendStruct(v interface{}) error {
	values, err := b.conn.structMap.Map("AppendStruct", b.block.ColumnsNames(), v, false)
	if err != nil {
		return err
	}
	return b.Append(values...)
}

func (b *streamingBuffer) Column(idx int) driver.StreamingBufferColumn {
	if len(b.block.Columns) <= idx {
		b.release(nil)
		return &streamingBufferColumn{
			err: &OpError{
				Op:  "streamingBuffer.Column",
				Err: fmt.Errorf("invalid column index %d", idx),
			},
		}
	}
	return &streamingBufferColumn{
		buffer: b,
		column: b.block.Columns[idx],
		release: func(err error) {
			b.err = err
			b.release(err)
		},
	}
}

func (b *streamingBuffer) Close() (err error) {
	defer func() {
		b.sent = true
		b.release(err)
	}()
	if err = b.Send(); err != nil {
		return err
	}
	if err = b.conn.sendData(&proto.Block{}, ""); err != nil {
		return err
	}
	if err = b.conn.encoder.Flush(); err != nil {
		return err
	}
	<-b.done
	return nil
}

func (b *streamingBuffer) Clear() (err error) {
	for i := range b.block.Columns {
		b.block.Columns[i], err = b.block.Columns[i].Type().Column()
		if err != nil {
			return
		}
	}
	return nil
}

func (b *streamingBuffer) Send() (err error) {
	if b.sent {
		return ErrStreamingBufferClosed
	}
	if b.err != nil {
		return b.err
	}
	if b.block.Rows() != 0 {
		if err = b.conn.sendData(b.block, ""); err != nil {
			return err
		}
	}
	if err = b.conn.encoder.Flush(); err != nil {
		return err
	}
	b.once.Do(func() {
		go func() {
			if err = b.conn.process(b.ctx, b.onProcess); err != nil {
				log.Fatal(err)
			}
			b.done <- struct{}{}
		}()
	})
	if err = b.Clear(); err != nil {
		return err
	}
	return nil
}

func (b *streamingBuffer) ReplaceBy(cols ...column.Interface) (err error) {
	if len(b.block.Columns) != len(cols) {
		return errors.New(fmt.Sprintf("colomn number is %d, not %d", len(b.block.Columns), len(cols)))
	}
	for i := 0; i < len(cols); i++ {
		if b.block.Columns[i].Type() != cols[i].Type() {
			return errors.New(fmt.Sprintf("type of colomn[%d] is %s, not %s", i, b.block.Columns[i].Type(), cols[i].Type()))
		}
	}
	rows := cols[0].Rows()
	for i := 1; i < len(cols); i++ {
		if rows != cols[i].Rows() {
			return errors.New("cols with different length")
		}
	}
	b.block.Columns = cols
	return nil
}

type streamingBufferColumn struct {
	err     error
	buffer  *streamingBuffer
	column  column.Interface
	release func(error)
}

func (b *streamingBufferColumn) Append(v interface{}) (err error) {
	if b.buffer.sent {
		return ErrStreamingBufferClosed
	}
	if b.err != nil {
		b.release(b.err)
		return b.err
	}
	if _, err = b.column.Append(v); err != nil {
		b.release(err)
		return err
	}
	return nil
}

var (
	_ (driver.StreamingBuffer)       = (*streamingBuffer)(nil)
	_ (driver.StreamingBufferColumn) = (*streamingBufferColumn)(nil)
)
