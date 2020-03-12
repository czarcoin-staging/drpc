// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package drpcconn

import (
	"context"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/drpc"
	"storj.io/drpc/drpcmanager"
	"storj.io/drpc/drpcstream"
	"storj.io/drpc/drpcwire"
	"storj.io/drpc/internal"
)

// Options controls configuration settings for a conn.
type Options struct {
	// Manager controls the options we pass to the manager of this conn.
	Manager drpcmanager.Options
}

// Conn is a drpc client connection.
type Conn struct {
	tr  drpc.Transport
	man *drpcmanager.Manager
}

var _ drpc.Conn = (*Conn)(nil)

// New returns a conn that uses the transport for reads and writes.
func New(tr drpc.Transport) *Conn {
	return NewWithOptions(tr, Options{})
}

// NewWithOptions returns a conn that uses the transport for reads and writes.
// The Options control details of how the conn operates.
func NewWithOptions(tr drpc.Transport, opts Options) *Conn {
	return &Conn{
		tr:  tr,
		man: drpcmanager.NewWithOptions(tr, opts.Manager),
	}
}

// Transport returns the transport the conn is using.
func (c *Conn) Transport() drpc.Transport {
	return c.tr
}

// Closed returns true if the connection is already closed.
func (c *Conn) Closed() bool {
	return c.man.Closed()
}

// Close closes the connection.
func (c *Conn) Close() (err error) {
	return c.man.Close()
}

const INVOKE_HEADER_VERSION_1 = 1
const INVOKE_HEADER_TRACEID = "trace-id"
const INVOKE_HEADER_PARENTID = "parent-id"

// Invoke issues the rpc on the transport serializing in, waits for a response, and
// deserializes it into out. Only one Invoke or Stream may be open at a time.
func (c *Conn) Invoke(ctx context.Context, rpc string, in, out drpc.Message) (err error) {
	defer mon.Task()(&ctx)(&err)
	defer mon.TaskNamed("invoke" + rpc)(&ctx)(&err)
	mon.Event("outgoing_requests")
	mon.Event("outgoing_invokes")

	headers := internal.InvokeHeader{
		Version: INVOKE_HEADER_VERSION_1,
		Headers: make(map[string][]byte),
	}

	headers.Headers["fooo"] = []byte("bar")
	headers.Headers["fooo2"] = []byte("bar2")

	headerData, err := proto.Marshal(&headers)
	fmt.Println(headerData)

	span := monkit.SpanFromCtx(ctx)
	if span != nil {
		fmt.Println(span.Trace().Id())
		fmt.Println(span.Id())
		fmt.Println(span.Parent().Id())
	}

	//span.Annotations()[0].

	// headers2 := internal.InvokeHeader{}
	// err = proto.Unmarshal(headerData, &headers2)
	// version := headers2.Version
	// foo := headers2.Headers["fooo2"]
	// fmt.Printf("%d\t%s\n", version, foo)

	data, err := proto.Marshal(in)
	if err != nil {
		return errs.Wrap(err)
	}

	stream, err := c.man.NewClientStream(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errs.Combine(err, stream.Close()) }()

	if err := c.doInvoke(stream, []byte(rpc), data, out); err != nil {
		return err
	}
	return nil
}

func (c *Conn) doInvoke(stream *drpcstream.Stream, rpc, data []byte, out drpc.Message) (err error) {
	if err := stream.RawWrite(drpcwire.KindInvoke, rpc); err != nil {
		return err
	}
	if err := stream.RawWrite(drpcwire.KindMessage, data); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	if err := stream.MsgRecv(out); err != nil {
		return err
	}
	return nil
}

// NewStream begins a streaming rpc on the connection. Only one Invoke or Stream may
// be open at a time.
func (c *Conn) NewStream(ctx context.Context, rpc string) (_ drpc.Stream, err error) {
	defer mon.Task()(&ctx)(&err)
	defer mon.TaskNamed("stream" + rpc)(&ctx)(&err)
	mon.Event("outgoing_requests")
	mon.Event("outgoing_streams")

	stream, err := c.man.NewClientStream(ctx)
	if err != nil {
		return nil, err
	}

	if err := c.doNewStream(stream, []byte(rpc)); err != nil {
		return nil, errs.Combine(err, stream.Close())
	}
	return stream, nil
}

func (c *Conn) doNewStream(stream *drpcstream.Stream, rpc []byte) error {
	if err := stream.RawWrite(drpcwire.KindInvoke, rpc); err != nil {
		return err
	}
	if err := stream.RawFlush(); err != nil {
		return err
	}
	return nil
}
