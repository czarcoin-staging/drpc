// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package drpcmux

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/textproto"
	"reflect"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/zeebo/errs"

	"storj.io/drpc"
	"storj.io/drpc/drpcerr"
)

// ServeHTTP handles unitary rpcs over an http request. The rpcs are hosted at a
// path based on their name, like `/service.Server/Method` and accept the request
// protobuf in json. The response will either be of the form
//
//    {
//      "status": "ok",
//      "response": ...
//    }
//
// if the request was successful, or
//
//    {
//      "status": "error",
//      "error": ...,
//      "code": ...
//    }
//
// where error is a textual description of the error, and code is the numeric code
// that was set with drpcerr, if any.
//
// Metadata can be attached by adding the "X-Drpc-Metadata" header to the request
// possibly multiple times. The format is
//
//     X-Drpc-Metadata: percentEncode(key)=percentEncode(value)
//
// where percentEncode is the encoding used for query strings. Only the '%' and '='
// characters are necessary to be escaped.
func (m *Mux) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	ctx, err := buildContext(req.Context(), headerValues(req.Header, "X-Drpc-Metadata"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data, err := m.serveHTTP(ctx, req.URL.Path, req.Body)
	if err != nil {
		data, err = json.MarshalIndent(map[string]interface{}{
			"status": "error",
			"error":  err.Error(),
			"code":   drpcerr.Code(err),
		}, "", "  ")
	} else {
		data, err = json.MarshalIndent(map[string]interface{}{
			"status":   "ok",
			"response": json.RawMessage(data),
		}, "", " ")
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
}

func (m *Mux) serveHTTP(ctx context.Context, rpc string, body io.Reader) ([]byte, error) {
	data, ok := m.rpcs[rpc]
	if !ok {
		return nil, drpc.ProtocolError.New("unknown rpc: %q", rpc)
	} else if !data.unitary {
		return nil, drpc.ProtocolError.New("non-unitary rpc: %q", rpc)
	}

	in, ok := reflect.New(data.in1.Elem()).Interface().(drpc.Message)
	if !ok {
		return nil, drpc.InternalError.New("invalid rpc input type")
	}
	if err := jsonpb.Unmarshal(body, in); err != nil {
		return nil, drpc.ProtocolError.Wrap(err)
	}

	out, err := data.receiver(data.srv, ctx, in, nil)
	if err != nil {
		return nil, errs.Wrap(err)
	} else if out == nil {
		return nil, nil
	}

	var buf bytes.Buffer
	if err := (&jsonpb.Marshaler{Indent: "  "}).Marshal(&buf, out); err != nil {
		return nil, drpc.InternalError.Wrap(err)
	}
	return buf.Bytes(), nil
}

func headerValues(h http.Header, key string) []string {
	if h == nil {
		return nil
	}
	return h[textproto.CanonicalMIMEHeaderKey(key)]
}
