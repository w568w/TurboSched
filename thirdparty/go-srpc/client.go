package srpc

import (
	"context"
	"github.com/smallnest/rpcx/client"
)

type Client struct {
	client.XClient
}

func (c *Client) CallStream(name string, args any) (h *StreamHandle, err error) {
	h = newStreamHandle(c)
	var sess Session
	ctx := context.Background()
	err = c.Call(ctx, name, args, &sess)
	if err != nil {
		return
	}
	h.sid = sess.Sid

	return
}

func WrapClient(c client.XClient) *Client {
	return &Client{XClient: c}
}
