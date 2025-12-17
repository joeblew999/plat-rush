package client

import (
	"context"
	"fmt"

	"github.com/joeblew999/plat-rush/rpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Config configures the gRPC client dial.
type Config struct {
	// Address is the gRPC endpoint (host:port). Required.
	Address string
	// Insecure uses plaintext transport when true. Otherwise default creds apply.
	Insecure bool
	// DialOptions lets callers add extra dial options.
	DialOptions []grpc.DialOption
}

// Client is a typed wrapper over the gorush gRPC API.
type Client struct {
	conn   *grpc.ClientConn
	Gorush proto.GorushClient
	Health proto.HealthClient
}

// New creates a Client with the provided config.
func New(cfg Config) (*Client, error) {
	if cfg.Address == "" {
		return nil, fmt.Errorf("address is required")
	}

	dialOpts := make([]grpc.DialOption, 0, 1+len(cfg.DialOptions))
	if cfg.Insecure {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	dialOpts = append(dialOpts, cfg.DialOptions...)

	conn, err := grpc.NewClient(cfg.Address, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial gorush: %w", err)
	}

	return &Client{
		conn:   conn,
		Gorush: proto.NewGorushClient(conn),
		Health: proto.NewHealthClient(conn),
	}, nil
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// Send calls Gorush.Send with the given request.
func (c *Client) Send(ctx context.Context, req *proto.NotificationRequest, opts ...grpc.CallOption) (*proto.NotificationReply, error) {
	if c == nil {
		return nil, fmt.Errorf("client is nil")
	}
	return c.Gorush.Send(ctx, req, opts...)
}

// HealthCheck calls the health Check RPC for the given service name.
func (c *Client) HealthCheck(ctx context.Context, service string, opts ...grpc.CallOption) (proto.HealthCheckResponse_ServingStatus, error) {
	if c == nil {
		return proto.HealthCheckResponse_UNKNOWN, fmt.Errorf("client is nil")
	}
	resp, err := c.Health.Check(ctx, &proto.HealthCheckRequest{Service: service}, opts...)
	if err != nil {
		return proto.HealthCheckResponse_UNKNOWN, err
	}
	return resp.GetStatus(), nil
}
