//go:build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package rntbd

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var errConnectionClosed = fmt.Errorf("rntbd connection closed")

// Connection represents a single RNTBD TCP connection to a Cosmos DB replica.
// It multiplexes requests/responses over the connection using TransportRequestID.
type Connection struct {
	conn          net.Conn
	writeMu       sync.Mutex
	mu            sync.Mutex
	pending       map[uint32]chan *Response // transportRequestID -> response channel
	nextRequestID atomic.Uint32
	ctx           *ContextResponse
	closed        atomic.Bool
	closeCh       chan struct{}
	addr          string // host:port
	closeErr      error
}

// Dial creates a new RNTBD connection to the given address.
// Performs TLS handshake and RNTBD context negotiation.
func Dial(ctx context.Context, addr string, tlsConfig *tls.Config) (*Connection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if addr == "" {
		return nil, fmt.Errorf("rntbd dial: address is empty")
	}

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("rntbd dial %q: %w", addr, err)
	}

	dialer := &net.Dialer{}
	rawConn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("rntbd dial %s: %w", addr, err)
	}

	cfg := &tls.Config{}
	if tlsConfig != nil {
		cfg = tlsConfig.Clone()
	}
	if cfg.ServerName == "" {
		cfg.ServerName = host
	}

	tlsConn := tls.Client(rawConn, cfg)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = rawConn.Close()
		return nil, fmt.Errorf("rntbd tls handshake %s: %w", addr, err)
	}

	conn := &Connection{
		conn:    tlsConn,
		pending: make(map[uint32]chan *Response),
		closeCh: make(chan struct{}),
		addr:    addr,
	}

	if deadline, ok := ctx.Deadline(); ok {
		if err := tlsConn.SetDeadline(deadline); err != nil {
			_ = tlsConn.Close()
			return nil, fmt.Errorf("rntbd set negotiation deadline %s: %w", addr, err)
		}
		defer func() { _ = tlsConn.SetDeadline(time.Time{}) }()
	}

	contextRequest := NewContextRequest("1.0.0", "azcosmos-go/1.0.0")
	if err := writeAll(tlsConn, contextRequest.Encode()); err != nil {
		_ = tlsConn.Close()
		return nil, fmt.Errorf("rntbd send context request %s: %w", addr, err)
	}

	response, err := readResponse(tlsConn)
	if err != nil {
		_ = tlsConn.Close()
		return nil, fmt.Errorf("rntbd read context response %s: %w", addr, err)
	}

	contextResponse, err := DecodeContextResponse(response)
	if err != nil {
		_ = tlsConn.Close()
		return nil, fmt.Errorf("rntbd decode context response %s: %w", addr, err)
	}
	conn.ctx = contextResponse

	go conn.readLoop()
	return conn, nil
}

// Send sends a request and waits for the matching response.
// The TransportRequestID is automatically assigned.
func (c *Connection) Send(ctx context.Context, req *Request) (*Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if req == nil {
		return nil, fmt.Errorf("rntbd send: request is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c == nil {
		return nil, fmt.Errorf("rntbd send: connection is nil")
	}
	if c.IsClosed() {
		return nil, c.connectionErr()
	}

	requestID := c.nextRequestID.Add(1)
	prepared := cloneRequest(req)
	if prepared.Headers == nil {
		prepared.Headers = &TokenSet{}
	}
	prepared.Headers.Set(uint16(RequestHeaderTransportRequestID), TokenTypeULong, requestID)

	responseCh := make(chan *Response, 1)
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		return nil, c.connectionErr()
	}
	c.pending[requestID] = responseCh
	c.mu.Unlock()
	defer c.unregisterPending(requestID)

	encoded := prepared.Encode()

	c.writeMu.Lock()
	if c.closed.Load() {
		c.writeMu.Unlock()
		return nil, c.connectionErr()
	}

	var resetDeadline bool
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetWriteDeadline(deadline); err != nil {
			c.writeMu.Unlock()
			return nil, fmt.Errorf("rntbd set write deadline for request %d: %w", requestID, err)
		}
		resetDeadline = true
	}
	if err := ctx.Err(); err != nil {
		if resetDeadline {
			_ = c.conn.SetWriteDeadline(time.Time{})
		}
		c.writeMu.Unlock()
		return nil, err
	}
	writeErr := writeAll(c.conn, encoded)
	if resetDeadline {
		_ = c.conn.SetWriteDeadline(time.Time{})
	}
	c.writeMu.Unlock()
	if writeErr != nil {
		wrappedErr := fmt.Errorf("rntbd write request %d to %s: %w", requestID, c.addr, writeErr)
		_ = c.closeWithErr(wrappedErr)
		return nil, wrappedErr
	}

	select {
	case response := <-responseCh:
		return response, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closeCh:
		return nil, c.connectionErr()
	}
}

// Close gracefully closes the connection.
func (c *Connection) Close() error {
	if c == nil {
		return nil
	}
	return c.closeWithErr(errConnectionClosed)
}

// PendingRequests returns the number of in-flight requests.
func (c *Connection) PendingRequests() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// IsClosed returns true if the connection has been closed.
func (c *Connection) IsClosed() bool {
	if c == nil {
		return true
	}
	return c.closed.Load()
}

func (c *Connection) readLoop() {
	for {
		response, err := readResponse(c.conn)
		if err != nil {
			_ = c.closeWithErr(fmt.Errorf("rntbd read response from %s: %w", c.addr, err))
			return
		}

		requestID, ok := response.TransportRequestID()
		if !ok {
			continue
		}

		c.mu.Lock()
		responseCh := c.pending[requestID]
		if responseCh != nil {
			delete(c.pending, requestID)
		}
		c.mu.Unlock()
		if responseCh == nil {
			continue
		}

		select {
		case responseCh <- response:
		default:
		}
	}
}

func (c *Connection) closeWithErr(err error) error {
	if err == nil {
		err = errConnectionClosed
	}
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}

	c.mu.Lock()
	c.closeErr = err
	c.pending = make(map[uint32]chan *Response)
	c.mu.Unlock()

	close(c.closeCh)
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Connection) connectionErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeErr != nil {
		return c.closeErr
	}
	return errConnectionClosed
}

func (c *Connection) unregisterPending(requestID uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pending, requestID)
}

func cloneRequest(req *Request) *Request {
	cloned := &Request{
		Frame:   req.Frame,
		Payload: clonePayload(req.Payload),
	}
	if req.Headers != nil {
		cloned.Headers = cloneTokenSet(req.Headers)
	}
	return cloned
}

func cloneTokenSet(tokens *TokenSet) *TokenSet {
	if tokens == nil {
		return nil
	}

	cloned := &TokenSet{tokens: make([]Token, len(tokens.tokens))}
	for i, token := range tokens.tokens {
		cloned.tokens[i] = Token{
			ID:       token.ID,
			Type:     token.Type,
			Value:    cloneTokenValue(token.Value),
			Required: token.Required,
			Present:  token.Present,
		}
	}
	return cloned
}

func readResponse(r io.Reader) (*Response, error) {
	wire, _, headers, err := readMessage(r, ResponseFrameLength)
	if err != nil {
		return nil, err
	}

	payloadPresent := false
	if value, ok := tokenByte(headers, uint16(ResponseHeaderPayloadPresent)); ok {
		payloadPresent = value != 0
	}
	if payloadPresent {
		payloadLengthBuf := make([]byte, 4)
		if _, err := io.ReadFull(r, payloadLengthBuf); err != nil {
			return nil, fmt.Errorf("read response payload length: %w", err)
		}
		payloadLength := int(binary.LittleEndian.Uint32(payloadLengthBuf))
		payload := make([]byte, payloadLength)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("read response payload: %w", err)
		}
		wire = append(wire, payloadLengthBuf...)
		wire = append(wire, payload...)
	}

	response, consumed, err := DecodeResponse(wire)
	if err != nil {
		return nil, err
	}
	if consumed != len(wire) {
		return nil, fmt.Errorf("decode response: consumed %d bytes, have %d", consumed, len(wire))
	}
	response.Headers = headers
	return response, nil
}

func readMessage(r io.Reader, frameLength int) ([]byte, []byte, *TokenSet, error) {
	prefix := make([]byte, 4)
	if _, err := io.ReadFull(r, prefix); err != nil {
		return nil, nil, nil, err
	}

	metadataLength := binary.LittleEndian.Uint32(prefix)
	if metadataLength < uint32(frameLength) {
		return nil, nil, nil, fmt.Errorf("read metadata: metadata length %d is smaller than frame length %d", metadataLength, frameLength)
	}

	metadata := make([]byte, metadataLength)
	copy(metadata[:4], prefix)
	if _, err := io.ReadFull(r, metadata[4:]); err != nil {
		return nil, nil, nil, fmt.Errorf("read metadata body: %w", err)
	}

	headers := &TokenSet{}
	if metadataLength > uint32(frameLength) {
		if err := headers.Decode(metadata[frameLength:]); err != nil {
			return nil, nil, nil, err
		}
	}

	return metadata, metadata[:frameLength], headers, nil
}

func writeAll(w io.Writer, buf []byte) error {
	for len(buf) > 0 {
		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		buf = buf[n:]
	}
	return nil
}
