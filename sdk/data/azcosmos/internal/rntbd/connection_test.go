//go:build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package rntbd

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	azuuid "github.com/Azure/azure-sdk-for-go/sdk/internal/uuid"
)

func TestRntbdConnectionDial(t *testing.T) {
	addr, cleanup := startMockRntbdServer(t)
	defer cleanup()

	conn, err := Dial(context.TODO(), addr, &tls.Config{InsecureSkipVerify: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()

	require.NotNil(t, conn.ctx)
	require.Equal(t, "mock-rntbd", conn.ctx.ServerAgent)
	require.Equal(t, "1.0.0", conn.ctx.ServerVersion)
	require.Equal(t, uint32(CurrentProtocolVersion), conn.ctx.ProtocolVersion)
	require.Equal(t, uint32(60), conn.ctx.IdleTimeout)
	require.Zero(t, conn.PendingRequests())
	require.False(t, conn.IsClosed())
}

func TestRntbdConnectionSend(t *testing.T) {
	addr, cleanup := startMockRntbdServer(t)
	defer cleanup()

	conn, err := Dial(context.TODO(), addr, &tls.Config{InsecureSkipVerify: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()

	payload := []byte(`{"id":"doc-1"}`)
	response, err := conn.Send(context.TODO(), newTestDocumentRequest(t, payload))
	require.NoError(t, err)
	require.Equal(t, int32(200), response.StatusCode())
	require.Equal(t, payload, response.Payload)
	require.True(t, response.IsPayloadPresent())
	transportRequestID, ok := response.TransportRequestID()
	require.True(t, ok)
	require.Equal(t, uint32(1), transportRequestID)
}

func TestRntbdConnectionMultiplex(t *testing.T) {
	addr, cleanup := startMockRntbdServer(t)
	defer cleanup()

	conn, err := Dial(context.TODO(), addr, &tls.Config{InsecureSkipVerify: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()

	const requestCount = 8
	responses := make([]*Response, requestCount)
	errors := make([]error, requestCount)

	var wg sync.WaitGroup
	for i := 0; i < requestCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := []byte(fmt.Sprintf("delay=%d|request-%d", (requestCount-i)*15, i))
			responses[i], errors[i] = conn.Send(context.TODO(), newTestDocumentRequest(t, payload))
		}(i)
	}
	wg.Wait()

	for i := 0; i < requestCount; i++ {
		require.NoError(t, errors[i])
		require.NotNil(t, responses[i])
		require.Equal(t, int32(200), responses[i].StatusCode())
		require.Equal(t, []byte(fmt.Sprintf("delay=%d|request-%d", (requestCount-i)*15, i)), responses[i].Payload)
	}
	require.Zero(t, conn.PendingRequests())
}

func TestRntbdConnectionClose(t *testing.T) {
	addr, cleanup := startMockRntbdServer(t)
	defer cleanup()

	conn, err := Dial(context.TODO(), addr, &tls.Config{InsecureSkipVerify: true})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := conn.Send(ctx, newTestDocumentRequest(t, []byte("delay=250|close")))
		errCh <- err
	}()

	require.Eventually(t, func() bool {
		return conn.PendingRequests() == 1
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, conn.Close())
	err = <-errCh
	require.Error(t, err)
	require.ErrorContains(t, err, "closed")
	require.True(t, conn.IsClosed())
	require.Eventually(t, func() bool {
		return conn.PendingRequests() == 0
	}, time.Second, 10*time.Millisecond)
}

func startMockRntbdServer(t *testing.T) (addr string, cleanup func()) {
	t.Helper()

	certificate := generateTestCertificate(t)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{certificate}})
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Temporary() {
					continue
				}
				return
			}

			wg.Add(1)
			go func(conn net.Conn) {
				defer wg.Done()
				handleMockRntbdConnection(conn)
			}(conn)
		}
	}()

	return listener.Addr().String(), func() {
		_ = listener.Close()
		wg.Wait()
	}
}

func handleMockRntbdConnection(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	contextRequest, err := readRequest(conn)
	if err != nil {
		return
	}
	if contextRequest.Frame.OperationType != OperationTypeConnection {
		return
	}

	contextHeaders := &TokenSet{}
	contextHeaders.Set(uint16(ContextResponseHeaderProtocolVersion), TokenTypeULong, uint32(CurrentProtocolVersion))
	contextHeaders.Set(uint16(ContextResponseHeaderServerAgent), TokenTypeSmallString, "mock-rntbd")
	contextHeaders.Set(uint16(ContextResponseHeaderServerVersion), TokenTypeSmallString, "1.0.0")
	contextHeaders.Set(uint16(ContextResponseHeaderIdleTimeoutInSeconds), TokenTypeULong, uint32(60))
	if err := writeAll(conn, encodeResponseWire(contextRequest.Frame.ActivityID, 200, contextHeaders, nil)); err != nil {
		return
	}

	var responseWG sync.WaitGroup
	var writeMu sync.Mutex
	defer responseWG.Wait()

	for {
		request, err := readRequest(conn)
		if err != nil {
			if err == io.EOF || strings.Contains(err.Error(), "closed") {
				return
			}
			return
		}

		transportRequestID, ok := request.Headers.Get(uint16(RequestHeaderTransportRequestID))
		if !ok {
			return
		}
		requestID, ok := transportRequestID.(uint32)
		if !ok {
			return
		}

		payload := clonePayload(request.Payload)
		activityID := request.Frame.ActivityID
		responseWG.Add(1)
		go func() {
			defer responseWG.Done()
			if delay := payloadDelay(payload); delay > 0 {
				time.Sleep(delay)
			}

			headers := &TokenSet{}
			headers.Set(uint16(ResponseHeaderPayloadPresent), TokenTypeByte, payloadPresentValue(payload))
			headers.Set(uint16(ResponseHeaderTransportRequestID), TokenTypeULong, requestID)
			wire := encodeResponseWire(activityID, 200, headers, payload)

			writeMu.Lock()
			defer writeMu.Unlock()
			_ = writeAll(conn, wire)
		}()
	}
}

func newTestDocumentRequest(t *testing.T, payload []byte) *Request {
	t.Helper()
	activityID, err := azuuid.New()
	require.NoError(t, err)

	return NewDocumentRequest(
		OperationTypeRead,
		ResourceTypeDocument,
		activityID,
		"type=master&ver=1.0&sig=test",
		"/apps/replicas/0/",
		[]byte("resource-id"),
		0,
		payload,
	)
}

func generateTestCertificate(t *testing.T) tls.Certificate {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	certificateTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	certificateDER, err := x509.CreateCertificate(rand.Reader, certificateTemplate, certificateTemplate, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)

	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER})
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	certificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	require.NoError(t, err)
	return certificate
}

func payloadDelay(payload []byte) time.Duration {
	parts := strings.SplitN(string(payload), "|", 2)
	if len(parts) == 0 || !strings.HasPrefix(parts[0], "delay=") {
		return 0
	}

	milliseconds, err := strconv.Atoi(strings.TrimPrefix(parts[0], "delay="))
	if err != nil {
		return 0
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func readRequest(r io.Reader) (*Request, error) {
	wire, _, headers, err := readMessage(r, RequestFrameLength)
	if err != nil {
		return nil, err
	}

	payloadPresent := false
	if value, ok := tokenByte(headers, uint16(RequestHeaderPayloadPresent)); ok {
		payloadPresent = value != 0
	}
	if payloadPresent {
		payloadLengthBuf := make([]byte, 4)
		if _, err := io.ReadFull(r, payloadLengthBuf); err != nil {
			return nil, fmt.Errorf("read request payload length: %w", err)
		}
		payloadLength := int(binary.LittleEndian.Uint32(payloadLengthBuf))
		payload := make([]byte, payloadLength)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("read request payload: %w", err)
		}
		wire = append(wire, payloadLengthBuf...)
		wire = append(wire, payload...)
	}

	frame := DecodeRequestFrame(wire[:RequestFrameLength])
	request := &Request{
		Frame:   frame,
		Headers: headers,
	}
	if payloadPresent {
		offset := int(frame.MetadataLength)
		payloadLength := int(binary.LittleEndian.Uint32(wire[offset:]))
		request.Payload = append([]byte(nil), wire[offset+4:offset+4+payloadLength]...)
	}
	return request, nil
}
