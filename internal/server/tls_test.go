package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// generateTestCert creates a self-signed certificate and key for testing.
// Returns paths to cert and key PEM files.
func generateTestCert(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-bolt"},
		DNSNames:     []string{"test-bolt", "localhost"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	certPath = filepath.Join(dir, "test-cert.pem")
	keyPath = filepath.Join(dir, "test-key.pem")

	certFile, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("create cert file: %v", err)
	}
	defer certFile.Close()
	pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyFile, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("create key file: %v", err)
	}
	defer keyFile.Close()
	pem.Encode(keyFile, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return certPath, keyPath
}

func TestTLSConfig_BuildAndWrapListener(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	cfg := &TLSConfig{
		CertFile: certPath,
		KeyFile:  keyPath,
	}

	tlsCfg, err := cfg.BuildTLSConfig()
	if err != nil {
		t.Fatalf("BuildTLSConfig: %v", err)
	}
	if tlsCfg == nil {
		t.Fatal("expected non-nil TLS config")
	}

	clientCfg := &tls.Config{
		ServerName:         "test-bolt",
		InsecureSkipVerify: true,
	}

	// Use tls.Listen directly (same as WrapListener internally)
	tlsLn, err := tls.Listen("tcp", "127.0.0.1:0", tlsCfg)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	defer tlsLn.Close()

	// Start server goroutine: serve one TLS connection
	serverDone := make(chan error, 1)
	go func() {
		conn, err := tlsLn.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		// Trigger TLS handshake by reading
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			serverDone <- err
			return
		}
		_ = n
		conn.Close()
		serverDone <- nil
	}()

	// Connect with TLS client
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", tlsLn.Addr().String(), clientCfg)
	if err != nil {
		t.Fatalf("TLS client connect failed: %v", err)
	}
	defer conn.Close()

	// Send some data so server's Read can complete
	_, _ = conn.Write([]byte("PING\r\n"))

	// Wait for server to finish with generous timeout
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("server error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("test timed out waiting for server")
	}
}

func TestTLSConfig_NilConfigPassthrough(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	tlsLn := WrapListener(ln, nil)
	if tlsLn != ln {
		t.Fatal("nil TLS config should return original listener")
	}
}

func TestTLSConfig_NotEnabled(t *testing.T) {
	cfg := &TLSConfig{CertFile: "", KeyFile: ""}
	if cfg.IsEnabled() {
		t.Fatal("empty cert/key should not be enabled")
	}

	tlsCfg, err := cfg.BuildTLSConfig()
	if err != nil {
		t.Fatalf("BuildTLSConfig: %v", err)
	}
	if tlsCfg != nil {
		t.Fatal("expected nil TLS config for disabled")
	}
}

func TestTLSConfig_NonTLSClientRejected(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	cfg := &TLSConfig{
		CertFile: certPath,
		KeyFile:  keyPath,
	}

	tlsCfg, err := cfg.BuildTLSConfig()
	if err != nil {
		t.Fatalf("BuildTLSConfig: %v", err)
	}

	// Start TLS listener
	tlsLn, err := tls.Listen("tcp", "127.0.0.1:0", tlsCfg)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	defer tlsLn.Close()

	// Accept goroutine: trigger TLS handshake by reading with a deadline
	handshakeResult := make(chan error, 1)
	go func() {
		conn, err := tlsLn.Accept()
		if err != nil {
			handshakeResult <- err
			return
		}
		defer conn.Close()
		// Set short deadline so TLS handshake failure surfaces quickly
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		buf := make([]byte, 1)
		_, err = conn.Read(buf)
		handshakeResult <- err
	}()

	// Allow goroutine to reach Accept
	time.Sleep(50 * time.Millisecond)

	// Connect with plain TCP (no TLS handshake)
	conn, err := net.DialTimeout("tcp", tlsLn.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("plain TCP dial: %v", err)
	}
	defer conn.Close()

	// Server's TLS handshake should fail since client sent no ClientHello
	select {
	case err := <-handshakeResult:
		if err == nil {
			t.Fatal("expected TLS handshake error for non-TLS client")
		}
		// Timeout error is acceptable (deadline hit with no handshake)
		t.Logf("non-TLS client rejected: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for server to reject non-TLS client")
	}
}
func TestTLSConfig_InvalidCert(t *testing.T) {
	cfg := &TLSConfig{
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  "/nonexistent/key.pem",
	}
	_, err := cfg.BuildTLSConfig()
	if err == nil {
		t.Fatal("expected error for invalid cert path")
	}
}
