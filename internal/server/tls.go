package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"time"
)

const dialTimeout = 5 * time.Second

// TLSConfig holds TLS configuration for the server.
type TLSConfig struct {
	CertFile string // path to PEM certificate file
	KeyFile  string // path to PEM private key file
	CAFile   string // path to CA certificate file (optional, for client verification)
	Require  bool   // if true, reject non-TLS connections (requires --tls-cert)
}

// IsEnabled returns true if TLS cert and key are configured.
func (tc *TLSConfig) IsEnabled() bool {
	return tc.CertFile != "" && tc.KeyFile != ""
}

// BuildTLSConfig creates a *tls.Config from the TLSConfig options.
// Returns nil if TLS is not enabled.
func (tc *TLSConfig) BuildTLSConfig() (*tls.Config, error) {
	if !tc.IsEnabled() {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(tc.CertFile, tc.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS certificate/key: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	// If CA file is provided, require client certificates signed by that CA
	if tc.CAFile != "" {
		caCert, err := os.ReadFile(tc.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsCfg.ClientCAs = caCertPool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsCfg, nil
}

// WrapListener wraps a net.Listener with TLS.
// Returns the original listener if tlsCfg is nil.
func WrapListener(ln net.Listener, tlsCfg *tls.Config) net.Listener {
	if tlsCfg == nil {
		return ln
	}
	return tls.NewListener(ln, tlsCfg)
}

// DialTLS creates a TLS connection to addr, using the provided TLS config.
// Used by replication, cluster bus, and sentinel gossip.
func DialTLS(addr string, tlsCfg *tls.Config) (net.Conn, error) {
	if tlsCfg == nil {
		return net.DialTimeout("tcp", addr, dialTimeout)
	}
	return tls.DialWithDialer(
		&net.Dialer{Timeout: dialTimeout},
		"tcp",
		addr,
		tlsCfg,
	)
}
