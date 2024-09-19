package opcua_plugin

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"strings"
	"time"
)

// GetOPCUAClientOptions constructs the OPC UA client options based on the selected endpoint and authentication method.
//
// This function is essential for configuring the OPC UA client with the appropriate security settings,
// authentication credentials, and session parameters. It ensures that the client adheres to the
// security policies of the selected endpoint and supports the desired authentication mechanism.
//
// **Why This Function is Needed:**
// - To dynamically generate client options that match the server’s security requirements.
// - To handle different authentication methods, such as anonymous or username/password-based logins.
// - To generate and include client certificates when using enhanced security policies like Basic256Sha256.
func (g *OPCUAInput) GetOPCUAClientOptions(selectedEndpoint *ua.EndpointDescription, selectedAuthentication ua.UserTokenType) (opts []opcua.Option, err error) {
	opts = append(opts, opcua.SecurityFromEndpoint(selectedEndpoint, selectedAuthentication))

	// Set additional options based on the authentication method
	switch selectedAuthentication {
	case ua.UserTokenTypeAnonymous:
		g.Log.Infof("Using anonymous login")
	case ua.UserTokenTypeUserName:
		g.Log.Infof("Using username/password login")
		opts = append(opts, opcua.AuthUsername(g.Username, g.Password))
	}

	// Generate certificates if Basic256Sha256
	if selectedEndpoint.SecurityPolicyURI == ua.SecurityPolicyURIBasic256Sha256 {
		randomStr := randomString(8) // Generates an 8-character random string
		clientName := "urn:benthos-umh:client-" + randomStr
		certPEM, keyPEM, err := GenerateCert(clientName, 2048, 24*time.Hour*365*10)
		if err != nil {
			g.Log.Errorf("Failed to generate certificate: %v", err)
			return nil, err
		}

		// Convert PEM to X509 Certificate and RSA PrivateKey for in-memory use.
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			g.Log.Errorf("Failed to parse certificate: %v", err)
			return nil, err
		}

		pk, ok := cert.PrivateKey.(*rsa.PrivateKey)
		if !ok {
			g.Log.Errorf("Invalid private key type")
			return nil, err
		}

		// Append the certificate and private key to the client options
		opts = append(opts, opcua.PrivateKey(pk), opcua.Certificate(cert.Certificate[0]))
	}

	opts = append(opts, opcua.SessionName("benthos-umh"))
	if g.SessionTimeout > 0 {
		opts = append(opts, opcua.SessionTimeout(time.Duration(g.SessionTimeout*int(time.Millisecond)))) // set the session timeout to prevent having to many connections
	} else {
		opts = append(opts, opcua.SessionTimeout(SessionTimeout))
	}
	opts = append(opts, opcua.ApplicationName("benthos-umh"))
	//opts = append(opts, opcua.ApplicationURI("urn:benthos-umh"))
	//opts = append(opts, opcua.ProductURI("urn:benthos-umh"))

	// Fine-Tune Buffer
	//opts = append(opts, opcua.MaxMessageSize(2*1024*1024))    // 2MB
	//opts = append(opts, opcua.ReceiveBufferSize(2*1024*1024)) // 2MB
	//opts = append(opts, opcua.SendBufferSize(2*1024*1024))    // 2MB
	return opts, nil
}

// LogEndpoint logs detailed information about a single OPC UA endpoint.
//
// This function provides comprehensive logging of an endpoint’s properties, including security
// configurations and server details. It is useful for debugging and auditing purposes, allowing
// developers and administrators to verify endpoint settings and ensure they meet the required standards.
func (g *OPCUAInput) LogEndpoint(endpoint *ua.EndpointDescription) {
	g.Log.Infof("  EndpointURL: %s", endpoint.EndpointURL)
	g.Log.Infof("  SecurityMode: %v", endpoint.SecurityMode)
	g.Log.Infof("  SecurityPolicyURI: %s", endpoint.SecurityPolicyURI)
	g.Log.Infof("  TransportProfileURI: %s", endpoint.TransportProfileURI)
	g.Log.Infof("  SecurityLevel: %d", endpoint.SecurityLevel)

	// If Server is not nil, log its details
	if endpoint.Server != nil {
		g.Log.Infof("  Server ApplicationURI: %s", endpoint.Server.ApplicationURI)
		g.Log.Infof("  Server ProductURI: %s", endpoint.Server.ProductURI)
		g.Log.Infof("  Server ApplicationName: %s", endpoint.Server.ApplicationName.Text)
		g.Log.Infof("  Server ApplicationType: %v", endpoint.Server.ApplicationType)
		g.Log.Infof("  Server GatewayServerURI: %s", endpoint.Server.GatewayServerURI)
		g.Log.Infof("  Server DiscoveryProfileURI: %s", endpoint.Server.DiscoveryProfileURI)
		g.Log.Infof("  Server DiscoveryURLs: %v", endpoint.Server.DiscoveryURLs)
	}

	// Output the certificate
	if len(endpoint.ServerCertificate) > 0 {
		// Convert to PEM format first, then log the certificate information
		pemCert := pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: endpoint.ServerCertificate,
		})
		g.logCertificateInfo(pemCert)
	}

	// Loop through UserIdentityTokens
	for j, token := range endpoint.UserIdentityTokens {
		g.Log.Infof("  UserIdentityToken %d:", j+1)
		g.Log.Infof("    PolicyID: %s", token.PolicyID)
		g.Log.Infof("    TokenType: %v", token.TokenType)
		g.Log.Infof("    IssuedTokenType: %s", token.IssuedTokenType)
		g.Log.Infof("    IssuerEndpointURL: %s", token.IssuerEndpointURL)
	}
}

// LogEndpoints logs information for a slice of OPC UA endpoints.
func (g *OPCUAInput) LogEndpoints(endpoints []*ua.EndpointDescription) {
	for i, endpoint := range endpoints {
		g.Log.Infof("Endpoint %d:", i+1)
		g.LogEndpoint(endpoint)
	}
}

// FetchAllEndpoints retrieves all available OPC UA endpoints from the specified server.
//
// This function queries the OPC UA server to obtain a list of its available endpoints. If only
// a single endpoint is returned, it performs additional discovery using the server’s discovery URL.
// It also adjusts endpoint URLs to use user-specified hosts to address potential connectivity issues
// related to DNS discrepancies.
//
// **Why This Function is Needed:**
//   - To discover all possible endpoints offered by an OPC UA server, ensuring the client can connect
//     using the most suitable endpoint.
//   - To handle servers that may return limited or DNS-specific endpoint information by substituting
//     with user-specified host details.
func (g *OPCUAInput) FetchAllEndpoints(ctx context.Context) ([]*ua.EndpointDescription, error) {
	g.Log.Infof("Querying OPC UA server at: %s", g.Endpoint)

	endpoints, err := opcua.GetEndpoints(ctx, g.Endpoint)
	if err != nil {
		g.Log.Errorf("Error fetching endpoints from: %s, error: %s", g.Endpoint, err)
		return nil, err
	}

	g.Log.Infof("Retrieved %d initial endpoint(s).", len(endpoints))

	// If only one endpoint is found, further discovery is attempted using the Discovery URL.
	if len(endpoints) == 1 {
		return g.handleSingleEndpointDiscovery(ctx, endpoints[0])
	}

	adjustedEndpoints, err := g.ReplaceHostInEndpoints(endpoints, g.Endpoint)
	if err != nil {
		g.Log.Errorf("Failed to adjust endpoint hosts: %s", err)
		return nil, err
	}

	// For multiple endpoints, adjust their hosts as per user specification.
	return adjustedEndpoints, nil
}

// handleSingleEndpointDiscovery processes a single discovered OPC UA endpoint by performing additional discovery.
//
// When only one endpoint is initially retrieved, this function attempts to discover more endpoints
// using the server’s discovery URL. It ensures that even servers with limited initial endpoint information
// are fully explored. Additionally, it adjusts the discovered endpoints’ hostnames to match user-specified
// configurations to mitigate connectivity issues.
//
// **Why This Function is Needed:**
// - To expand the list of available endpoints beyond the initial discovery when only one endpoint is found.
// - To ensure comprehensive endpoint discovery, especially for servers that may not provide extensive endpoint lists.
func (g *OPCUAInput) handleSingleEndpointDiscovery(ctx context.Context, endpoint *ua.EndpointDescription) ([]*ua.EndpointDescription, error) {
	if endpoint == nil || endpoint.Server == nil || len(endpoint.Server.DiscoveryURLs) == 0 {
		if endpoint != nil && endpoint.Server != nil && len(endpoint.Server.DiscoveryURLs) == 0 { // This is the edge case when there is no discovery URL
			g.Log.Infof("No discovery URL. This is the endpoint: %v", endpoint)
			g.LogEndpoint(endpoint)

			// Adjust the hosts of the endpoint that has no discovery URL
			updatedURL, err := g.ReplaceHostInEndpointURL(endpoint.EndpointURL, g.Endpoint)
			if err != nil {
				return nil, err
			}

			// Update the endpoint URL with the new host.
			endpoint.EndpointURL = updatedURL
			var updatedEndpoints []*ua.EndpointDescription
			updatedEndpoints = append(updatedEndpoints, endpoint)

			return updatedEndpoints, nil

		} else {
			g.Log.Errorf("Invalid endpoint configuration")
		}
		return nil, errors.New("invalid endpoint configuration")
	}

	discoveryURL := endpoint.Server.DiscoveryURLs[0]
	g.Log.Infof("Using discovery URL for further discovery: %s", discoveryURL)

	updatedURL, err := g.ReplaceHostInEndpointURL(discoveryURL, g.Endpoint)
	if err != nil {
		g.Log.Errorf("Failed to adjust endpoint URL: %s", err)
		return nil, err
	}

	moreEndpoints, err := opcua.GetEndpoints(ctx, updatedURL)
	if err != nil {
		return nil, err
	}

	// Adjust the hosts of the newly discovered endpoints.
	adjustedEndpoints, err := g.ReplaceHostInEndpoints(moreEndpoints, g.Endpoint)
	if err != nil {
		g.Log.Errorf("Failed to adjust endpoint hosts: %s", err)
		return nil, err
	}

	return adjustedEndpoints, nil
}

// ReplaceHostInEndpoints updates the host portion of each endpoint’s URL to a specified new host.
//
// This function iterates through a list of OPC UA endpoints and replaces their existing hostnames with a
// new host provided by the user. This is particularly useful for addressing connectivity issues caused
// by DNS name discrepancies or when routing through specific network configurations.
func (g *OPCUAInput) ReplaceHostInEndpoints(endpoints []*ua.EndpointDescription, newHost string) ([]*ua.EndpointDescription, error) {
	var updatedEndpoints []*ua.EndpointDescription

	for _, endpoint := range endpoints {
		updatedURL, err := g.ReplaceHostInEndpointURL(endpoint.EndpointURL, newHost)
		if err != nil {
			return nil, err
		}

		// Update the endpoint URL with the new host.
		endpoint.EndpointURL = updatedURL
		updatedEndpoints = append(updatedEndpoints, endpoint)
	}

	return updatedEndpoints, nil
}

// ReplaceHostInEndpointURL constructs a new OPC UA endpoint URL by replacing the existing host with a new host.
//
// This function parses the provided endpoint URL, substitutes the host component with a new host, and reconstructs
// the URL while preserving the original path and query parameters. It ensures that the modified URL remains
// valid and compatible with OPC UA communication protocols.
func (g *OPCUAInput) ReplaceHostInEndpointURL(endpointURL, newHost string) (string, error) {

	// Remove the "opc.tcp://" prefix to simplify parsing.
	newHost = strings.TrimPrefix(newHost, "opc.tcp://")

	// Remove the "opc.tcp://" prefix to simplify parsing.
	withoutPrefix := strings.TrimPrefix(endpointURL, "opc.tcp://")

	// Identify the first slash ("/") to separate the host from the path.
	slashIndex := strings.Index(withoutPrefix, "/")

	if slashIndex == -1 {
		g.Log.Infof("Endpoint URL does not contain a path: %s", endpointURL)
		// Assume entire URL is a host and directly replace with newHost, retaining the "opc.tcp://" prefix.
		return "opc.tcp://" + newHost, nil
	}

	// Reconstruct the endpoint URL with the new host and the original path/query.
	newURL := fmt.Sprintf("opc.tcp://%s%s", newHost, withoutPrefix[slashIndex:])
	g.Log.Infof("Updated endpoint URL to: %s", newURL)

	return newURL, nil
}

// logCertificateInfo logs detailed information about a server’s TLS certificate.
//
// This function decodes and parses the server’s certificate from PEM format and logs various attributes,
// such as validity periods, DNS names, IP addresses, and URIs. It provides transparency into the security
// credentials presented by the OPC UA server, aiding in the verification of server authenticity and trustworthiness.
func (g *OPCUAInput) logCertificateInfo(certBytes []byte) {
	g.Log.Infof("  Server certificate:")

	// Decode the certificate from base64 to DER format
	block, _ := pem.Decode(certBytes)
	if block == nil {
		g.Log.Errorf("Failed to decode certificate")
		return
	}

	// Parse the DER-format certificate
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		g.Log.Errorf("Failed to parse certificate: " + err.Error())
		return
	}

	// Log the details
	g.Log.Infof("    Not Before: %v", cert.NotBefore)
	g.Log.Infof("    Not After: %v", cert.NotAfter)
	g.Log.Infof("    DNS Names: %v", cert.DNSNames)
	g.Log.Infof("    IP Addresses: %v", cert.IPAddresses)
	g.Log.Infof("    URIs: %v", cert.URIs)
}
