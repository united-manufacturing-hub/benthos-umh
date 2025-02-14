package opcua_plugin

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/crypto/sha3"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
)

// parseBase64PEMBundle takes a base64-encoded string of a PEM bundle (containing both
// the CERTIFICATE block and the PRIVATE KEY block) and returns a parsed tls.Certificate.
func parseBase64PEMBundle(b64PemBundle string) (*tls.Certificate, error) {
	raw, err := base64.StdEncoding.DecodeString(b64PemBundle)
	if err != nil {
		return nil, fmt.Errorf("base64 decode error: %w", err)
	}

	var certPEM, keyPEM []byte
	rest := raw
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		switch block.Type {
		case "CERTIFICATE":
			certPEM = pem.EncodeToMemory(block)
		case "PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY":
			keyPEM = pem.EncodeToMemory(block)
		}
	}

	if len(certPEM) == 0 || len(keyPEM) == 0 {
		return nil, errors.New("unable to find both CERTIFICATE and PRIVATE KEY in the provided PEM bundle")
	}

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to load X509KeyPair: %w", err)
	}
	return &tlsCert, nil
}

// GetOPCUAClientOptions constructs the OPC UA client options based on the selected endpoint and authentication method.
//
// This function is essential for configuring the OPC UA client with the appropriate security settings,
// authentication credentials, and session parameters. It ensures that the client adheres to the
// security policies of the selected endpoint and supports the desired authentication mechanism.
//
// **Why This Function is Needed:**
// - To dynamically generate client options that match the server’s security requirements.
// - To handle different authentication methods, such as anonymous or username/password-based logins.
// - To generate and include client certificates when using enhanced security policies like Basic256Sha256/Basic256/Basic128Rsa15
func (g *OPCUAInput) GetOPCUAClientOptions(
	selectedEndpoint *ua.EndpointDescription,
	selectedAuthentication ua.UserTokenType,
	discoveryOnly bool,
) (opts []opcua.Option, err error) {

	// Basic security from the endpoint + user token type
	opts = append(opts, opcua.SecurityFromEndpoint(selectedEndpoint, selectedAuthentication))

	// Set additional options based on the authentication method
	switch selectedAuthentication {
	case ua.UserTokenTypeAnonymous:
		g.Log.Infof("Using anonymous login")
	case ua.UserTokenTypeUserName:
		g.Log.Infof("Using username/password login")
		opts = append(opts, opcua.AuthUsername(g.Username, g.Password))
	}

	// If the endpoint's security policy is not None, we must attach a certificate
	if selectedEndpoint.SecurityPolicyURI != ua.SecurityPolicyURINone {
		// Only generate or parse the certificate if we don't already have one cached
		if g.cachedTLSCertificate == nil {
			// If ClientCertificate is provided (base64 of PEM blocks), parse it
			if g.ClientCertificate != "" {
				g.Log.Infof("Using base64-encoded client certificate from configuration")

				cert, err := parseBase64PEMBundle(g.ClientCertificate)
				if err != nil {
					g.Log.Errorf("Failed to parse the provided Base64 certificate/key: %v", err)
					return nil, err
				}
				g.cachedTLSCertificate = cert

			} else {
				g.Log.Infof("No base64-encoded certificate provided, generating a new one...")

				// Generate brand-new certificate
				certPEM, keyPEM, clientName, err := GenerateCertWithMode(
					24*time.Hour*365*10,
					g.SecurityMode,
					g.SecurityPolicy,
					// Provide any random string for the subject, purely internal
				)
				if err != nil {
					g.Log.Errorf("Failed to generate certificate: %v", err)
					return nil, err
				}

				// Parse into a tls.Certificate
				newTLSCert, err := tls.X509KeyPair(certPEM, keyPEM)
				if err != nil {
					g.Log.Errorf("Failed to parse generated certificate: %v", err)
					return nil, err
				}

				g.cachedTLSCertificate = &newTLSCert
				g.Log.Infof("The clients certificate was created, to use an encrypted "+
					"connection please proceed to the OPC-UA Server's configuration and "+
					"trust either all clients or the clients certificate with the client-name: "+
					"'%s'", clientName)

				// Encode the combined PEM (cert + key) as base64 so the user can persist it
				combinedPEM := append(
					append([]byte{}, certPEM...),
					keyPEM...,
				)
				encodedClientCert := base64.StdEncoding.EncodeToString(combinedPEM)
				if !discoveryOnly {
					g.Log.Infof("The client certificate was generated randomly upon startup "+
						"and will change on every restart. To reuse this certificate in future "+
						"sessions, set your 'clientCertificate: %s'", encodedClientCert)
				}
				g.ClientCertificate = encodedClientCert
			}
		}

		// We have g.cachedTLSCertificate set; parse out the private key for the OPC UA library
		pk, ok := g.cachedTLSCertificate.PrivateKey.(*rsa.PrivateKey)
		if !ok {
			g.Log.Errorf("Invalid private key type or not RSA")
			return nil, errors.New("invalid private key type or not RSA")
		}

		// Append the certificate and private key to the client options
		opts = append(opts,
			opcua.PrivateKey(pk),
			opcua.Certificate(g.cachedTLSCertificate.Certificate[0]),
		)
	}

	// Session config
	opts = append(opts, opcua.SessionName("benthos-umh"))
	if g.SessionTimeout > 0 {
		opts = append(opts,
			opcua.SessionTimeout(time.Duration(g.SessionTimeout)*time.Millisecond),
		)
	} else {
		opts = append(opts,
			opcua.SessionTimeout(SessionTimeout),
		)
	}
	opts = append(opts, opcua.ApplicationName("benthos-umh-predefined"))

	// Auto reconnect?
	if g.AutoReconnect {
		opts = append(opts, opcua.AutoReconnect(true))
		reconnectInterval := time.Duration(g.ReconnectIntervalInSeconds) * time.Second
		opts = append(opts, opcua.ReconnectInterval(reconnectInterval))
	}

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

		// Store the fingerprint of the servers certificate
		g.storeServerCertificateFingerprint(endpoint)
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
	g.Log.Infof("Fetched %d endpoints", len(endpoints))
	for i, endpoint := range endpoints {
		g.Log.Infof("Endpoint %d:", i+1)
		g.LogEndpoint(endpoint)
	}
}

// checkForSecurityEndpoints iterates through the endpoint-list and checks
// if there are any endpoints available which have a securityPolicy set, that is
// not "None". If so it returns the first endpoint. The first endpoint will
// always be the most "secure", because we order the endpoints by their security-
// policy and -mode.
func (g *OPCUAInput) checkForSecurityEndpoints(
	ctx context.Context,
	endpoints []*ua.EndpointDescription,
	authType ua.UserTokenType,
) {
	// Oder the endpoints to get the most "secure" endpoint first and provide
	// the user with its information (SecurityPolicy / SecurityMode / Fingerprint).
	orderedEndpoints := g.orderEndpoints(endpoints, authType)

	for _, ep := range orderedEndpoints {
		if isUserTokenSupported(ep, authType) {
			if !isNoSecurityEndpoint(ep) {
				securityMode := strings.TrimPrefix(ep.SecurityMode.String(), "MessageSecurityMode")
				securityPolicy := strings.TrimPrefix(ep.SecurityPolicyURI, ua.SecurityPolicyURIPrefix)

				// we use the openConnection func here to pre-create the cert and with
				// the connect-attempt we send it over to the OPC-UA server so the user,
				// can proceed to trust this certificate and only has to copy the snippet
				// below
				c, err := g.openConnection(ctx, ep, authType, true)
				if err == nil {
					c.Close(ctx)
				}

				g.Log.Infof("Secure endpoint detected. To ensure that your connection is "+
					"fully encrypted, please set the following fields in your configuration:\n"+
					"- securityMode: '%s'\n"+
					"- securityPolicy: '%s'\n"+
					"- serverCertificateFingerprint: '%s'\n"+
					"- clientCertificate: '%s'\n"+
					"These settings ensure that data is encrypted and that the server's "+
					"identity is verified. Without them, encryption is not fully enabled, "+
					"which could expose your connection to security risks.",
					securityMode, securityPolicy, g.ServerCertificates[ep], g.ClientCertificate)
				return
			}
		}
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

// checkServerCertificateFingerprint will check the endpoints provided by the server if the fingerprint is set
//
// This function will check if the endpoint's certificate fingerprint does
// match with the one provided by the user. Therefore we can check if the
// server is "trusted". This should only be called if we are trying to use
// an "encrypted" connection.
// On top of that if no fingerprint provided it informs the user that it
// would be safer using this and writes back the server certificates fingerprint.
//
// **Why This Function is Needed:**
//   - To ensure the client connects to the correct server(when encrypted)
//   - To inform the user using a Fingerprint of the Server's certificate would
//     be more secure
func (g *OPCUAInput) checkServerCertificateFingerprint(endpoint *ua.EndpointDescription) error {

	if g.ServerCertificates[endpoint] == "" {
		return fmt.Errorf("The servers endpoint '%s' doesn't provide any server certificate. "+
			"This is crucial in order to get a reliable OPC-UA connection, please "+
			"check your server's certificates and make sure it exists.", endpoint.EndpointURL)
	}

	switch {
	case g.ServerCertificateFingerprint != "":
		// Return error with information about the mismatch of the endpoints
		// certificate fingerprint.
		if g.ServerCertificates[endpoint] != g.ServerCertificateFingerprint {
			return fmt.Errorf("fingerprint mismatch for endpoint %s\n"+
				"Expected: '%s'\n"+
				"Got: '%s'\n\n"+
				"Either the server's certificate was intentionally updated, or you are connecting to the wrong server.\n"+
				"If intentional, please set the new 'serverCertificateFingerprint' in your configuration.\n"+
				"Otherwise, double-check your security settings.",
				endpoint.EndpointURL, g.ServerCertificateFingerprint, g.ServerCertificates[endpoint])
		}
	default:
		return fmt.Errorf(
			"No 'serverCertificateFingerprint' was provided. "+
				"We strongly recommend specifying 'serverCertificateFingerprint=%s' to verify the server's identity "+
				"and avoid potential security risks. Future releases may escalate this to a warning that prevents deployment.", g.ServerCertificates[endpoint],
		)
	}
	return nil
}

// storeServerCertificateFingerprint will store the endpoint and it's certificates fingerprint
//
// Store the OPC-UA servers endpoint and it's certificates fingerprint in a map
// in order to check the specific fingerprint of that endpoint. This should
// prevent any edge-cases where certificates could differ for endpoints of the
// same OPC-UA server.
//
// **Why this Function is needed:**
//   - to store each endpoint of the OPC-UA-Server and it's corresponding fingerprint
//   - to prevent edge-cases where certificates could differ for its endpoints
func (g *OPCUAInput) storeServerCertificateFingerprint(endpoint *ua.EndpointDescription) error {

	pemCert := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: endpoint.ServerCertificate,
	})

	// decode the certificate from base64 to DER format
	block, _ := pem.Decode(pemCert)
	if block == nil {
		return fmt.Errorf("Could not decode server certificate.")
	}

	// parse the DER-format certificate
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("Error while parsing server certificate.")
	}

	// calculating the checksum of the certificate (sha3 is needed here)
	// and convert the array into a slice for encoding
	shaFingerprint := sha3.Sum512(cert.Raw)

	g.ServerCertificates[endpoint] = hex.EncodeToString(shaFingerprint[:])

	return nil
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

// connectWithoutSecurity attempts to connect to the available OPC UA endpoints
// using the specified authentication type. It orders the endpoints based on the expected
// success of the connection and tries to establish a connection with each endpoint.
//
// Parameters:
// - ctx: The context for managing the connection lifecycle.
// - endpoints: A slice of endpoint descriptions to attempt connections to.
// - authType: The type of user token to use for authentication.
//
// Returns:
// - *opcua.Client: A pointer to the connected OPC UA client, if successful.
// - error: An error object if the connection attempt fails.
func (g *OPCUAInput) connectWithoutSecurity(
	ctx context.Context,
	endpoints []*ua.EndpointDescription,
	authType ua.UserTokenType,
) (*opcua.Client, error) {

	// Order the endpoints based on the expected success of the connection
	// orderedEndpoints := g.orderEndpoints(endpoints, authType)
	for _, currentEndpoint := range endpoints {

		// Connect to the "None"-security endpoint if found in endpoints-list.
		if isNoSecurityEndpoint(currentEndpoint) {
			c, err := g.openConnection(ctx, currentEndpoint, authType, false)
			if err == nil {
				return c, nil
			}

			// case where an error occured
			g.CloseExpected(ctx)
			g.Log.Infof("Failed to connect, but continue anyway: %v", err)

			if errors.Is(err, ua.StatusBadUserAccessDenied) || errors.Is(err, ua.StatusBadTooManySessions) {
				var timeout time.Duration
				// Adding a sleep to prevent immediate re-connect
				// In the case of ua.StatusBadUserAccessDenied, the session is for some reason not properly closed on the server
				// If we were to re-connect, we could overload the server with too many sessions as the default timeout is 10 seconds
				if g.SessionTimeout > 0 {
					timeout = time.Duration(g.SessionTimeout * int(time.Millisecond))
				} else {
					timeout = SessionTimeout
				}
				g.Log.Errorf("Encountered unrecoverable error. Waiting before trying to re-connect to prevent overloading the server: %v with timeout %v", err, timeout)
				time.Sleep(timeout)
				return nil, err
			}

			if errors.Is(err, ua.StatusBadTimeout) {
				g.Log.Infof("Selected endpoint timed out. Selecting next one: %v", currentEndpoint)
			}
		}

	}
	return nil, errors.New("error could not connect successfully to any endpoint")
}

// encryptedConnect establishes a connection to an OPC UA server using the specified
// security mode, policy and server certificate fingerprint.
// It takes a context for cancellation, a list of endpoint descriptions, and an authentication type.
// It returns an OPC UA client if the connection is successful, or an error if it fails.
// If the SecurityMode is not SignAndEncrypt or the Policy is set to None we return early.
// This is ONLY for encrypted connections.
//
// Parameters:
// - ctx: context.Context - The context for managing cancellation and timeouts.
// - endpoints: []*ua.EndpointDescription - A list of endpoint descriptions to connect to.
// - authType: ua.UserTokenType - The type of user authentication to use.
//
// Returns:
// - *opcua.Client - The connected OPC UA client.
// - error - An error if the connection fails.
func (g *OPCUAInput) encryptedConnect(
	ctx context.Context,
	endpoints []*ua.EndpointDescription,
	authType ua.UserTokenType,
) (*opcua.Client, error) {
	g.Log.Info("Trying to strictly connect with encryption")

	foundEndpoint, err := g.getEndpointIfExists(endpoints, authType, g.SecurityMode, g.SecurityPolicy)
	if err != nil {
		g.Log.Errorf("Failed to get endpoint: %s", err)
		return nil, err
	}

	if foundEndpoint == nil {
		g.Log.Errorf("No suitable endpoint found")
		return nil, errors.New("no suitable endpoint found")
	}

	g.Log.Infof("Establishing a strict connection to endpoint: '%s' using securityMode: '%s' and "+
		"securityPolicy: '%s'.", foundEndpoint.EndpointURL, g.SecurityMode, g.SecurityPolicy)

	// Check on the Server Certificate's Fingerprint
	err = g.checkServerCertificateFingerprint(foundEndpoint)
	if err != nil {
		g.Log.Infof("The server certificate fingerprint does not match or is missing. "+
			"To ensure that you are connecting to the intended server securely, please verify "+
			"that your configuration includes the correct serverCertificateFingerprint. "+
			"This value is critical for confirming the server’s identity and enabling encryption. "+
			"Error details: %s", err)
		// later on we might escalate this and return the err
		// For now we only log this info-message so the user could check the correct
		// Fingerprint here, since this is not fundamentally important for the
		// encryption.
	}

	c, err := g.openConnection(ctx, foundEndpoint, authType, false)
	if err != nil {
		_ = g.Close(ctx)
		g.Log.Errorf("Failed to connect: %v", err)
		return nil, err
	}

	return c, nil
}

// unencryptedConnect is used as a fallback, so if the user decides not to use
// an encrypted connection this will be executed and tries to connect to any
// endpoints ("encrypted">none-encrypted)
func (g *OPCUAInput) unencryptedConnect(
	ctx context.Context,
	endpoints []*ua.EndpointDescription,
	authType ua.UserTokenType,
) (*opcua.Client, error) {

	g.Log.Infof("Establishing an unencrypted connection...")

	// If there are no endpoints found or the `directConnect`-flag is set try to
	// directly connect to the endpoint.
	if g.DirectConnect || len(endpoints) == 0 {
		c, err := g.connectToDirectEndpoint(ctx, authType)
		if err != nil {
			g.Log.Infof("error while connecting to the endpoint directly: %v", err)
			return nil, err
		}
		g.Client = c
		return c, nil
	}

	// Otherwise just connect without security
	c, err := g.connectWithoutSecurity(ctx, endpoints, authType)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// connectToDirectEndpoint establishes a connection to an OPC UA server using a direct endpoint.
// It takes a context for managing the connection lifecycle and an authentication type for user token.
// It returns an OPC UA client instance and an error if the connection fails.
func (g *OPCUAInput) connectToDirectEndpoint(ctx context.Context, authType ua.UserTokenType) (*opcua.Client, error) {

	g.Log.Infof("Directly connecting to the endpoint %s", g.Endpoint)

	// Create a new endpoint description
	// It will never be used directly by the OPC UA library, but we need it for our internal helper functions
	// such as GetOPCUAClientOptions
	securityMode := ua.MessageSecurityModeNone
	if g.SecurityMode != "" {
		securityMode = ua.MessageSecurityModeFromString(g.SecurityMode)
	}

	securityPolicyURI := ua.SecurityPolicyURINone
	if g.SecurityPolicy != "" {
		securityPolicyURI = fmt.Sprintf("http://opcfoundation.org/UA/SecurityPolicy#%s", g.SecurityPolicy)
	}

	directEndpoint := &ua.EndpointDescription{
		EndpointURL:       g.Endpoint,
		SecurityMode:      securityMode,
		SecurityPolicyURI: securityPolicyURI,
	}

	// If we are not using a NoSecurityEndpoint meaning we are using an "encrypted"
	// endpoint, we should check if a 'serverCertificateFingerprint' was provided
	// and otherwise let the user know, he should possibly use this option.
	if !isNoSecurityEndpoint(directEndpoint) {
		err := g.checkServerCertificateFingerprint(directEndpoint)
		if err != nil {
			g.Log.Infof("The server certificate fingerprint does not match or is missing. "+
				"To ensure that you are connecting to the intended server securely, please verify "+
				"that your configuration includes the correct serverCertificateFingerprint. "+
				"This value is critical for confirming the server’s identity and enabling encryption. "+
				"Error details: %s", err)
			// later on we might escalate this and return the err
		}
	}

	c, err := g.openConnection(ctx, directEndpoint, authType, false)
	if err != nil {
		_ = g.Close(ctx)
		g.Log.Errorf("Failed to connect: %v", err)
		return nil, err
	}

	return c, nil
}

// connect establishes a connection for the OPCUAInput instance.
// It takes a context to handle cancellation and timeouts.
// On successful connection establishment, the function sets the connection to g.Client
//
// Parameters:
// - ctx: The context to control the connection lifecycle.
//
// Returns:
// - error: An error if the connection fails, otherwise nil.
func (g *OPCUAInput) connect(ctx context.Context) error {
	var (
		c         *opcua.Client
		endpoints []*ua.EndpointDescription
		err       error
	)

	// Step 1: Retrieve all available endpoints from the OPC UA server
	// Iterate through DiscoveryURLs until we receive a list of all working endpoints including their potential security modes, etc.

	endpoints, err = g.FetchAllEndpoints(ctx)
	if err != nil {
		g.Log.Infof("Trying to connect to the endpoint as a last resort measure")
		return err
	}

	// Step 2 (optional): Log details of each discovered endpoint for debugging
	g.LogEndpoints(endpoints)

	// Step 3: Determine the authentication method to use.
	// Default to Anonymous if neither username nor password is provided.
	selectedAuthentication := g.getUserAuthenticationType()

	// NOTE: strictConnect only if:
	//				-	SecurityMode
	//				-	SecurityPolicy
	//				-	ServerCertificateFingerprint
	//				are correctly set, if not we will use 'unencryptedConnect' and
	//				provide the user with some information.
	if g.isSecuritySelected() {
		c, err = g.encryptedConnect(ctx, endpoints, selectedAuthentication)
		if err != nil {
			g.Log.Infof("Error while connecting using securityMode: '%s',"+
				"securityPolicy: '%s', serverCertificateFingerprint: '%s'. err:%v",
				g.SecurityMode, g.SecurityPolicy, g.ServerCertificateFingerprint, err)
			return err
		}

		g.Client = c
		return nil
	}

	// Step 4: Check if there is any "security"-based endpoint available and if
	// so print out some information on how to use it.
	g.checkForSecurityEndpoints(ctx, endpoints, selectedAuthentication)

	c, err = g.unencryptedConnect(ctx, endpoints, selectedAuthentication)
	if err != nil {
		return err
	}

	// If no connection was successful, return an error
	if c == nil {
		g.Log.Errorf("Failed to connect to any endpoint")
		return errors.New("failed to connect to any endpoint")
	}

	g.Client = c

	return nil
}

// We can use this function to prevent getting duplicated code, whenever we are
// trying to open a connection to the OPC-UA server
// On success:
//   - it returns the client
//
// On failure:
//   - it returns the error
func (g *OPCUAInput) openConnection(
	ctx context.Context,
	endpoint *ua.EndpointDescription,
	authType ua.UserTokenType,
	discoveryOnly bool,
) (*opcua.Client, error) {

	opts, err := g.GetOPCUAClientOptions(endpoint, authType, discoveryOnly)
	if err != nil {
		g.Log.Errorf("Failed to get OPC UA client options: %s", err)
		return nil, err
	}

	c, err := opcua.NewClient(endpoint.EndpointURL, opts...)
	if err != nil {
		g.Log.Errorf("Failed to create a new client")
		return nil, err
	}

	// Connect to the endpoint
	// If connection fails, then continue to the next endpoint
	// Connect to the selected endpoint
	err = c.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return c, nil
}
