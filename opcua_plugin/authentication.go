package opcua_plugin

import (
	"errors"

	"github.com/gopcua/opcua/ua"
)

// orderEndpoints prioritizes the endpoints based on their security settings,
// aiming to select the most secure options first.
// It orders endpoints by preferring those with SignAndEncrypt and Basic256Sha256 security settings,
// and then falling back to None security settings if specified.
// Other options are discarded. If they want to be used, they should be specified in the configuration.
func (g *OPCUAInput) orderEndpoints(
	endpoints []*ua.EndpointDescription,
	selectedAuthentication ua.UserTokenType,
) []*ua.EndpointDescription {

	var highSecurityEndpoints, noSecurityEndpoints []*ua.EndpointDescription

	for _, endpoint := range endpoints {
		if isUserTokenSupported(endpoint, selectedAuthentication) {
			switch {
			case isSignAndEncryptbasic256Sha256Endpoint(endpoint):
				highSecurityEndpoints = append(highSecurityEndpoints, endpoint)
			case isNoSecurityEndpoint(endpoint):
				noSecurityEndpoints = append(noSecurityEndpoints, endpoint)
			}
		}
	}

	// Append no security endpoints to the end of the high security endpoints.
	orderedEndpoints := append(highSecurityEndpoints, noSecurityEndpoints...)

	return orderedEndpoints
}

// isUserTokenSupported checks if the endpoint supports the selected user token authentication.
func isUserTokenSupported(endpoint *ua.EndpointDescription, selectedAuth ua.UserTokenType) bool {
	for _, token := range endpoint.UserIdentityTokens {
		if selectedAuth == token.TokenType {
			return true
		}
	}
	return false
}

// isSignAndEncryptbasic256Sha256Endpoint checks if the endpoint is configured with SignAndEncrypt and Basic256Sha256 security.
func isSignAndEncryptbasic256Sha256Endpoint(endpoint *ua.EndpointDescription) bool {
	return endpoint.SecurityMode == ua.MessageSecurityModeFromString("SignAndEncrypt") &&
		endpoint.SecurityPolicyURI == ua.FormatSecurityPolicyURI("Basic256Sha256")
}

// isNoSecurityEndpoint checks if the endpoint has no security configured.
func isNoSecurityEndpoint(endpoint *ua.EndpointDescription) bool {
	return endpoint.SecurityMode == ua.MessageSecurityModeFromString("None") &&
		endpoint.SecurityPolicyURI == ua.FormatSecurityPolicyURI("None")
}

// getEndpointIfExists searches within the provided endpoints for a suitable OPC UA endpoint.
// If the endpoint is not found, it returns an error.
func (g *OPCUAInput) getEndpointIfExists(
	endpoints []*ua.EndpointDescription,
	selectedAuthentication ua.UserTokenType,
	securityMode string,
	securityPolicy string,
) (*ua.EndpointDescription, error) {

	// Return nil immediately if no endpoints are provided.
	if len(endpoints) == 0 {
		return nil, errors.New("no endpoints provided")
	}

	// Iterate over each endpoint to find a matching one.
	for _, endpoint := range endpoints {

		// Check each user identity token in the endpoint.
		for _, userIdentity := range endpoint.UserIdentityTokens {

			// Match the endpoint with the selected authentication type.
			if selectedAuthentication == userIdentity.TokenType &&
				endpoint.SecurityPolicyURI == ua.FormatSecurityPolicyURI(securityPolicy) &&
				endpoint.SecurityMode == ua.MessageSecurityModeFromString(securityMode) {

				return endpoint, nil
			}
		}
	}

	// Return nil if no suitable endpoint is found.
	return nil, errors.New("no suitable endpoint found")
}

func (g *OPCUAInput) getUserAuthenticationType() ua.UserTokenType {
	// Here we can add UserTokenTypeCertificate and UserTokenTypeIssuedToken later
	switch {
	case g.Username != "" && g.Password != "":
		return ua.UserTokenTypeUserName
	default:
		return ua.UserTokenTypeAnonymous
	}
}

// explicitely check if security is selected + only allow specified settings
func (g *OPCUAInput) isSecuritySelected() bool {
	var (
		securityModeOK   bool
		securityPolicyOK bool
	)
	switch g.SecurityMode {
	case "Sign":
		securityModeOK = true
	case "SignAndEncrypt":
		securityModeOK = true
	case "None":
		g.Log.Infof("Invalid securityMode '%s'. For secure (encrypted) connections "+
			", please set securityMode to 'SignAndEncrypt' or 'Sign'. This setting is "+
			"required to enable encryption and verify the server's certificate.", g.SecurityMode)
		securityModeOK = false
	default:
		securityModeOK = false
	}

	switch g.SecurityPolicy {
	case "Basic128Rsa15":
		securityPolicyOK = true
	case "Basic256":
		securityPolicyOK = true
	case "Basic256Sha256":
		securityPolicyOK = true
	case "None":
		g.Log.Infof("Invalid securityPolicy '%s'. For encrypted communication, "+
			"please choose a valid policy (e.g., 'Basic256Sha256', 'Basic256', or "+
			"'Basic128Rsa15') that your server supports.", g.SecurityPolicy)
		securityPolicyOK = false
	default:
		securityPolicyOK = false
	}

	if securityModeOK && securityPolicyOK && g.ServerCertificateFingerprint != "" {
		return true
	}
	return false
}
