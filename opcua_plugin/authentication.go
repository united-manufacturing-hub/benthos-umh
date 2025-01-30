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

	var highSecurityEndpoints, mediumSecurityEndpoints, lowSecurityEndpoints, noSecurityEndpoints []*ua.EndpointDescription

	for _, endpoint := range endpoints {
		if isUserTokenSupported(endpoint, selectedAuthentication) {
			switch {
			case isSignAndEncryptbasic256Sha256Endpoint(endpoint):
				highSecurityEndpoints = append(highSecurityEndpoints, endpoint)
			case isSignAndEncryptbasic256Endpoint(endpoint):
				mediumSecurityEndpoints = append(mediumSecurityEndpoints, endpoint)
			case isSignAndEncryptbasic128Rsa15Endpoint(endpoint):
				lowSecurityEndpoints = append(lowSecurityEndpoints, endpoint)
			case isNoSecurityEndpoint(endpoint):
				noSecurityEndpoints = append(noSecurityEndpoints, endpoint)
			}
		}
	}

	// Append no security endpoints to the end of the high security endpoints.
	orderedEndpoints := append(highSecurityEndpoints, mediumSecurityEndpoints...)
	orderedEndpoints = append(orderedEndpoints, lowSecurityEndpoints...)
	orderedEndpoints = append(orderedEndpoints, noSecurityEndpoints...)

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
		endpoint.SecurityPolicyURI == "http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256"
}

func isSignAndEncryptbasic256Endpoint(endpoint *ua.EndpointDescription) bool {
	return endpoint.SecurityMode == ua.MessageSecurityModeFromString("SignAndEncrypt") &&
		endpoint.SecurityPolicyURI == "http://opcfoundation.org/UA/SecurityPolicy#Basic256"
}

func isSignAndEncryptbasic128Rsa15Endpoint(endpoint *ua.EndpointDescription) bool {
	return endpoint.SecurityMode == ua.MessageSecurityModeFromString("SignAndEncrypt") &&
		endpoint.SecurityPolicyURI == "http://opcfoundation.org/UA/SecurityPolicy#Basic128Rsa15"
}

// isNoSecurityEndpoint checks if the endpoint has no security configured.
func isNoSecurityEndpoint(endpoint *ua.EndpointDescription) bool {
	return endpoint.SecurityMode == ua.MessageSecurityModeFromString("None") &&
		endpoint.SecurityPolicyURI == "http://opcfoundation.org/UA/SecurityPolicy#None"
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
			if selectedAuthentication == userIdentity.TokenType && endpoint.SecurityPolicyURI == "http://opcfoundation.org/UA/SecurityPolicy#"+securityPolicy && endpoint.SecurityMode == ua.MessageSecurityModeFromString(securityMode) {

				return endpoint, nil
			}
		}
	}

	// Return nil if no suitable endpoint is found.
	return nil, errors.New("no suitable endpoint found")
}
