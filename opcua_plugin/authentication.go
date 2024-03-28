package opcua_plugin

import (
	"sort"

	"errors"

	"github.com/gopcua/opcua/ua"
)

// getReasonableEndpoint selects an appropriate OPC UA endpoint based on specified criteria.
// It filters the endpoints based on the authentication method, security mode, and security policy.
// If no suitable endpoint is found, it returns nil.
// This can potentially be replaced by SelectEndpoint function in goopcua package
func (g *OPCUAInput) getReasonableEndpoint(
	endpoints []*ua.EndpointDescription,
	selectedAuthentication ua.UserTokenType,
	disableEncryption bool,
	securityMode string,
	securityPolicy string,
) *ua.EndpointDescription {

	// Return nil immediately if no endpoints are provided.
	if len(endpoints) == 0 {
		return nil
	}

	// Sort endpoints in descending order of security level.
	sort.Sort(sort.Reverse(bySecurityLevel(endpoints)))

	// Iterate over each endpoint to find a matching one.
	for _, endpoint := range endpoints {

		// Check each user identity token in the endpoint.
		for _, userIdentity := range endpoint.UserIdentityTokens {

			// Match the endpoint with the selected authentication type.
			if selectedAuthentication == userIdentity.TokenType {

				// Check for encryption requirements.
				if disableEncryption && endpoint.SecurityMode == ua.MessageSecurityModeFromString("None") {
					// Return if encryption is disabled and the endpoint has no security.
					return endpoint
				} else if !disableEncryption {
					// Handle the case where encryption is not disabled.

					// Always try to use the security level SignAndEncrypt and Basic256Sha256
					// because this is what works at the most servers
					if securityMode == "" && securityPolicy == "" {
						if endpoint.SecurityMode == ua.MessageSecurityModeFromString("SignAndEncrypt") && endpoint.SecurityPolicyURI == "http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256" {
							return endpoint
						}
					} else { // If security mode or security policy is specified, then try to match them
						// Check for a specific security mode if provided.
						if securityMode != "" && endpoint.SecurityMode == ua.MessageSecurityModeFromString(securityMode) {

							// Check for a specific security policy if provided.
							if securityPolicy != "" && endpoint.SecurityPolicyURI == "http://opcfoundation.org/UA/SecurityPolicy#"+securityPolicy {
								return endpoint
							} else if securityPolicy == "" {
								// If no specific security policy is needed, return the endpoint.
								return endpoint
							}
							// Continue searching if the security policy doesn't match.
						} else if securityMode == "" {
							// If no specific security policy is needed, return the endpoint.
							return endpoint
						}
					}

				}
				// Continue searching if other conditions are not met.
			}
		}
	}

	// Return nil if no suitable endpoint is found.
	return nil
}

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
		endpoint.SecurityPolicyURI == "http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256"
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

// Copy paste from opcua library
type bySecurityLevel []*ua.EndpointDescription

func (a bySecurityLevel) Len() int           { return len(a) }
func (a bySecurityLevel) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a bySecurityLevel) Less(i, j int) bool { return a[i].SecurityLevel < a[j].SecurityLevel }

// Copy-=paste end
