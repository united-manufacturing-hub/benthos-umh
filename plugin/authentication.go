package plugin

import (
	"sort"

	"github.com/gopcua/opcua/ua"
)

// getReasonableEndpoint selects an appropriate OPC UA endpoint based on specified criteria.
// It filters the endpoints based on the authentication method, security mode, and security policy.
// If no suitable endpoint is found, it returns nil.
// This can potentially be replaced by 
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
				// Continue searching if other conditions are not met.
			}
		}
	}

	// Return nil if no suitable endpoint is found.
	return nil
}

// Copy paste from opcua library
type bySecurityLevel []*ua.EndpointDescription

func (a bySecurityLevel) Len() int           { return len(a) }
func (a bySecurityLevel) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a bySecurityLevel) Less(i, j int) bool { return a[i].SecurityLevel < a[j].SecurityLevel }

// Copy-=paste end
