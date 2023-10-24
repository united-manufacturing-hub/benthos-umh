package plugin

import (
	"sort"

	"github.com/gopcua/opcua/ua"
)

func (g *OPCUAInput) getReasonableEndpoint(endpoints []*ua.EndpointDescription, selectedAuthentication ua.UserTokenType) *ua.EndpointDescription {
	if len(endpoints) == 0 {
		return nil
	}

	sort.Sort(sort.Reverse(bySecurityLevel(endpoints)))

	for _, p := range endpoints {
		// Take the first endpoint that supports our selectedAuthentication
		for _, userIdentity := range p.UserIdentityTokens {

			if selectedAuthentication == userIdentity.TokenType {
				// We have a winner
				return p
			}
		}

	}
	return nil

}

// Copy paste from opcua library
type bySecurityLevel []*ua.EndpointDescription

func (a bySecurityLevel) Len() int           { return len(a) }
func (a bySecurityLevel) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a bySecurityLevel) Less(i, j int) bool { return a[i].SecurityLevel < a[j].SecurityLevel }

// Copy-=paste end
