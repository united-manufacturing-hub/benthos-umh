// Copyright 2023 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plugin

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"testing"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"github.com/stretchr/testify/assert"
)

/*

TODO:
=== RUN   TestAgainstSimulator/Logging_Endpoints
    opcua_test.go:154: Endpoint 1:
    opcua_test.go:155:   EndpointURL: opc.tcp://ba26b088a1b0:50000/
    opcua_test.go:156:   SecurityMode: MessageSecurityModeSignAndEncrypt
    opcua_test.go:157:   SecurityPolicyURI: http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256
    opcua_test.go:158:   TransportProfileURI: http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary
    opcua_test.go:159:   SecurityLevel: 106
    opcua_test.go:163:   Server ApplicationURI: urn:OpcPlc:ba26b088a1b0
    opcua_test.go:164:   Server ProductURI: https://github.com/azure-samples/iot-edge-opc-plc
    opcua_test.go:165:   Server ApplicationName: OpcPlc
    opcua_test.go:166:   Server ApplicationType: ApplicationTypeServer
    opcua_test.go:167:   Server GatewayServerURI:
    opcua_test.go:168:   Server DiscoveryProfileURI:
    opcua_test.go:169:   Server DiscoveryURLs: [opc.tcp://ba26b088a1b0:50000/]
    opcua_test.go:788:   Server certificate:
    opcua_test.go:805:     Not Before: 2023-11-13 00:00:00 +0000 UTC
    opcua_test.go:806:     Not After: 2024-11-13 00:00:00 +0000 UTC
    opcua_test.go:807:     DNS Names: [ba26b088a1b0]
    opcua_test.go:808:     IP Addresses: []
    opcua_test.go:809:     URIs: [urn:OpcPlc:ba26b088a1b0]
    opcua_test.go:184:   UserIdentityToken 1:
    opcua_test.go:185:     PolicyID: 1
    opcua_test.go:186:     TokenType: UserTokenTypeAnonymous
    opcua_test.go:187:     IssuedTokenType:
    opcua_test.go:188:     IssuerEndpointURL:
    opcua_test.go:184:   UserIdentityToken 2:
    opcua_test.go:185:     PolicyID: 2
    opcua_test.go:186:     TokenType: UserTokenTypeUserName
    opcua_test.go:187:     IssuedTokenType:
    opcua_test.go:188:     IssuerEndpointURL:
    opcua_test.go:184:   UserIdentityToken 3:
    opcua_test.go:185:     PolicyID: 3
    opcua_test.go:186:     TokenType: UserTokenTypeCertificate
    opcua_test.go:187:     IssuedTokenType:
    opcua_test.go:188:     IssuerEndpointURL:
    opcua_test.go:154: Endpoint 2:
    opcua_test.go:155:   EndpointURL: opc.tcp://ba26b088a1b0:50000/
    opcua_test.go:156:   SecurityMode: MessageSecurityModeSignAndEncrypt
    opcua_test.go:157:   SecurityPolicyURI: http://opcfoundation.org/UA/SecurityPolicy#Aes128_Sha256_RsaOaep
    opcua_test.go:158:   TransportProfileURI: http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary
    opcua_test.go:159:   SecurityLevel: 108
    opcua_test.go:163:   Server ApplicationURI: urn:OpcPlc:ba26b088a1b0
    opcua_test.go:164:   Server ProductURI: https://github.com/azure-samples/iot-edge-opc-plc
    opcua_test.go:165:   Server ApplicationName: OpcPlc
    opcua_test.go:166:   Server ApplicationType: ApplicationTypeServer
    opcua_test.go:167:   Server GatewayServerURI:
    opcua_test.go:168:   Server DiscoveryProfileURI:
    opcua_test.go:169:   Server DiscoveryURLs: [opc.tcp://ba26b088a1b0:50000/]
    opcua_test.go:788:   Server certificate:
    opcua_test.go:805:     Not Before: 2023-11-13 00:00:00 +0000 UTC
    opcua_test.go:806:     Not After: 2024-11-13 00:00:00 +0000 UTC
    opcua_test.go:807:     DNS Names: [ba26b088a1b0]
    opcua_test.go:808:     IP Addresses: []
    opcua_test.go:809:     URIs: [urn:OpcPlc:ba26b088a1b0]
    opcua_test.go:184:   UserIdentityToken 1:
    opcua_test.go:185:     PolicyID: 4
    opcua_test.go:186:     TokenType: UserTokenTypeAnonymous
    opcua_test.go:187:     IssuedTokenType:
    opcua_test.go:188:     IssuerEndpointURL:
    opcua_test.go:184:   UserIdentityToken 2:
    opcua_test.go:185:     PolicyID: 5
    opcua_test.go:186:     TokenType: UserTokenTypeUserName
    opcua_test.go:187:     IssuedTokenType:
    opcua_test.go:188:     IssuerEndpointURL:
    opcua_test.go:184:   UserIdentityToken 3:
    opcua_test.go:185:     PolicyID: 6
    opcua_test.go:186:     TokenType: UserTokenTypeCertificate
    opcua_test.go:187:     IssuedTokenType:
    opcua_test.go:188:     IssuerEndpointURL:
    opcua_test.go:154: Endpoint 3:
    opcua_test.go:155:   EndpointURL: opc.tcp://ba26b088a1b0:50000/
    opcua_test.go:156:   SecurityMode: MessageSecurityModeSignAndEncrypt
    opcua_test.go:157:   SecurityPolicyURI: http://opcfoundation.org/UA/SecurityPolicy#Aes256_Sha256_RsaPss
    opcua_test.go:158:   TransportProfileURI: http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary
    opcua_test.go:159:   SecurityLevel: 110
    opcua_test.go:163:   Server ApplicationURI: urn:OpcPlc:ba26b088a1b0
    opcua_test.go:164:   Server ProductURI: https://github.com/azure-samples/iot-edge-opc-plc
    opcua_test.go:165:   Server ApplicationName: OpcPlc
    opcua_test.go:166:   Server ApplicationType: ApplicationTypeServer
    opcua_test.go:167:   Server GatewayServerURI:
    opcua_test.go:168:   Server DiscoveryProfileURI:
    opcua_test.go:169:   Server DiscoveryURLs: [opc.tcp://ba26b088a1b0:50000/]
    opcua_test.go:788:   Server certificate:
    opcua_test.go:805:     Not Before: 2023-11-13 00:00:00 +0000 UTC
    opcua_test.go:806:     Not After: 2024-11-13 00:00:00 +0000 UTC
    opcua_test.go:807:     DNS Names: [ba26b088a1b0]
    opcua_test.go:808:     IP Addresses: []
    opcua_test.go:809:     URIs: [urn:OpcPlc:ba26b088a1b0]
    opcua_test.go:184:   UserIdentityToken 1:
    opcua_test.go:185:     PolicyID: 7
    opcua_test.go:186:     TokenType: UserTokenTypeAnonymous
    opcua_test.go:187:     IssuedTokenType:
    opcua_test.go:188:     IssuerEndpointURL:
    opcua_test.go:184:   UserIdentityToken 2:
    opcua_test.go:185:     PolicyID: 8
    opcua_test.go:186:     TokenType: UserTokenTypeUserName
    opcua_test.go:187:     IssuedTokenType:
    opcua_test.go:188:     IssuerEndpointURL:
    opcua_test.go:184:   UserIdentityToken 3:
    opcua_test.go:185:     PolicyID: 9
    opcua_test.go:186:     TokenType: UserTokenTypeCertificate
    opcua_test.go:187:     IssuedTokenType:
    opcua_test.go:188:     IssuerEndpointURL:
    opcua_test.go:154: Endpoint 4:
    opcua_test.go:155:   EndpointURL: opc.tcp://ba26b088a1b0:50000/
    opcua_test.go:156:   SecurityMode: MessageSecurityModeSign
    opcua_test.go:157:   SecurityPolicyURI: http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256
    opcua_test.go:158:   TransportProfileURI: http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary
    opcua_test.go:159:   SecurityLevel: 6
    opcua_test.go:163:   Server ApplicationURI: urn:OpcPlc:ba26b088a1b0
    opcua_test.go:164:   Server ProductURI: https://github.com/azure-samples/iot-edge-opc-plc
    opcua_test.go:165:   Server ApplicationName: OpcPlc
    opcua_test.go:166:   Server ApplicationType: ApplicationTypeServer
    opcua_test.go:167:   Server GatewayServerURI:
    opcua_test.go:168:   Server DiscoveryProfileURI:
    opcua_test.go:169:   Server DiscoveryURLs: [opc.tcp://ba26b088a1b0:50000/]
    opcua_test.go:788:   Server certificate:
    opcua_test.go:805:     Not Before: 2023-11-13 00:00:00 +0000 UTC
    opcua_test.go:806:     Not After: 2024-11-13 00:00:00 +0000 UTC
    opcua_test.go:807:     DNS Names: [ba26b088a1b0]
    opcua_test.go:808:     IP Addresses: []
    opcua_test.go:809:     URIs: [urn:OpcPlc:ba26b088a1b0]
    opcua_test.go:184:   UserIdentityToken 1:
    opcua_test.go:185:     PolicyID: 10
    opcua_test.go:186:     TokenType: UserTokenTypeAnonymous
    opcua_test.go:187:     IssuedTokenType:
    opcua_test.go:188:     IssuerEndpointURL:
    opcua_test.go:184:   UserIdentityToken 2:
    opcua_test.go:185:     PolicyID: 11
    opcua_test.go:186:     TokenType: UserTokenTypeUserName
    opcua_test.go:187:     IssuedTokenType:
    opcua_test.go:188:     IssuerEndpointURL:
    opcua_test.go:184:   UserIdentityToken 3:
    opcua_test.go:185:     PolicyID: 12
    opcua_test.go:186:     TokenType: UserTokenTypeCertificate
    opcua_test.go:187:     IssuedTokenType:
    opcua_test.go:188:     IssuerEndpointURL:
    opcua_test.go:154: Endpoint 5:
    opcua_test.go:155:   EndpointURL: opc.tcp://ba26b088a1b0:50000/
    opcua_test.go:156:   SecurityMode: MessageSecurityModeSign
    opcua_test.go:157:   SecurityPolicyURI: http://opcfoundation.org/UA/SecurityPolicy#Aes128_Sha256_RsaOaep
    opcua_test.go:158:   TransportProfileURI: http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary
    opcua_test.go:159:   SecurityLevel: 8
    opcua_test.go:163:   Server ApplicationURI: urn:OpcPlc:ba26b088a1b0
    opcua_test.go:164:   Server ProductURI: https://github.com/azure-samples/iot-edge-opc-plc
    opcua_test.go:165:   Server ApplicationName: OpcPlc
    opcua_test.go:166:   Server ApplicationType: ApplicationTypeServer
    opcua_test.go:167:   Server GatewayServerURI:
    opcua_test.go:168:   Server DiscoveryProfileURI:
    opcua_test.go:169:   Server DiscoveryURLs: [opc.tcp://ba26b088a1b0:50000/]
    opcua_test.go:788:   Server certificate:
    opcua_test.go:805:     Not Before: 2023-11-13 00:00:00 +0000 UTC
    opcua_test.go:806:     Not After: 2024-11-13 00:00:00 +0000 UTC
    opcua_test.go:807:     DNS Names: [ba26b088a1b0]
    opcua_test.go:808:     IP Addresses: []
    opcua_test.go:809:     URIs: [urn:OpcPlc:ba26b088a1b0]
    opcua_test.go:184:   UserIdentityToken 1:
    opcua_test.go:185:     PolicyID: 13
    opcua_test.go:186:     TokenType: UserTokenTypeAnonymous
    opcua_test.go:187:     IssuedTokenType:
    opcua_test.go:188:     IssuerEndpointURL:
    opcua_test.go:184:   UserIdentityToken 2:
    opcua_test.go:185:     PolicyID: 14
    opcua_test.go:186:     TokenType: UserTokenTypeUserName
    opcua_test.go:187:     IssuedTokenType:
    opcua_test.go:188:     IssuerEndpointURL:
    opcua_test.go:184:   UserIdentityToken 3:
    opcua_test.go:185:     PolicyID: 15
    opcua_test.go:186:     TokenType: UserTokenTypeCertificate
    opcua_test.go:187:     IssuedTokenType:
    opcua_test.go:188:     IssuerEndpointURL:
    opcua_test.go:154: Endpoint 6:
    opcua_test.go:155:   EndpointURL: opc.tcp://ba26b088a1b0:50000/
    opcua_test.go:156:   SecurityMode: MessageSecurityModeSign
    opcua_test.go:157:   SecurityPolicyURI: http://opcfoundation.org/UA/SecurityPolicy#Aes256_Sha256_RsaPss
    opcua_test.go:158:   TransportProfileURI: http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary
    opcua_test.go:159:   SecurityLevel: 10
    opcua_test.go:163:   Server ApplicationURI: urn:OpcPlc:ba26b088a1b0
    opcua_test.go:164:   Server ProductURI: https://github.com/azure-samples/iot-edge-opc-plc
    opcua_test.go:165:   Server ApplicationName: OpcPlc
    opcua_test.go:166:   Server ApplicationType: ApplicationTypeServer
    opcua_test.go:167:   Server GatewayServerURI:
    opcua_test.go:168:   Server DiscoveryProfileURI:
    opcua_test.go:169:   Server DiscoveryURLs: [opc.tcp://ba26b088a1b0:50000/]
    opcua_test.go:788:   Server certificate:
    opcua_test.go:805:     Not Before: 2023-11-13 00:00:00 +0000 UTC
    opcua_test.go:806:     Not After: 2024-11-13 00:00:00 +0000 UTC
    opcua_test.go:807:     DNS Names: [ba26b088a1b0]
    opcua_test.go:808:     IP Addresses: []
    opcua_test.go:809:     URIs: [urn:OpcPlc:ba26b088a1b0]
    opcua_test.go:184:   UserIdentityToken 1:
    opcua_test.go:185:     PolicyID: 16
    opcua_test.go:186:     TokenType: UserTokenTypeAnonymous
    opcua_test.go:187:     IssuedTokenType:
    opcua_test.go:188:     IssuerEndpointURL:
    opcua_test.go:184:   UserIdentityToken 2:
    opcua_test.go:185:     PolicyID: 17
    opcua_test.go:186:     TokenType: UserTokenTypeUserName
    opcua_test.go:187:     IssuedTokenType:
    opcua_test.go:188:     IssuerEndpointURL:
    opcua_test.go:184:   UserIdentityToken 3:
    opcua_test.go:185:     PolicyID: 18
    opcua_test.go:186:     TokenType: UserTokenTypeCertificate
    opcua_test.go:187:     IssuedTokenType:
    opcua_test.go:188:     IssuerEndpointURL:
    opcua_test.go:192: selected endpoint &{opc.tcp://ba26b088a1b0:50000/ 0xc0002ec000 [48 130 3 127 48 130 2 103 160 3 2 1 2 2 10 102 180 228 167 249 107 33 136 28 9 48 13 6 9 42 134 72 134 247 13 1 1 11 5 0 48 17 49 15 48 13 6 3 85 4 3 19 6 79 112 99 80 108 99 48 30 23 13 50 51 49 49 49 51 48 48 48 48 48 48 90 23 13 50 52 49 49 49 51 48 48 48 48 48 48 90 48 17 49 15 48 13 6 3 85 4 3 19 6 79 112 99 80 108 99 48 130 1 34 48 13 6 9 42 134 72 134 247 13 1 1 1 5 0 3 130 1 15 0 48 130 1 10 2 130 1 1 0 197 11 246 154 113 78 115 167 35 29 144 79 71 157 169 112 156 131 50 98 113 222 63 218 43 56 32 111 95 147 118 74 208 51 159 187 137 109 167 90 61 79 224 206 238 131 188 138 250 130 70 112 98 199 34 73 204 96 91 209 234 119 189 57 212 250 92 5 203 27 236 183 156 71 26 204 27 176 237 12 215 119 159 140 251 9 219 63 23 20 107 241 147 154 26 86 142 163 75 50 198 178 231 137 217 101 220 28 47 179 191 210 85 235 13 113 109 227 178 161 116 224 179 244 35 0 173 152 96 174 163 109 148 200 112 139 29 192 249 38 124 68 170 57 152 77 89 229 30 117 127 112 55 160 55 149 79 87 228 204 131 218 154 118 167 222 130 229 233 43 114 146 24 97 60 200 208 228 78 81 30 164 115 50 9 255 70 156 197 239 32 168 126 222 98 183 124 34 186 89 157 25 44 76 76 89 149 96 225 49 47 112 38 34 184 6 193 101 65 45 5 198 156 182 219 107 25 148 240 230 113 226 235 123 169 246 250 212 17 219 167 146 60 241 233 212 118 18 172 142 160 215 134 117 240 43 2 3 1 0 1 163 129 216 48 129 213 48 12 6 3 85 29 19 1 1 255 4 2 48 0 48 29 6 3 85 29 14 4 22 4 20 146 117 63 67 197 132 169 252 180 177 179 250 34 196 210 51 237 219 74 36 48 66 6 3 85 29 35 4 59 48 57 128 20 146 117 63 67 197 132 169 252 180 177 179 250 34 196 210 51 237 219 74 36 161 21 164 19 48 17 49 15 48 13 6 3 85 4 3 19 6 79 112 99 80 108 99 130 10 102 180 228 167 249 107 33 136 28 9 48 14 6 3 85 29 15 1 1 255 4 4 3 2 2 244 48 32 6 3 85 29 37 1 1 255 4 22 48 20 6 8 43 6 1 5 5 7 3 1 6 8 43 6 1 5 5 7 3 2 48 48 6 3 85 29 17 4 41 48 39 134 23 117 114 110 58 79 112 99 80 108 99 58 98 97 50 54 98 48 56 56 97 49 98 48 130 12 98 97 50 54 98 48 56 56 97 49 98 48 48 13 6 9 42 134 72 134 247 13 1 1 11 5 0 3 130 1 1 0 35 83 105 127 75 236 23 107 29 220 31 207 223 4 82 53 212 179 131 76 104 76 48 156 89 116 195 224 35 92 233 123 158 206 14 107 239 11 207 115 173 102 180 105 87 139 72 55 148 190 168 205 9 63 128 213 197 116 193 23 183 190 193 132 90 241 231 80 178 57 100 195 48 198 174 128 70 138 229 9 48 132 188 86 108 117 59 162 106 49 241 109 247 148 159 157 239 137 0 105 191 27 219 182 221 227 14 63 101 236 47 210 49 161 133 87 28 211 126 203 49 96 168 119 211 121 104 45 58 61 239 113 101 63 223 4 96 176 29 24 35 33 149 76 232 23 119 197 199 170 122 198 65 133 61 91 80 57 119 110 106 167 42 172 130 30 238 204 216 107 255 29 165 35 232 119 47 168 30 177 167 201 192 242 33 204 96 119 187 160 31 184 140 32 177 164 136 183 149 172 163 213 192 125 69 46 63 21 40 64 19 58 251 170 95 232 37 252 109 68 249 203 162 2 43 127 255 20 193 1 3 237 178 179 234 138 202 236 173 124 48 53 155 156 171 78 237 54 166 135 116 18 3 95 82 95] MessageSecurityModeSignAndEncrypt http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256 [0xc0001140a0 0xc0001140f0 0xc000114140] http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary 106}:
    opcua_test.go:209: Public Key: [45 45 45 45 45 66 69 71 73 78 32 67 69 82 84 73 70 73 67 65 84 69 45 45 45 45 45 10 77 73 73 68 85 122 67 67 65 106 117 103 65 119 73 66 65 103 73 82 65 77 116 98 119 98 109 118 108 67 90 109 78 98 78 48 66 75 80 78 68 78 107 119 68 81 89 74 75 111 90 73 104 118 99 78 65 81 69 76 66 81 65 119 10 72 84 69 98 77 66 107 71 65 49 85 69 67 104 77 83 82 50 57 119 89 51 86 104 73 70 82 108 99 51 81 103 81 50 120 112 90 87 53 48 77 66 52 88 68 84 73 122 77 84 69 120 78 84 69 119 77 68 85 121 77 70 111 88 10 68 84 77 122 77 84 69 120 77 106 69 119 77 68 85 121 77 70 111 119 72 84 69 98 77 66 107 71 65 49 85 69 67 104 77 83 82 50 57 119 89 51 86 104 73 70 82 108 99 51 81 103 81 50 120 112 90 87 53 48 77 73 73 66 10 73 106 65 78 66 103 107 113 104 107 105 71 57 119 48 66 65 81 69 70 65 65 79 67 65 81 56 65 77 73 73 66 67 103 75 67 65 81 69 65 119 82 98 73 83 51 43 121 83 101 110 85 99 77 49 81 103 99 54 51 104 109 67 48 10 53 80 103 72 51 113 117 112 53 113 114 97 70 83 105 48 85 72 108 99 90 98 83 79 81 43 115 84 48 86 100 86 81 75 80 119 107 87 85 77 89 67 85 54 79 105 66 82 57 98 90 112 66 109 85 72 52 51 115 114 65 89 102 115 10 89 104 52 83 84 109 80 118 43 67 47 73 110 87 85 55 78 75 90 69 111 119 69 120 65 104 53 81 47 50 74 52 65 72 56 47 68 107 86 72 48 106 79 65 110 118 119 107 116 79 117 110 121 118 49 82 85 82 75 71 52 43 116 103 10 83 70 73 103 57 99 70 55 67 86 119 67 72 77 106 113 113 82 52 53 113 78 87 77 55 43 100 122 109 54 121 111 104 121 51 54 47 83 67 43 72 121 72 69 99 53 47 105 104 122 47 57 105 53 74 53 55 104 47 100 86 67 119 56 10 117 51 49 88 104 72 103 53 76 85 85 50 54 47 89 57 87 119 70 54 70 53 47 49 82 76 68 99 113 119 74 80 72 109 114 48 115 52 50 117 67 43 79 109 75 112 57 81 89 87 65 85 73 53 54 76 54 52 88 74 70 66 110 81 10 74 103 83 47 118 73 109 101 54 81 107 108 100 103 107 57 43 122 56 108 112 122 43 49 69 69 115 47 67 73 48 56 102 85 55 49 115 50 51 69 54 83 105 55 106 66 104 121 77 104 82 121 70 77 82 51 54 113 67 82 50 81 73 68 10 65 81 65 66 111 52 71 78 77 73 71 75 77 65 52 71 65 49 85 100 68 119 69 66 47 119 81 69 65 119 73 67 57 68 65 100 66 103 78 86 72 83 85 69 70 106 65 85 66 103 103 114 66 103 69 70 66 81 99 68 65 81 89 73 10 75 119 89 66 66 81 85 72 65 119 73 119 68 65 89 68 86 82 48 84 65 81 72 47 66 65 73 119 65 68 66 76 66 103 78 86 72 82 69 69 82 68 66 67 103 104 57 49 99 109 52 54 89 109 86 117 100 71 104 118 99 121 49 49 10 98 87 103 54 89 50 120 112 90 87 53 48 76 88 81 119 82 69 108 54 99 49 73 120 104 104 57 49 99 109 52 54 89 109 86 117 100 71 104 118 99 121 49 49 98 87 103 54 89 50 120 112 90 87 53 48 76 88 81 119 82 69 108 54 10 99 49 73 120 77 65 48 71 67 83 113 71 83 73 98 51 68 81 69 66 67 119 85 65 65 52 73 66 65 81 66 65 104 48 115 90 56 52 49 55 68 97 103 71 54 85 69 89 70 82 119 86 48 56 119 121 48 53 117 51 105 43 50 116 10 108 78 105 82 68 104 121 107 65 109 48 57 81 89 97 43 50 107 53 85 101 57 82 97 73 100 116 102 121 81 86 77 50 119 55 88 74 74 78 66 90 108 119 86 118 82 43 109 66 85 57 100 87 121 43 56 49 57 57 113 52 84 103 79 10 121 74 73 88 99 51 116 121 51 116 103 107 102 81 53 77 53 107 47 82 104 98 65 119 100 56 97 106 115 121 72 86 79 78 47 85 54 82 117 110 84 103 90 122 120 110 67 120 66 108 53 89 98 114 77 88 77 74 121 114 107 81 112 118 10 111 102 71 51 108 68 79 119 55 98 53 47 56 56 104 116 47 122 71 74 120 56 68 86 43 97 53 81 113 108 65 106 100 108 118 86 84 97 109 67 104 118 81 76 114 52 43 107 53 103 114 87 112 90 72 118 57 66 71 88 51 112 107 89 10 69 108 90 87 79 72 47 108 119 89 49 98 83 72 69 108 65 100 104 101 55 75 48 117 97 108 74 109 116 105 82 48 70 47 79 111 104 103 85 65 121 106 108 71 107 115 76 66 53 110 118 97 105 82 87 87 90 108 69 97 99 98 89 68 10 104 68 78 120 101 51 70 77 98 108 108 100 112 56 49 56 73 103 106 90 109 73 113 87 66 118 81 66 113 81 73 97 106 103 118 98 98 117 80 69 109 117 47 52 99 90 116 122 102 102 85 103 10 45 45 45 45 45 69 78 68 32 67 69 82 84 73 70 73 67 65 84 69 45 45 45 45 45 10]




[10:05:20 INF] ChannelId 31: in Connecting state.
[10:05:20 INF] TCPSERVERCHANNEL SOCKET ATTACHED: 03CF0E88, ChannelId=31
[10:05:20 INF] ChannelId 31: in Opening state.
[10:05:20 INF] Security Policy: http://opcfoundation.org/UA/SecurityPolicy#None
[10:05:20 INF] Sender Certificate: (none)
[10:05:20 INF] ChannelId 31: Token #0 created. CreatedAt=10:05:20.304. Lifetime=3600000.
[10:05:20 INF] SECURE CHANNEL CREATED [.NET Standard ServerChannel UA-TCP 1.4.372.76] [ID=31] Connected To: opc.tcp://ba26b088a1b0:50000/
[10:05:20 INF] ChannelId 31: Token #1 activated. CreatedAt=10:05:20.304. Lifetime=3600000.
[10:05:20 INF] ChannelId 31: in Open state.
[10:05:20 INF] TCPSERVERCHANNEL ProcessCloseSecureChannelRequest success, ChannelId=31, TokenId=1, Socket=03CF0E88
[10:05:20 INF] ChannelId 31: in Closed state.
[10:05:20 INF] ChannelId 31: closed
[10:05:20 ERR] TCPSERVERCHANNEL ForceChannelFault Socket=FFFFFFFF, ChannelId=31, TokenId=1, Reason=BadConnectionClosed 'Remote side closed connection.'
[10:05:20 INF] ChannelId 31: in Faulted state.
[10:05:20 INF] ChannelId 32: in Connecting state.
[10:05:20 INF] TCPSERVERCHANNEL SOCKET ATTACHED: 01A0A231, ChannelId=32
[10:05:20 INF] ChannelId 32: in Opening state.
[10:05:20 WRN] Certificate Validation failed. Reason=BadCertificateUntrusted. [O=Gopcua Test Client] [05A224DBFEE20C53C71B25F7EAE918D45D681B71]
[10:05:20 WRN] Trusting certificate O=Gopcua Test Client because of corresponding command line option
[10:05:20 WRN] Validation errors suppressed:  [O=Gopcua Test Client] [05A224DBFEE20C53C71B25F7EAE918D45D681B71]
[10:05:20 INF] Security Policy: http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256
[10:05:20 INF] Sender Certificate: [O=Gopcua Test Client] [05A224DBFEE20C53C71B25F7EAE918D45D681B71]
[10:05:20 INF] ChannelId 32: Token #0 created. CreatedAt=10:05:20.453. Lifetime=3600000.
[10:05:20 INF] SECURE CHANNEL CREATED [.NET Standard ServerChannel UA-TCP 1.4.372.76] [ID=32] Connected To: opc.tcp://ba26b088a1b0:50000/ [SignAndEncrypt/Basic256Sha256/Binary]
[10:05:20 INF] Client Certificate:  [O=Gopcua Test Client] [05A224DBFEE20C53C71B25F7EAE918D45D681B71]
[10:05:20 INF] Server Certificate:  [CN=OpcPlc] [43428A86C4E034B4C7955F2D46EBE69778F5A23A]
[10:05:20 INF] ChannelId 32: Token #1 activated. CreatedAt=10:05:20.453. Lifetime=3600000.
[10:05:20 INF] ChannelId 32: in Open state.
[10:05:20 WRN] Certificate Validation failed. Reason=BadCertificateUntrusted. [O=Gopcua Test Client] [05A224DBFEE20C53C71B25F7EAE918D45D681B71]
[10:05:20 WRN] Trusting certificate O=Gopcua Test Client because of corresponding command line option
[10:05:20 WRN] Validation errors suppressed:  [O=Gopcua Test Client] [05A224DBFEE20C53C71B25F7EAE918D45D681B71]
[10:05:20 INF] Session CREATED, Id=ns=7;i=196415766, Name=gopcua-1700042720456603376, ChannelId=7fe53482-d7d0-420c-9518-25dd46ed51fc-32, User=Anonymous
[10:05:20 INF] Server - SESSION CREATED. SessionId=ns=7;i=196415766
[10:05:20 INF] Session VALIDATED, Id=ns=7;i=196415766, Name=gopcua-1700042720456603376, ChannelId=7fe53482-d7d0-420c-9518-25dd46ed51fc-32, User=Anonymous
[10:05:20 INF] Session FIRST ACTIVATION, Id=ns=7;i=196415766, Name=gopcua-1700042720456603376, ChannelId=7fe53482-d7d0-420c-9518-25dd46ed51fc-32, User=Anonymous
[10:05:20 INF] Server - SESSION ACTIVATED.
[10:05:20 INF] ChannelId 33: in Connecting state.
[10:05:20 INF] TCPSERVERCHANNEL SOCKET ATTACHED: 00D03EA8, ChannelId=33
[10:05:20 INF] ChannelId 33: in Opening state.
[10:05:20 INF] Security Policy: http://opcfoundation.org/UA/SecurityPolicy#None
[10:05:20 INF] Sender Certificate: (none)
[10:05:20 INF] ChannelId 33: Token #0 created. CreatedAt=10:05:20.465. Lifetime=3600000.
[10:05:20 INF] SECURE CHANNEL CREATED [.NET Standard ServerChannel UA-TCP 1.4.372.76] [ID=33] Connected To: opc.tcp://ba26b088a1b0:50000/
[10:05:20 INF] ChannelId 33: Token #1 activated. CreatedAt=10:05:20.465. Lifetime=3600000.
[10:05:20 INF] ChannelId 33: in Open state.
[10:05:20 INF] TCPSERVERCHANNEL ProcessCloseSecureChannelRequest success, ChannelId=33, TokenId=1, Socket=00D03EA8
[10:05:20 INF] ChannelId 33: in Closed state.
[10:05:20 INF] ChannelId 33: closed
[10:05:20 ERR] TCPSERVERCHANNEL ForceChannelFault Socket=FFFFFFFF, ChannelId=33, TokenId=1, Reason=BadConnectionClosed 'Remote side closed connection.'
[10:05:20 INF] ChannelId 33: in Faulted state.
[10:05:20 INF] ChannelId 34: in Connecting state.
[10:05:20 INF] TCPSERVERCHANNEL SOCKET ATTACHED: 00ED19AF, ChannelId=34
[10:05:20 INF] ChannelId 34: in Opening state.
[10:05:20 WRN] Certificate Validation failed. Reason=BadCertificateUntrusted. [O=Gopcua Test Client] [D203923396D7F4FC073D537860395EBB71FFABB1]
[10:05:20 WRN] Trusting certificate O=Gopcua Test Client because of corresponding command line option
[10:05:20 WRN] Validation errors suppressed:  [O=Gopcua Test Client] [D203923396D7F4FC073D537860395EBB71FFABB1]
[10:05:20 ERR] Could not validate signature.
[10:05:20 ERR] Certificate:  [O=Gopcua Test Client] [D203923396D7F4FC073D537860395EBB71FFABB1]
[10:05:20 ERR] MessageType =OPNF, Length =1729, ActualSignature=77D219DB0E1F6A3AA7B1618B2E454A25F1A31A3D4055F59873D73F3EA12C725773548170C503FB2BD530A4F69228D11C3B39E42913869FB6F48A153B139435A438D133FDE2DEC0B78ED29EA89BCC6EA73FD92EA4C7E4D5725207395C5B62BD402DF0AB309CC0A22758902E124D3620D9BE72721CD921469122A59733FF23E1752F318F018B00E279EFFCCAA96C63684AF407E70EE89FAF188F226578A10D47541AFD6799CF9CEDC6E17A0F6CE4AECC221FB8AB027C55F3173052DE7F50439BCE933E78592D2B8DBE8AA32977B9E5E712CEF409B099B51255C14ED3BCC6C4720C132C938BE10AF8236441F7250100000000000000000000000000000000000000
[10:05:20 WRN] Could not verify signature on message.
[10:05:20 ERR] TCPSERVERCHANNEL ForceChannelFault Socket=00ED19AF, ChannelId=0, TokenId=0, Reason=BadSecurityChecksFailed 'Could not verify security on OpenSecureChannel request.'
[10:05:20 INF] ChannelId 34: in Faulted state.
[10:05:20 ERR] TCPSERVERCHANNEL ForceChannelFault Socket=00ED19AF, ChannelId=0, TokenId=0, Reason=BadConnectionClosed 'Remote side closed connection'
[10:05:20 INF] ChannelId 35: in Connecting state.
[10:05:20 INF] TCPSERVERCHANNEL SOCKET ATTACHED: 03EE5BAE, ChannelId=35
[10:05:20 INF] ChannelId 35: in Opening state.
[10:05:20 INF] Security Policy: http://opcfoundation.org/UA/SecurityPolicy#None
[10:05:20 INF] Sender Certificate: (none)
[10:05:20 INF] ChannelId 35: Token #0 created. CreatedAt=10:05:20.632. Lifetime=3600000.
[10:05:20 INF] SECURE CHANNEL CREATED [.NET Standard ServerChannel UA-TCP 1.4.372.76] [ID=35] Connected To: opc.tcp://ba26b088a1b0:50000/
[10:05:20 INF] ChannelId 35: Token #1 activated. CreatedAt=10:05:20.632. Lifetime=3600000.
[10:05:20 INF] ChannelId 35: in Open state.
[10:05:20 INF] TCPSERVERCHANNEL ProcessCloseSecureChannelRequest success, ChannelId=35, TokenId=1, Socket=03EE5BAE
[10:05:20 INF] ChannelId 35: in Closed state.
[10:05:20 INF] ChannelId 35: closed
[10:05:20 ERR] TCPSERVERCHANNEL ForceChannelFault Socket=FFFFFFFF, ChannelId=35, TokenId=1, Reason=BadConnectionClosed 'Remote side closed connection.'
[10:05:20 INF] ChannelId 35: in Faulted state.
[10:05:21 INF] ChannelId 36: in Connecting state.
[10:05:21 INF] TCPSERVERCHANNEL SOCKET ATTACHED: 01734046, ChannelId=36
[10:05:21 INF] ChannelId 36: in Opening state.
[10:05:21 WRN] Certificate Validation failed. Reason=BadCertificateUntrusted. [O=Gopcua Test Client] [DFB6AF905FA6CB0872D33FA9E34E9EEFB918A3B6]
[10:05:21 WRN] Trusting certificate O=Gopcua Test Client because of corresponding command line option
[10:05:21 WRN] Validation errors suppressed:  [O=Gopcua Test Client] [DFB6AF905FA6CB0872D33FA9E34E9EEFB918A3B6]
[10:05:21 ERR] Could not validate signature.
[10:05:21 ERR] Certificate:  [O=Gopcua Test Client] [DFB6AF905FA6CB0872D33FA9E34E9EEFB918A3B6]
[10:05:21 ERR] MessageType =OPNF, Length =1729, ActualSignature=EC84BE07B149FBDF0F50378D01EE33361E3CE7616AC1328263B415A473459C76C33669446732BBD6534686E82362FC1E328D92564117790E510D380D3D0E809E000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
[10:05:21 WRN] Could not verify signature on message.
[10:05:21 ERR] TCPSERVERCHANNEL ForceChannelFault Socket=01734046, ChannelId=0, TokenId=0, Reason=BadSecurityChecksFailed 'Could not verify security on OpenSecureChannel request.'
[10:05:21 INF] ChannelId 36: in Faulted state.
[10:05:21 ERR] TCPSERVERCHANNEL ForceChannelFault Socket=01734046, ChannelId=0, TokenId=0, Reason=BadConnectionClosed 'Remote side closed connection'
[10:05:21 ERR] TCPSERVERCHANNEL ForceChannelFault Socket=01A0A231, ChannelId=32, TokenId=1, Reason=BadConnectionClosed 'Remote side closed connection'
[10:05:21 INF] ChannelId 32: in Faulted state.


*/

func TestAgainstSimulator(t *testing.T) {

	t.Run("Logging Endpoints", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var endpoints []*ua.EndpointDescription
		var err error

		input := &OPCUAInput{
			endpoint: "opc.tcp://localhost:50000",
			username: "",
			password: "",
			nodeIDs:  nil,
			insecure: false,
		}

		endpoints, err = opcua.GetEndpoints(ctx, input.endpoint)
		assert.NoError(t, err)

		for i, endpoint := range endpoints {
			t.Logf("Endpoint %d:", i+1)
			t.Logf("  EndpointURL: %s", endpoint.EndpointURL)
			t.Logf("  SecurityMode: %v", endpoint.SecurityMode)
			t.Logf("  SecurityPolicyURI: %s", endpoint.SecurityPolicyURI)
			t.Logf("  TransportProfileURI: %s", endpoint.TransportProfileURI)
			t.Logf("  SecurityLevel: %d", endpoint.SecurityLevel)

			// If Server is not nil, log its details
			if endpoint.Server != nil {
				t.Logf("  Server ApplicationURI: %s", endpoint.Server.ApplicationURI)
				t.Logf("  Server ProductURI: %s", endpoint.Server.ProductURI)
				t.Logf("  Server ApplicationName: %s", endpoint.Server.ApplicationName.Text)
				t.Logf("  Server ApplicationType: %v", endpoint.Server.ApplicationType)
				t.Logf("  Server GatewayServerURI: %s", endpoint.Server.GatewayServerURI)
				t.Logf("  Server DiscoveryProfileURI: %s", endpoint.Server.DiscoveryProfileURI)
				t.Logf("  Server DiscoveryURLs: %v", endpoint.Server.DiscoveryURLs)
			}

			// Output the certificate
			if len(endpoint.ServerCertificate) > 0 {
				// Convert to PEM format first, then log the certificate information
				pemCert := pem.EncodeToMemory(&pem.Block{
					Type:  "CERTIFICATE",
					Bytes: endpoint.ServerCertificate,
				})
				logCertificateInfo(t, pemCert)
			}

			// Loop through UserIdentityTokens
			for j, token := range endpoint.UserIdentityTokens {
				t.Logf("  UserIdentityToken %d:", j+1)
				t.Logf("    PolicyID: %s", token.PolicyID)
				t.Logf("    TokenType: %v", token.TokenType)
				t.Logf("    IssuedTokenType: %s", token.IssuedTokenType)
				t.Logf("    IssuerEndpointURL: %s", token.IssuerEndpointURL)
			}
		}
		selectedEndpoint := input.getReasonableEndpoint(endpoints, ua.UserTokenTypeFromString("Anonymous"), input.insecure, "SignAndEncrypt", "Basic256Sha256")
		t.Logf("selected endpoint %v:", selectedEndpoint)

		var c *opcua.Client

		// Step 4: Initialize OPC UA client options
		opts := make([]opcua.Option, 0)
		opts = append(opts, opcua.SecurityFromEndpoint(selectedEndpoint, ua.UserTokenTypeFromString("Anonymous")))

		// Step 5: Generate Certificates, because this is really a step that can not happen in the background...
		if !input.insecure {
			// Generate a new certificate in memory, no file read/write operations.
			randomStr := randomString(8) // Generates an 8-character random string
			clientName := "urn:benthos-umh:client-" + randomStr
			certPEM, keyPEM, err := GenerateCert(clientName, 2048, 24*time.Hour*365*10)
			if err != nil {
				t.Logf("Failed to generate certificate: %v", err)
			}
			t.Logf("Public Key: %v", certPEM)

			// Convert PEM to X509 Certificate and RSA PrivateKey for in-memory use.
			cert, err := tls.X509KeyPair(certPEM, keyPEM)
			if err != nil {
				t.Logf("Failed to parse certificate: %v", err)
			}

			pk, ok := cert.PrivateKey.(*rsa.PrivateKey)
			if !ok {
				t.Logf("Invalid private key type")
			}

			// Append the certificate and private key to the client options
			opts = append(opts, opcua.PrivateKey(pk), opcua.Certificate(cert.Certificate[0]))
		}

		// Step 6: Create and connect the OPC UA client
		// Note that we are not taking `selectedEndpoint.EndpointURL` here as the server can be misconfigured. We are taking instead the user input.
		c, err = opcua.NewClient(input.endpoint, opts...)
		if err != nil {
			t.Logf("Failed to create a new client")
		}

		// Connect to the selected endpoint
		if err := c.Connect(ctx); err != nil {
			t.Logf("Failed to connect %s", err)
		}
	})

	t.Run("ConnectAnonymous", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		input := &OPCUAInput{
			endpoint: "opc.tcp://localhost:50000",
			username: "",
			password: "",
			nodeIDs:  nil,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Username-Password fail", func(t *testing.T) {
		t.Skip() // Needs to be skipped, the current OPC-UA simulator does only logging in once, after that it fails
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		input := &OPCUAInput{
			endpoint: "opc.tcp://localhost:50000",
			username: "sysadmin_bad", // bad user and password
			password: "demo",
			nodeIDs:  nil,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.Error(t, err)

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Username-Password success", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		input := &OPCUAInput{
			endpoint: "opc.tcp://localhost:50000",
			username: "sysadmin",
			password: "demo",
			nodeIDs:  nil,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

}

func TestAgainstRemoteInstance(t *testing.T) {

	// These information can be found in Bitwarden under WAGO PLC
	endpoint := os.Getenv("TEST_WAGO_ENDPOINT_URI")
	username := os.Getenv("TEST_WAGO_USERNAME")
	password := os.Getenv("TEST_WAGO_PASSWORD")

	// Check if environment variables are set
	if endpoint == "" || username == "" || password == "" {
		t.Skip("Skipping test: environment variables not set")
		return
	}

	t.Run("ConnectAnonymous", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		input := &OPCUAInput{
			endpoint: endpoint,
			username: "",
			password: "",
			nodeIDs:  nil,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("ConnectAnonymousWithNoEncryption", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		input := &OPCUAInput{
			endpoint: endpoint,
			username: "",
			password: "",
			nodeIDs:  nil,
			insecure: true,
		}

		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Username-Password fail", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		input := &OPCUAInput{
			endpoint: endpoint,
			username: "123", // bad user and password
			password: "123",
			nodeIDs:  nil,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.Error(t, err)

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Username-Password success", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		input := &OPCUAInput{
			endpoint: endpoint,
			username: username,
			password: password,
			nodeIDs:  nil,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Parse nodes", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint: endpoint,
			username: username,
			password: password,
			nodeIDs:  parsedNodeIDs,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("ReadBatch", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint: endpoint,
			username: username,
			password: password,
			nodeIDs:  parsedNodeIDs,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 1, len(messageBatch))

		for _, message := range messageBatch {
			message, err := message.AsStructuredMut()
			if err != nil {
				t.Fatal(err)
			}
			var exampleNumber json.Number = "22.565684"
			assert.IsType(t, exampleNumber, message) // it should be a number
			t.Log("Received message: ", message)
		}

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Subscribe", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL", "ns=4;s=|vprop|WAGO 750-8101 PFC100 CS 2ETH.Application.RevisionCounter"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint:         endpoint,
			username:         username,
			password:         password,
			nodeIDs:          parsedNodeIDs,
			subscribeEnabled: true,
		}

		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		t.Log("Connected!")

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// expect 2 messages for both nodes
		assert.Equal(t, 2, len(messageBatch))

		for _, message := range messageBatch {
			message, err := message.AsStructuredMut()
			if err != nil {
				t.Fatal(err)
			}
			var exampleNumber json.Number = "22.565684"
			assert.IsType(t, exampleNumber, message) // it should be a number
			t.Log("Received message: ", message)
		}

		messageBatch2, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// expect 1 message only as RevisionCounter will not change
		assert.Equal(t, 1, len(messageBatch2))

		for _, message := range messageBatch2 {
			message, err := message.AsStructuredMut()
			if err != nil {
				t.Fatal(err)
			}
			var exampleNumber json.Number = "22.565684"
			assert.IsType(t, exampleNumber, message) // it should be a number
			t.Log("Received message: ", message)
		}

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("ReadBatch_Insecure", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint: endpoint,
			username: username,
			password: password,
			nodeIDs:  parsedNodeIDs,
			insecure: true,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 1, len(messageBatch))

		for _, message := range messageBatch {
			message, err := message.AsStructuredMut()
			if err != nil {
				t.Fatal(err)
			}
			var exampleNumber json.Number = "22.565684"
			assert.IsType(t, exampleNumber, message) // it should be a number
			t.Log("Received message: ", message)
		}

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("ReadBatch_SecurityMode_SecurityPolicy", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint:       endpoint,
			username:       username,
			password:       password,
			nodeIDs:        parsedNodeIDs,
			insecure:       false,
			securityMode:   "SignAndEncrypt",
			securityPolicy: "Basic128Rsa15",
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 1, len(messageBatch))

		for _, message := range messageBatch {
			message, err := message.AsStructuredMut()
			if err != nil {
				t.Fatal(err)
			}
			var exampleNumber json.Number = "22.565684"
			assert.IsType(t, exampleNumber, message) // it should be a number
			t.Log("Received message: ", message)
		}

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

}

func MockGetEndpoints() []*ua.EndpointDescription {
	// Define the mock endpoints with the desired properties
	endpoint1 := &ua.EndpointDescription{
		EndpointURL: "opc.tcp://example.com:4840", // Replace with your actual server URL
		Server: &ua.ApplicationDescription{
			ApplicationURI:  "urn:example:server", // Replace with your server's URI
			ApplicationType: ua.ApplicationTypeServer,
		},
		ServerCertificate: []byte{},                                                    // Replace with your server certificate
		SecurityMode:      ua.MessageSecurityModeFromString("SignAndEncrypt"),          // Use appropriate security mode
		SecurityPolicyURI: "http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256", // Use appropriate security policy URI
		UserIdentityTokens: []*ua.UserTokenPolicy{
			{
				PolicyID:          "anonymous",
				TokenType:         ua.UserTokenTypeAnonymous,
				IssuedTokenType:   "http://opcfoundation.org/UA/UserTokenPolicy#Anonymous",
				SecurityPolicyURI: "http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256",
			},
			{
				PolicyID:          "username",
				TokenType:         ua.UserTokenTypeUserName,
				IssuedTokenType:   "http://opcfoundation.org/UA/UserTokenPolicy#UserName",
				SecurityPolicyURI: "http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256",
			},
		},
		TransportProfileURI: "http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary",
		SecurityLevel:       3, // Use an appropriate security level
	}

	endpoint2 := &ua.EndpointDescription{
		EndpointURL: "opc.tcp://example2.com:4840", // Replace with your actual server URL
		Server: &ua.ApplicationDescription{
			ApplicationURI:  "urn:example2:server", // Replace with your server's URI
			ApplicationType: ua.ApplicationTypeServer,
		},
		ServerCertificate: []byte("mock_certificate_2"),                      // Replace with your server certificate
		SecurityMode:      ua.MessageSecurityModeFromString("None"),          // Use appropriate security mode
		SecurityPolicyURI: "http://opcfoundation.org/UA/SecurityPolicy#None", // Use appropriate security policy URI
		UserIdentityTokens: []*ua.UserTokenPolicy{
			{
				PolicyID:          "anonymous",
				TokenType:         ua.UserTokenTypeAnonymous,
				IssuedTokenType:   "http://opcfoundation.org/UA/UserTokenPolicy#Anonymous",
				SecurityPolicyURI: "http://opcfoundation.org/UA/SecurityPolicy#None",
			},
			{
				PolicyID:          "username",
				TokenType:         ua.UserTokenTypeUserName,
				IssuedTokenType:   "http://opcfoundation.org/UA/UserTokenPolicy#UserName",
				SecurityPolicyURI: "http://opcfoundation.org/UA/SecurityPolicy#None",
			},
		},
		TransportProfileURI: "http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary",
		SecurityLevel:       0, // Use an appropriate security level
	}

	endpoint3 := &ua.EndpointDescription{
		EndpointURL: "opc.tcp://example3.com:4840", // Replace with your actual server URL
		Server: &ua.ApplicationDescription{
			ApplicationURI:  "urn:example3:server", // Replace with your server's URI
			ApplicationType: ua.ApplicationTypeServer,
		},
		ServerCertificate: []byte("mock_certificate_2"),                                    // Replace with your server certificate
		SecurityMode:      ua.MessageSecurityModeFromString("SignAndEncrypt"),              // Use appropriate security mode
		SecurityPolicyURI: "http://opcfoundation.org/UA/SecurityPolicy#Aes256Sha256RsaPss", // Use appropriate security policy URI
		UserIdentityTokens: []*ua.UserTokenPolicy{
			{
				PolicyID:          "anonymous",
				TokenType:         ua.UserTokenTypeAnonymous,
				IssuedTokenType:   "http://opcfoundation.org/UA/UserTokenPolicy#Anonymous",
				SecurityPolicyURI: "http://opcfoundation.org/UA/SecurityPolicy#Aes256Sha256RsaPss",
			},
			{
				PolicyID:          "username",
				TokenType:         ua.UserTokenTypeUserName,
				IssuedTokenType:   "http://opcfoundation.org/UA/UserTokenPolicy#UserName",
				SecurityPolicyURI: "http://opcfoundation.org/UA/SecurityPolicy#Aes256Sha256RsaPss",
			},
		},
		TransportProfileURI: "http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary",
		SecurityLevel:       0, // Use an appropriate security level
	}

	// Return the mock endpoints as a slice
	return []*ua.EndpointDescription{endpoint1, endpoint2, endpoint3}
}

func TestGetReasonableEndpoint_Insecure(t *testing.T) {
	input := &OPCUAInput{
		endpoint: "",
		username: "",
		password: "",
		nodeIDs:  nil,
		insecure: true,
	}

	endpoints := MockGetEndpoints()
	selectedEndpoint := input.getReasonableEndpoint(endpoints, ua.UserTokenTypeFromString("Anonymous"), input.insecure, "", "")

	if selectedEndpoint != nil {
		if selectedEndpoint.SecurityMode != ua.MessageSecurityModeFromString("None") {
			t.Errorf("Expected selected endpoint to have no encryption, but got %v", selectedEndpoint.SecurityMode)
		}
	} else {
		t.Error("Expected a reasonable endpoint, but got nil")
	}

	input2 := &OPCUAInput{
		endpoint: "",
		username: "",
		password: "",
		nodeIDs:  nil,
		insecure: false,
	}

	selectedEndpoint2 := input.getReasonableEndpoint(endpoints, ua.UserTokenTypeFromString("Anonymous"), input2.insecure, "", "")

	if selectedEndpoint2 != nil {
		if selectedEndpoint2.SecurityMode != ua.MessageSecurityModeFromString("SignAndEncrypt") {
			t.Errorf("Expected selected endpoint to have encryption, but got %v", selectedEndpoint.SecurityMode)
		}
	} else {
		t.Error("Expected a reasonable endpoint, but got nil")
	}
}

func TestGetReasonableEndpoint_SecurityModeAndPolicy(t *testing.T) {
	input := &OPCUAInput{
		endpoint:       "",
		username:       "123",
		password:       "213",
		nodeIDs:        nil,
		insecure:       false,
		securityMode:   "SignAndEncrypt",
		securityPolicy: "Aes256Sha256RsaPss",
	}

	endpoints := MockGetEndpoints()
	selectedEndpoint := input.getReasonableEndpoint(endpoints, ua.UserTokenTypeFromString("UserName"), input.insecure, input.securityMode, input.securityPolicy)

	if selectedEndpoint != nil {
		if selectedEndpoint.SecurityMode != ua.MessageSecurityModeFromString(input.securityMode) && selectedEndpoint.SecurityPolicyURI != "http://opcfoundation.org/UA/SecurityPolicy#"+input.securityPolicy {
			t.Errorf("Expected selected endpoint to have encryption with security mode %v and policy %v, but got %v and %v", input.securityMode, input.securityPolicy, selectedEndpoint.SecurityMode, selectedEndpoint.SecurityPolicyURI)
		}
	} else {
		t.Error("Expected a reasonable endpoint, but got nil")
	}
}

func logCertificateInfo(t *testing.T, certBytes []byte) {
	t.Logf("  Server certificate:")

	// Decode the certificate from base64 to DER format
	block, _ := pem.Decode(certBytes)
	if block == nil {
		t.Log("Failed to decode certificate")
		return
	}

	// Parse the DER-format certificate
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Log("Failed to parse certificate:", err)
		return
	}

	// Log the details
	t.Log("    Not Before:", cert.NotBefore)
	t.Log("    Not After:", cert.NotAfter)
	t.Log("    DNS Names:", cert.DNSNames)
	t.Log("    IP Addresses:", cert.IPAddresses)
	t.Log("    URIs:", cert.URIs)
}
