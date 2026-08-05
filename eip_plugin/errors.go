// Copyright 2025 UMH Systems GmbH
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

package eip_plugin

import (
	"errors"
	"net"
	"os"
	"syscall"
)

// gologix-library wraps around these errors. io.EOF and io.ErrUnexpectedEOF are
// deliberately absent: the same values come back from parsing a reply buffer.
var transportErrors = []error{
	os.ErrDeadlineExceeded,
	net.ErrClosed,
	syscall.ECONNRESET,
	syscall.ECONNREFUSED,
	syscall.ECONNABORTED,
	syscall.EPIPE,
	syscall.ETIMEDOUT,
	syscall.EHOSTUNREACH,
	syscall.ENETUNREACH,
}

func isTransportError(err error) bool {
	if err == nil {
		return false
	}

	for _, transErr := range transportErrors {
		if errors.Is(err, transErr) {
			return true
		}
	}

	var netErr net.Error
	return errors.As(err, &netErr)
}
