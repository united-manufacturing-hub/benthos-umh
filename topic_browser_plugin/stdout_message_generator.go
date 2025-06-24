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

package topic_browser_plugin

/*
	The functions in this file are used to create an stdout-ready message from our protobuf bytes.
*/

import (
	"encoding/hex"
	"fmt"
	"time"
)

// Wire format delimiters for umh-core consumption
const (
	startBlock = "STARTSTARTSTART\n"
	midBlock   = "ENDDATAENDDATAENDDATA\n"
	finalBlock = "ENDENDENDEND"
)

// bytesToMessageWithStartEndBlocksAndTimestamp wraps the byte string in start & end blocks and includes a timestamp
// This allows easy parsing on the umh-core side.
func bytesToMessageWithStartEndBlocksAndTimestamp(protobytes []byte) []byte {
	hexBytes := hex.EncodeToString(protobytes)
	unixTimestampMs := time.Now().UnixMilli()
	return []byte(startBlock + hexBytes + "\n" + midBlock + fmt.Sprintf("%d", unixTimestampMs) + "\n" + finalBlock)
}
