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

package modbus_plugin

func determineConverterBit(byteOrder string, bit uint8) (converterFunc, error) {
	tohost, err := endiannessConverter16(byteOrder)
	if err != nil {
		return nil, err
	}

	return func(b []byte) interface{} {
		// Swap the bytes according to endianness
		v := tohost(b)
		return uint8(v >> bit & 0x01)
	}, nil
}
