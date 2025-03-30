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

import "github.com/danomagnum/gologix"

// NOTE: Abstraction for the CIP calls so we can potentially mock them
// possibly add some more calls here later on
type CIPReader interface {
	Connect() error
	Disconnect() error
	Read(tag string, data any) error
	GetAttrSingle(cls gologix.CIPClass, inst gologix.CIPInstance, attr gologix.CIPAttribute) (*gologix.CIPItem, error)
}
