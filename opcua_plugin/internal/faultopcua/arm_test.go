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

package faultopcua

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"
)

// TestArm_PublishesTheFaultComplete pins the order arm has to keep: a fault is
// complete before it is in the namespace's map, because the map is where the
// server's request handlers find it, and they can find it the moment it is
// inserted.
//
// A fault published before its parameters are set is met, by a write that
// arrives in between, with its status still at the zero value: RefuseWrite
// answers Good -- the lie refuseASuccessStatus exists to prevent, reached
// around the guard -- and records a firing. Setting the parameters after
// publishing is also a data race on every request the fault then answers.
//
// The hammer is one goroutine calling the write path the way the server's
// single service loop does, without pause. The fault is armed and left armed
// while it works, so under -race the very first write it meets reports the
// race on its own, without depending on a lucky schedule. The accounting
// below catches the same defect without -race when a write lands in the
// window: every call either meets a fault -- and answers with its refusal --
// or meets an honest machine, so "answered Good" plus "fired a fault" must
// add up to the number of calls, and a call that did both is a write that was
// refused with success.
func TestArm_PublishesTheFaultComplete(t *testing.T) {
	srv := New(t)
	v := srv.AddVariable("Setpoint", int32(7))
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("faultopcua: start server: %v", err)
	}

	val, err := ua.NewVariant(int32(4200))
	if err != nil {
		t.Fatalf("faultopcua: new variant: %v", err)
	}
	dv := &ua.DataValue{EncodingMask: ua.DataValueValue, Value: val}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var calls, oks int
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if st := v.ns.SetAttribute(v.id, ua.AttributeIDValue, dv); st == ua.StatusOK {
				oks++
			}
			calls++
		}
	}()

	// The honest machine first, so the hammer is provably reaching the node
	// before anything is armed against it.
	time.Sleep(50 * time.Millisecond)
	f := srv.RefuseWrite(v, ua.StatusBadInternalError)

	// The fault stays armed with nothing on the arming side touching the
	// namespace again: every write the hammer now meets reads a field that
	// was set after the fault was published, with no synchronization since.
	time.Sleep(100 * time.Millisecond)
	f.Disarm()
	close(stop)
	wg.Wait()

	if calls == 0 {
		t.Fatal("the hammer never reached the node, so this test proved nothing")
	}
	fired := f.Count()
	if fired == 0 {
		t.Error("the fault never fired, so this test proved nothing")
	}
	if got := oks + fired; got != calls {
		t.Errorf("%d writes reached the node, but %d answered Good and %d fired a fault, %d too many: "+
			"a write met RefuseWrite after it was armed but before its status was set, and was refused with success",
			calls, oks, fired, got-calls)
	}
}
