// Copyright 2026 UMH Systems GmbH
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

package beckhoff_ads_plugin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	adsLib "github.com/RuneRoven/go-ads/v2"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// goADSClient is the go-ads adapter — the ONLY file (besides client.go)
// importing the fork. All WithRoute/NewAMSAddress/type-mapping logic lives here.
type goADSClient struct {
	session *adsLib.Session
}

// resolveRouteHostIP returns the local IP for the ADS route: the configured
// hostIP, else the source IP of a TCP dial to the ADS port (same interface).
func resolveRouteHostIP(ctx context.Context, cfg SessionConfig) (string, error) {
	if cfg.HostIP != "" {
		return cfg.HostIP, nil
	}
	dialer := net.Dialer{Timeout: routeDialTimeout}
	tcpConn, dialErr := dialer.DialContext(ctx, "tcp4", net.JoinHostPort(cfg.TargetIP, strconv.Itoa(cfg.TargetPort)))
	if dialErr != nil {
		// PLC unreachable; fall back to a UDP routing lookup (no packet sent).
		udpConn, udpErr := net.Dial("udp4", net.JoinHostPort(cfg.TargetIP, adsDiscoveryPort))
		if udpErr != nil {
			return "", fmt.Errorf("could not auto-detect a local IP for the route (set hostIP explicitly): %w", dialErr)
		}
		defer udpConn.Close()
		return udpConn.LocalAddr().(*net.UDPAddr).IP.String(), nil
	}
	defer tcpConn.Close()
	return tcpConn.LocalAddr().(*net.TCPAddr).IP.String(), nil
}

// buildSessionOptions assembles the go-ads SessionOptions: logger bridge,
// route registration, local AMS override, and request timeout.
func buildSessionOptions(ctx context.Context, cfg SessionConfig, log *service.Logger) ([]adsLib.SessionOption, error) {
	// go-ads verbosity follows the benthos pipeline log level via this bridge;
	// no global SetDefaultLogger (last-input-wins across multiple ADS inputs).
	opts := []adsLib.SessionOption{adsLib.WithLogger(slog.New(&benthosLogHandler{logger: log}))}

	if cfg.Username != "" && cfg.Password != "" {
		hostAddr, err := resolveRouteHostIP(ctx, cfg)
		if err != nil {
			return nil, err
		}
		routeName := fmt.Sprintf("benthosADS-%s", hostAddr)
		log.With("routeName", routeName, "clientIP", hostAddr, "targetIP", cfg.TargetIP).
			Info("Registering route on PLC")
		opts = append(opts, adsLib.WithRoute(routeName, cfg.Username, cfg.Password), adsLib.WithHostIP(hostAddr))
	}

	// "auto" (default) lets go-ads derive local AMS from the TCP connection.
	if cfg.HostAMS != "" && cfg.HostAMS != "auto" {
		localAMS, err := adsLib.NewAMSAddress(cfg.HostAMS, uint16(cfg.HostPort))
		if err != nil {
			return nil, fmt.Errorf("hostAMS %q is not a valid AMS NetID: %w", cfg.HostAMS, err)
		}
		opts = append(opts, adsLib.WithLocalAMS(localAMS))
	}
	if cfg.RequestTimeout > 0 {
		opts = append(opts, adsLib.WithRequestTimeout(cfg.RequestTimeout))
	}
	return opts, nil
}

// newGoADSClient builds session options and the go-ads session from
// SessionConfig. It does not call Connect; the caller drives that.
func newGoADSClient(ctx context.Context, cfg SessionConfig, log *service.Logger) (Client, error) {
	opts, err := buildSessionOptions(ctx, cfg, log)
	if err != nil {
		return nil, err
	}
	// A zero NetID tells go-ads to ask the PLC for its own; a configured one is
	// verified against the device and only warned about on a mismatch.
	targetAMS := adsLib.AMSAddress{Port: uint16(cfg.RuntimePort)}
	if cfg.TargetAMS != "" {
		targetAMS, err = adsLib.NewAMSAddress(cfg.TargetAMS, uint16(cfg.RuntimePort))
		if err != nil {
			return nil, fmt.Errorf("targetAMS %q is not a valid AMS NetID: %w", cfg.TargetAMS, err)
		}
	}
	// Background ctx: session lifetime is driven by Close, not the per-call
	// construction ctx (which would tear the session down on return).
	sess, err := adsLib.NewSession(context.Background(), adsLib.AMSEndpoint{
		IP: cfg.TargetIP, Port: cfg.TargetPort, AMS: targetAMS,
	}, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating the ADS session failed: %w", err)
	}
	return &goADSClient{session: sess}, nil
}

func (c *goADSClient) Connect(ctx context.Context) error     { return c.session.Connect(ctx) }
func (c *goADSClient) Close() error                          { return c.session.Close() }
func (c *goADSClient) IsClosed() bool                        { return c.session.IsClosed() }
func (c *goADSClient) LoadSymbols(ctx context.Context) error { return c.session.LoadSymbols(ctx) }

func (c *goADSClient) GetSymbol(ctx context.Context, name string) (SymbolInfo, error) {
	v, err := c.session.GetSymbol(ctx, name)
	if err != nil {
		return SymbolInfo{}, err
	}
	return SymbolInfo{DataType: v.DataType, BaseType: v.BaseTypeName(), Length: v.Length}, nil
}

func (c *goADSClient) ReadMultipleSymbols(ctx context.Context, names []string) (map[string]string, error) {
	values, err := c.session.ReadMultipleSymbols(ctx, names)
	// Since go-ads v2.3.0 a partial batch returns *BatchError with the good
	// values still in the map; translate it so read.go keeps them.
	var batchErr *adsLib.BatchError
	if errors.As(err, &batchErr) {
		failed := make([]NotifyResult, len(batchErr.Items))
		for i, item := range batchErr.Items {
			failed[i] = NotifyResult{
				SymbolName: item.Symbol,
				Skipped:    item.Skipped != nil,
				Code:       uint32(item.Error),
			}
		}
		return values, &BatchReadError{Requested: batchErr.Requested, Failed: failed}
	}
	return values, err
}

func (c *goADSClient) ReadFromSymbol(ctx context.Context, name string) (string, error) {
	return c.session.ReadFromSymbol(ctx, name)
}

// toTransMode maps the plugin's plain transmission-mode code to the go-ads
// protocol constant (see transmissionModeValue in ads.go for the code table).
func toTransMode(code int) adsLib.TransMode {
	switch code {
	case 1:
		return adsLib.TransModeServerCycle
	case 2:
		return adsLib.TransModeServerOnChange2
	case 3:
		return adsLib.TransModeServerCycle2
	default:
		return adsLib.TransModeServerOnChange
	}
}

func (c *goADSClient) AddNotifications(ctx context.Context, cfgs []NotifyConfig, ch chan *adsLib.Update) ([]NotifyResult, error) {
	adsCfgs := make([]adsLib.NotificationConfig, len(cfgs))
	for i, cfg := range cfgs {
		adsCfgs[i] = adsLib.NotificationConfig{
			SymbolName:       cfg.SymbolName,
			MaxDelay:         cfg.MaxDelay,
			CycleTime:        cfg.CycleTime,
			TransmissionMode: toTransMode(cfg.TransmissionMode),
		}
	}
	results, err := c.session.AddSymbolNotifications(ctx, adsCfgs, ch)
	if err != nil {
		return nil, err
	}
	out := make([]NotifyResult, len(results))
	for i, r := range results {
		out[i] = NotifyResult{
			SymbolName: cfgs[i].SymbolName,
			Registered: r.Skipped == nil && r.Error == adsLib.ReturnCodeNoErrors,
			Skipped:    r.Skipped != nil,
			Code:       uint32(r.Error),
		}
	}
	return out, nil
}

// Channel accessors — the one deliberate *adsLib.Update leak. go-ads Update.Value
// is already a decoded string (see the Client value contract).
func sampleName(u *adsLib.Update) string    { return u.Variable }
func sampleValue(u *adsLib.Update) string   { return u.Value }
func sampleTime(u *adsLib.Update) time.Time { return u.TimeStamp }
