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

package sensorconnect_plugin

import (
	"context"
	"encoding/xml"
	"fmt"
	"os"
)

// IoDevice Parsing of IODD File content
type IoDevice struct {
	DocumentInfo           DocumentInfo           `xml:"DocumentInfo"`
	ExternalTextCollection ExternalTextCollection `xml:"ExternalTextCollection"`
	ProfileBody            ProfileBody            `xml:"ProfileBody"`
}

type DocumentInfo struct {
	ReleaseDate string `xml:"releaseDate,attr"`
}

type ProfileBody struct {
	DeviceIdentity DeviceIdentity `xml:"DeviceIdentity"`
	DeviceFunction DeviceFunction `xml:"DeviceFunction"`
}

type DeviceIdentity struct {
	VendorName string `xml:"vendorName,attr"`
	DeviceId   int    `xml:"deviceId,attr"` // Id of type of a device, given by device vendor
	VendorId   int64  `xml:"vendorId,attr"`
}

type ExternalTextCollection struct {
	PrimaryLanguage PrimaryLanguage `xml:"PrimaryLanguage"`
}

type PrimaryLanguage struct {
	Text []Text `xml:"Text"`
}

type Text struct {
	Id    string `xml:"id,attr"`
	Value string `xml:"value,attr"`
}

type DeviceFunction struct {
	DatatypeCollection    DatatypeCollection    `xml:"DatatypeCollection"`
	ProcessDataCollection ProcessDataCollection `xml:"ProcessDataCollection"`
}

type DatatypeCollection struct {
	DatatypeArray []Datatype `xml:"Datatype"`
}

type ProcessDataCollection struct {
	ProcessData ProcessData `xml:"ProcessData"`
}

type ProcessData struct {
	ProcessDataIn ProcessDataIn `xml:"ProcessDataIn"`
}

type ProcessDataIn struct {
	DatatypeRef DatatypeRef `xml:"DatatypeRef"`
	Name        Name        `xml:"Name"`
	Datatype    Datatype    `xml:"Datatype"`
}

type Datatype struct {
	Type             string        `xml:"type,attr"`
	Id               string        `xml:"id,attr"`
	RecordItemArray  []RecordItem  `xml:"RecordItem"`
	SingleValueArray []SingleValue `xml:"SingleValue"`
	ValueRangeArray  []ValueRange  `xml:"ValueRange"`
	BitLength        uint          `xml:"bitLength,attr"`
	FixedLength      uint          `xml:"fixedLength,attr"`
}

type SingleValue struct {
	Value string `xml:"value,attr"` // can be any kind of type depending on Type of item -> determine later
	Name  Name   `xml:"Name"`
}

type ValueRange struct {
	LowerValue string `xml:"lowerValue,attr"` // can be any kind of type depending on Type of item -> determine later
	UpperValue string `xml:"upperValue,attr"` // can be any kind of type depending on Type of item -> determine later
	Name       Name   `xml:"Name"`
}

type RecordItem struct {
	Name           Name           `xml:"Name"`
	DatatypeRef    DatatypeRef    `xml:"DatatypeRef"`
	SimpleDatatype SimpleDatatype `xml:"SimpleDatatype"`
	BitOffset      int            `xml:"bitOffset,attr"`
}

type Name struct {
	TextId string `xml:"textId,attr"`
}

type DatatypeRef struct {
	DatatypeId string `xml:"datatypeId,attr"`
}

type SimpleDatatype struct {
	ValueRange  ValueRange  `xml:"ValueRange"`
	SingleValue SingleValue `xml:"SingleValue"`
	Type        string      `xml:"type,attr"`
	BitLength   uint        `xml:"bitLength,attr"`
	FixedLength uint        `xml:"fixedLength,attr"`
}

// IoddFilemapKey represents the key for the IODD file map
type IoddFilemapKey struct {
	VendorId int64
	DeviceId int
}

// AddNewDeviceToIoddFilesAndMap uses ioddFilemapKey to download new IODD file (if key not already in IoDevice map).
func (s *SensorConnectInput) AddNewDeviceToIoddFilesAndMap(ctx context.Context,
	ioddFilemapKey IoddFilemapKey,
) error {
	s.logger.Debugf("Requesting IODD file %v -> %s", ioddFilemapKey)
	err := s.RequestSaveIoddFile(ctx, ioddFilemapKey)
	if err != nil {
		s.logger.Errorf("Error in AddNewDeviceToIoddFilesAndMap: %s", err.Error())
		return err
	}
	s.logger.Debugf("Reading IODD files %v -> %s")

	_, ok := s.IoDeviceMap.Load(ioddFilemapKey)
	if !ok {
		s.logger.Errorf("Error in AddNewDeviceToIoddFilesAndMap: %s", err.Error())
		return err
	}

	return nil
}

// UnmarshalIoddFile unmarshals the IODD XML file into an IoDevice struct
func (s *SensorConnectInput) UnmarshalIoddFile(ioddFile []byte, absoluteFilePath string) (IoDevice, error) {
	payload := IoDevice{}

	// Unmarshal file
	err := xml.Unmarshal(ioddFile, &payload)
	if err != nil {
		s.logger.Errorf(
			"Unmarshaling of IoDevice from %s failed. Deleting IODD file. Error: %v",
			absoluteFilePath,
			err)
		errRemove := os.Remove(absoluteFilePath)
		if errRemove != nil {
			s.logger.Errorf("Removing file: %s failed. Error: %v", absoluteFilePath, errRemove)
		}
		return payload, fmt.Errorf("failed to unmarshal IODD file: %w", err)
	}
	return payload, nil
}

// RequestSaveIoddFile will download the IODD file if the ioddFilemapKey is not already in IoDeviceMap
func (s *SensorConnectInput) RequestSaveIoddFile(ctx context.Context, ioddFilemapKey IoddFilemapKey) error {
	// Check if IoDevice already in IoDeviceMap
	if _, ok := s.IoDeviceMap.Load(ioddFilemapKey); ok {
		return nil
	}
	// Execute download and saving of IODD file
	err := s.FetchAndStoreIoDDFile(ctx, ioddFilemapKey.VendorId, ioddFilemapKey.DeviceId)
	if err != nil {
		s.logger.Errorf("Saving error: %s", err.Error())
		return err
	}
	return nil
}
