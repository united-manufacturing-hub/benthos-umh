package sensorconnect_plugin

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/goccy/go-json"
	"io"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
)

func (s *SensorConnectInput) FetchAndStoreIoDDFile(ctx context.Context, vendorId int64, deviceId int) (err error) {
	// download iodd file
	zap.S().Debugf("Downloading iodd file for vendorId: %v, deviceId: %v", vendorId, deviceId)
	fileMap, err := s.GetIoddFile(ctx, vendorId, deviceId)
	if err != nil {
		return err
	}

	latest := int64(0)
	index := 0
	for i, file := range fileMap {
		if file.Context.UploadDate > latest {
			index = i
			latest = file.Context.UploadDate
		}
	}

	fileMapKey := IoddFilemapKey{
		VendorId: vendorId,
		DeviceId: deviceId,
	}

	s.IoDeviceMap.Store(fileMapKey, fileMap[index].File)
	return
}

// GetIoddFile downloads a ioddfiles from ioddfinder and returns a list of valid files for the request (This can be multiple, if the vendor has multiple languages or versions published)
func (s *SensorConnectInput) GetIoddFile(ctx context.Context, vendorId int64, deviceId int) (files []IoDDFile, err error) {
	var body []byte
	body, err = s.GetUrlWithRetry(ctx,
		fmt.Sprintf(
			"https://ioddfinder.io-link.com/api/drivers?page=0&size=2000&status=APPROVED&status=UPLOADED&deviceIdString=%d",
			deviceId))
	if err != nil {
		return
	}
	var ioddfinder Ioddfinder
	ioddfinder, err = UnmarshalIoddfinder(body)
	if err != nil {
		return
	}

	validIds := make([]int, 0)

	for i, content := range ioddfinder.Content {
		if content.VendorID == vendorId {
			validIds = append(validIds, i)
		}
	}

	if len(validIds) == 0 {
		err = fmt.Errorf("No IODD file for vendorID [%d] and deviceID [%d]", vendorId, deviceId)
		return
	}

	files = make([]IoDDFile, 0)

	for _, id := range validIds {
		ioddId := ioddfinder.Content[id].IoddID
		var ioddzip []byte
		ioddzip, err = s.GetUrlWithRetry(ctx,
			fmt.Sprintf(
				"https://ioddfinder.io-link.com/api/vendors/%d/iodds/%d/files/zip/rated",
				vendorId,
				ioddId))
		if err != nil {
			return
		}
		var zipReader *zip.Reader
		zipReader, err = zip.NewReader(bytes.NewReader(ioddzip), int64(len(ioddzip)))
		if err != nil {
			return
		}

		for _, zipFile := range zipReader.File {
			if strings.HasSuffix(zipFile.Name, "xml") {
				var file []byte
				file, err = readZipFile(zipFile)
				if err != nil {
					return
				}
				files = append(
					files, IoDDFile{
						Name:    zipFile.Name,
						File:    file,
						Context: ioddfinder.Content[id],
					})
			}
		}
	}

	return
}

// IoDDFile is a helper structure with the name, file and additional context of the iodd file
type IoDDFile struct {
	Name    string
	File    []byte
	Context Content
}

// readZipFile gets the content of a zip file
func readZipFile(zf *zip.File) ([]byte, error) {
	f, err := zf.Open()
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// GetUrlWithRetry attempts to GET the specified URL up to maxRetries times with exponential backoff.
func (s *SensorConnectInput) GetUrlWithRetry(ctx context.Context, url string) ([]byte, error) {
	s.logger.Debugf("Attempting to GET URL: %s", url)
	var body []byte
	var status int
	var err error

	maxRetries := 10
	minBackoff := 10 * time.Second
	maxBackoff := 60 * time.Second

	for i := 0; i < maxRetries; i++ {
		body, err, status = s.GetUrl(ctx, url)
		if err != nil {
			s.logger.Errorf("Error fetching URL %s: %v", url, err)
			return nil, err
		}
		if status == http.StatusOK {
			s.logger.Debugf("Successfully fetched URL %s on attempt %d", url, i+1)
			return body, nil
		}
		backoff := GetBackoffTime(int64(i), minBackoff, maxBackoff)
		s.logger.Debugf("Attempt %d failed with status %d. Retrying in %v...", i+1, status, backoff)
		select {
		case <-time.After(backoff):
			// Continue to next attempt
		case <-ctx.Done():
			s.logger.Warnf("Context canceled while waiting to retry URL %s", url)
			return nil, ctx.Err()
		}
	}

	return nil, errors.New("failed to retrieve URL after 10 attempts")
}

// GetUrl executes a GET request to the specified URL and returns the response body and status code.
func (s *SensorConnectInput) GetUrl(ctx context.Context, url string) ([]byte, error, int) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		s.logger.Warnf("Failed to create GET request for URL %s: %v", url, err)
		return nil, err, 0
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.logger.Debugf("No response from URL %s: %v", url, err)
		return nil, err, 0
	}
	defer resp.Body.Close()

	status := resp.StatusCode
	if status != http.StatusOK {
		s.logger.Debugf("Received non-200 status code %d for URL %s", status, url)
		return nil, nil, status
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		s.logger.Errorf("Failed to read response body from URL %s: %v", url, err)
		return nil, err, status
	}

	return body, nil, status
}

func UnmarshalIoddfinder(data []byte) (Ioddfinder, error) {
	var r Ioddfinder

	err := json.Unmarshal(data, &r)
	return r, err
}

func (r *Ioddfinder) Marshal() ([]byte, error) {

	return json.Marshal(r)
}

// GetBackoffTime calculates the backoff duration based on the attempt number.
// It uses exponential backoff with jitter, bounded by min and max durations.
func GetBackoffTime(attempt int64, min, max time.Duration) time.Duration {
	exponent := float64(attempt)
	backoff := time.Duration(float64(min) * math.Pow(2, exponent))
	if backoff > max {
		backoff = max
	}
	// Add jitter: random duration between 0 and backoff
	jitter := time.Duration(rand.Int63n(int64(backoff)))
	return jitter
}

type Ioddfinder struct {
	Content          []Content     `json:"content"`
	Sort             []interface{} `json:"sort"`
	Number           int64         `json:"number"`
	Size             int64         `json:"size"`
	NumberOfElements int64         `json:"numberOfElements"`
	TotalPages       int64         `json:"totalPages"`
	TotalElements    int64         `json:"totalElements"`
	First            bool          `json:"first"`
	Last             bool          `json:"last"`
}

type Content struct {
	ProductName        string `json:"productName"`
	IndicationOfSource string `json:"indicationOfSource"`
	IoLinkRev          string `json:"ioLinkRev"`
	VersionString      string `json:"versionString"`
	IoddStatus         string `json:"ioddStatus"`
	ProductID          string `json:"productId"`
	VendorName         string `json:"vendorName"`
	ProductVariantID   int64  `json:"productVariantId"`
	UploadDate         int64  `json:"uploadDate"`
	VendorID           int64  `json:"vendorId"`
	IoddID             int64  `json:"ioddId"`
	DeviceID           int64  `json:"deviceId"`
	HasMoreVersions    bool   `json:"hasMoreVersions"`
}
