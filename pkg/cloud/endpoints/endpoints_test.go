/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package endpoints

import (
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/onsi/gomega"
)

const (
	testContainersHost = "https://private.eu-de.containers.cloud.ibm.com/global"
	testVPCHost        = "https://vpchost:8080"
	testRCHost         = "https://rchost:8080"
	testRegionLon      = "lon"
	testRegionUS       = "us-south"
	testPowerVSID      = "powervs"
)

func TestParseFlags(t *testing.T) {
	testCases := []struct {
		name           string
		flagToParse    string
		expectedOutput []ServiceEndpoint
		expectedError  error
	}{
		{
			name:           "no configuration",
			flagToParse:    "",
			expectedOutput: nil,
			expectedError:  nil,
		},
		{
			name:           "none configuration",
			flagToParse:    "none",
			expectedOutput: nil,
			expectedError:  nil,
		},
		{
			name:        "single region, single vpc service",
			flagToParse: "eu-gb:vpc=" + testVPCHost,
			expectedOutput: []ServiceEndpoint{
				{
					ID:     "vpc",
					URL:    testVPCHost,
					Region: "eu-gb",
				},
			},
			expectedError: nil,
		},
		{
			name:        "single region, single powervs service",
			flagToParse: testRegionLon + ":powervs=https://pvshost:8080",
			expectedOutput: []ServiceEndpoint{
				{
					ID:     testPowerVSID,
					URL:    "https://pvshost:8080",
					Region: testRegionLon,
				},
			},
			expectedError: nil,
		},
		{
			name:        "single region, multiple services",
			flagToParse: testRegionLon + ":powervs=https://pvshost:8080,rc=" + testRCHost,
			expectedOutput: []ServiceEndpoint{
				{
					ID:     testPowerVSID,
					URL:    "https://pvshost:8080",
					Region: testRegionLon,
				},
				{
					ID:     "rc",
					URL:    testRCHost,
					Region: testRegionLon,
				},
			},
			expectedError: nil,
		},
		{
			name:           "single region, duplicate service",
			flagToParse:    "eu-gb:vpc=https://localhost:8080,vpc=" + testVPCHost,
			expectedOutput: nil,
			expectedError:  errServiceEndpointDuplicateID,
		},
		{
			name:           "single region, non-valid URI",
			flagToParse:    "eu-gb:vpc=fdsfs",
			expectedOutput: nil,
			expectedError:  errServiceEndpointURL,
		},
		{
			name:        "multiples regions",
			flagToParse: "eu-gb:vpc=" + testVPCHost + ";" + testRegionLon + ":powervs=https://pvshost:8080,rc=" + testRCHost,
			expectedOutput: []ServiceEndpoint{
				{
					ID:     "vpc",
					URL:    testVPCHost,
					Region: "eu-gb",
				},
				{
					ID:     testPowerVSID,
					URL:    "https://pvshost:8080",
					Region: testRegionLon,
				},
				{
					ID:     "rc",
					URL:    testRCHost,
					Region: testRegionLon,
				},
			},
			expectedError: nil,
		},
		{
			name:        "multiples regions, multiple services",
			flagToParse: "eu-gb:vpc=" + testVPCHost + ";" + testRegionLon + ":powervs=https://pvshost:8080,rc=" + testRCHost + ";" + testRegionUS + ":powervs=https://pvshost-us:8080",
			expectedOutput: []ServiceEndpoint{
				{
					ID:     "vpc",
					URL:    testVPCHost,
					Region: "eu-gb",
				},
				{
					ID:     testPowerVSID,
					URL:    "https://pvshost:8080",
					Region: testRegionLon,
				},
				{
					ID:     "rc",
					URL:    testRCHost,
					Region: testRegionLon,
				},
				{
					ID:     testPowerVSID,
					URL:    "https://pvshost-us:8080",
					Region: testRegionUS,
				},
			},
			expectedError: nil,
		},
		{
			name:           "invalid config",
			flagToParse:    "eu-gb=localhost",
			expectedOutput: nil,
			expectedError:  errServiceEndpointRegion,
		},
		{
			name:        "containers service per region",
			flagToParse: "eu-de:containers=" + testContainersHost + ";" + testRegionUS + ":containers=https://private.us-south.containers.cloud.ibm.com/global",
			expectedOutput: []ServiceEndpoint{
				{
					ID:     "containers",
					URL:    testContainersHost,
					Region: "eu-de",
				},
				{
					ID:     "containers",
					URL:    "https://private.us-south.containers.cloud.ibm.com/global",
					Region: testRegionUS,
				},
			},
			expectedError: nil,
		},
		{
			name:           "containers service defined twice for one region",
			flagToParse:    "eu-de:containers=" + testContainersHost + ",containers=https://other:8080/global",
			expectedOutput: nil,
			expectedError:  errServiceEndpointDuplicateID,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := ParseServiceEndpointFlag(tc.flagToParse)
			require.ErrorIs(t, err, tc.expectedError)
			require.ElementsMatch(t, out, tc.expectedOutput)
		})
	}
}

func TestFetchVPCEndpoint(t *testing.T) {
	testCases := []struct {
		name            string
		region          string
		serviceEndpoint []ServiceEndpoint
		expectedOutput  string
	}{
		{
			name:            "Return constructed endpoint",
			region:          testRegionUS,
			serviceEndpoint: []ServiceEndpoint{},
			expectedOutput:  "https://us-south.iaas.cloud.ibm.com/v1",
		},
		{
			name:   "Return fetched endpoint",
			region: testRegionUS,
			serviceEndpoint: []ServiceEndpoint{
				{
					ID:     "vpc",
					URL:    testVPCHost,
					Region: testRegionUS,
				},
			},
			expectedOutput: testVPCHost,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			out := FetchVPCEndpoint(tc.region, tc.serviceEndpoint)
			require.Equal(t, tc.expectedOutput, out)
		})
	}
}

func TestFetchPVSEndpoint(t *testing.T) {
	testCases := []struct {
		name            string
		region          string
		serviceEndpoint []ServiceEndpoint
		expectedOutput  string
	}{
		{
			name:            "Return empty endpoint",
			region:          testRegionUS,
			serviceEndpoint: []ServiceEndpoint{},
			expectedOutput:  "",
		},
		{
			name:   "Return fetched endpoint",
			region: testRegionUS,
			serviceEndpoint: []ServiceEndpoint{
				{
					ID:     testPowerVSID,
					URL:    "https://powervshost:8080",
					Region: testRegionUS,
				},
			},
			expectedOutput: "https://powervshost:8080",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			out := FetchPVSEndpoint(tc.region, tc.serviceEndpoint)
			require.Equal(t, tc.expectedOutput, out)
		})
	}
}

func TestFetchRCEndpoint(t *testing.T) {
	testCases := []struct {
		name            string
		serviceEndpoint []ServiceEndpoint
		expectedOutput  string
	}{
		{
			name:            "Return empty endpoint",
			serviceEndpoint: []ServiceEndpoint{},
			expectedOutput:  "",
		},
		{
			name: "Return fetched endpoint",
			serviceEndpoint: []ServiceEndpoint{
				{
					ID:     "rc",
					URL:    testRCHost,
					Region: testRegionUS,
				},
			},
			expectedOutput: testRCHost,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			out := FetchRCEndpoint(tc.serviceEndpoint)
			require.Equal(t, tc.expectedOutput, out)
		})
	}
}

func TestFetchEndpoints(t *testing.T) {
	testCases := []struct {
		name            string
		serviceEndpoint []ServiceEndpoint
		serviceID       string
		expectedOutput  string
	}{
		{
			name:            "With empty service endpoints",
			serviceEndpoint: []ServiceEndpoint{},
			expectedOutput:  "",
		},
		{
			name:      "With invalid service id",
			serviceID: "abc",
			serviceEndpoint: []ServiceEndpoint{
				{
					ID:     "rc",
					URL:    testRCHost,
					Region: testRegionUS,
				},
			},
			expectedOutput: "",
		},
		{
			name:      "With service id not preset in service endpoints",
			serviceID: testPowerVSID,
			serviceEndpoint: []ServiceEndpoint{
				{
					ID:     "rc",
					URL:    testRCHost,
					Region: testRegionUS,
				},
			},
			expectedOutput: "",
		},
		{
			name:      "With valid service id",
			serviceID: "rc",
			serviceEndpoint: []ServiceEndpoint{
				{
					ID:     "rc",
					URL:    testRCHost,
					Region: testRegionUS,
				},
				{
					ID:  testPowerVSID,
					URL: "https://powervs:8081",
				},
			},
			expectedOutput: testRCHost,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			out := FetchEndpoints(tc.serviceID, tc.serviceEndpoint)
			require.Equal(t, tc.expectedOutput, out)
		})
	}
}

func TestCostructRegionFromZone(t *testing.T) {
	testCases := []struct {
		name           string
		zone           string
		expectedRegion string
	}{
		{
			name:           "Return region dal",
			zone:           "dal12",
			expectedRegion: "dal",
		},
		{
			name:           "Return region eu-de",
			zone:           "eu-de-1",
			expectedRegion: "eu-de",
		},
		{
			name:           "Return region lon04",
			zone:           "lon04",
			expectedRegion: "lon",
		},
		{
			name:           "Return region mon01",
			zone:           "mon01",
			expectedRegion: "mon",
		},
		{
			name:           "Return region osa21",
			zone:           "osa21",
			expectedRegion: "osa",
		},
		{
			name:           "Return region sao01",
			zone:           "sao01",
			expectedRegion: "sao",
		},
		{
			name:           "Return region syd04",
			zone:           "syd04",
			expectedRegion: "syd",
		},
		{
			name:           "Return region tok04",
			zone:           "tok04",
			expectedRegion: "tok",
		},
		{
			name:           "Return region tor01",
			zone:           "tor01",
			expectedRegion: "tor",
		},
		{
			name:           "Return region wdc06",
			zone:           "wdc06",
			expectedRegion: "wdc",
		},
		{
			name:           "Return region us-east",
			zone:           "us-east",
			expectedRegion: "us-east",
		},
		{
			name:           "Return region us-south",
			zone:           "us-south",
			expectedRegion: "us-south",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			out := ConstructRegionFromZone(tc.zone)
			require.Equal(t, tc.expectedRegion, out)
		})
	}
}

func TestFetchRegionalEndpoint(t *testing.T) {
	configured := []ServiceEndpoint{
		{ID: string(VPC), URL: testVPCHost, Region: "eu-de"},
		{ID: string(Containers), URL: testContainersHost, Region: "eu-de"},
		{ID: string(Containers), URL: "https://private.us-south.containers.cloud.ibm.com/global", Region: testRegionUS},
		{ID: string(RC), URL: testRCHost},
	}
	testCases := []struct {
		name            string
		serviceID       string
		region          string
		serviceEndpoint []ServiceEndpoint
		expectedOutput  string
	}{
		{
			name:            "the entry of the region wins",
			serviceID:       string(Containers),
			region:          "eu-de",
			serviceEndpoint: configured,
			expectedOutput:  testContainersHost,
		},
		{
			name:            "another region's entry is never used",
			serviceID:       string(Containers),
			region:          "jp-tok",
			serviceEndpoint: configured,
			expectedOutput:  "",
		},
		{
			name:            "a regionless entry is not a regional entry",
			serviceID:       string(RC),
			region:          "eu-de",
			serviceEndpoint: configured,
			expectedOutput:  "",
		},
		{
			name:            "another service of the same region is not returned",
			serviceID:       string(Containers),
			region:          testRegionUS,
			serviceEndpoint: []ServiceEndpoint{{ID: string(VPC), URL: testVPCHost, Region: testRegionUS}},
			expectedOutput:  "",
		},
		{
			name:            "no entries",
			serviceID:       string(Containers),
			region:          "eu-de",
			serviceEndpoint: nil,
			expectedOutput:  "",
		},
		{
			name:            "empty region",
			serviceID:       string(Containers),
			region:          "",
			serviceEndpoint: []ServiceEndpoint{{ID: string(Containers), URL: testContainersHost}},
			expectedOutput:  "",
		},
		{
			name:            "empty service ID",
			serviceID:       "",
			region:          "eu-de",
			serviceEndpoint: configured,
			expectedOutput:  "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(FetchRegionalEndpoint(tc.serviceID, tc.region, tc.serviceEndpoint)).To(Equal(tc.expectedOutput))
		})
	}
}
