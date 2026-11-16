/*
Copyright 2026 The Kubernetes Authors.

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

package credentials

import (
	"testing"

	"github.com/IBM/go-sdk-core/v5/core"

	. "github.com/onsi/gomega"
)

// staticSource is a CredentialSource that returns a fixed bearer-token authenticator.
type staticSource struct{}

// NewAuthenticator implements CredentialSource.
func (staticSource) NewAuthenticator() (core.Authenticator, error) {
	return core.NewBearerTokenAuthenticator("test-token")
}

func TestControllerKey(t *testing.T) {
	g := NewWithT(t)

	key := ControllerKey()
	g.Expect(key).To(Equal(IdentityKey{Kind: KindController}))
	// A function, not a shared variable: changing one result must not change the next.
	key.Name = "changed"
	g.Expect(ControllerKey()).To(Equal(IdentityKey{Kind: KindController}))
	// The key is comparable, so it can key a map.
	seen := map[IdentityKey]bool{ControllerKey(): true}
	g.Expect(seen[IdentityKey{Kind: KindController}]).To(BeTrue())
}

func TestIdentityKeyString(t *testing.T) {
	testCases := []struct {
		name string
		key  IdentityKey
		want string
	}{
		{name: "controller key prints its kind only", key: ControllerKey(), want: "Controller"},
		{name: "object key prints kind, name and uid", key: IdentityKey{Kind: "SomeKind", Name: "team-a", UID: "uid-1"}, want: "SomeKind/team-a (uid uid-1)"},
		{name: "empty key prints empty", key: IdentityKey{}, want: ""},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(tc.key.String()).To(Equal(tc.want))
		})
	}
}

func TestIdentityValidate(t *testing.T) {
	otherKey := IdentityKey{Kind: "SomeKind", Name: "team-a", UID: "uid-1"}
	testCases := []struct {
		name     string
		identity Identity
		wantErr  []string
	}{
		{
			name:     "controller identity without source is valid",
			identity: Identity{Key: ControllerKey(), Description: ControllerDescription},
		},
		{
			name:     "other identity with source is valid",
			identity: Identity{Key: otherKey, Description: "the team-a identity", Source: staticSource{}},
		},
		{
			name:     "empty key kind is named",
			identity: Identity{Description: ControllerDescription},
			wantErr:  []string{"no Key.Kind", "ControllerKey()"},
		},
		{
			name:     "empty description is named with the expected form",
			identity: Identity{Key: ControllerKey()},
			wantErr:  []string{"identity Controller has no Description", ControllerDescription},
		},
		{
			name:     "other identity without source is rejected",
			identity: Identity{Key: otherKey, Description: "the team-a identity"},
			wantErr:  []string{"SomeKind/team-a (uid uid-1) has no Source"},
		},
		{
			name:     "every problem is reported at once",
			identity: Identity{Key: otherKey},
			wantErr:  []string{"has no Description", "has no Source"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			err := tc.identity.Validate()
			if len(tc.wantErr) == 0 {
				g.Expect(err).NotTo(HaveOccurred())
				return
			}
			g.Expect(err).To(HaveOccurred())
			for _, want := range tc.wantErr {
				g.Expect(err.Error()).To(ContainSubstring(want))
			}
		})
	}
}
