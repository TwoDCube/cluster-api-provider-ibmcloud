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
	"errors"
	"fmt"

	"github.com/IBM/go-sdk-core/v5/core"

	"k8s.io/apimachinery/pkg/types"
)

// KindController is the IdentityKey kind of CAPIBM's own credentials (the manager-bootstrap-credentials Secret).
const KindController = "Controller"

// ControllerDescription names the controller identity in condition messages and events, which name every identity by
// its Description. It is deliberately generic: messages never carry an IAM ID, a key owner, an e-mail or a key
// fingerprint.
const ControllerDescription = "the CAPIBM controller identity"

// IdentityKey identifies an identity CAPIBM can act as. It is comparable, holds no secret material, and is the identity
// part of every authenticator, client and data cache key.
type IdentityKey struct {
	// Kind is KindController, or (in a later release) the kind of the object that defines the identity.
	Kind string
	// Name is the name of that object; "" for the controller identity.
	Name string
	// UID is the UID of that object, so that a deleted and re-created object with the same name is a new key; "" for
	// the controller identity.
	UID types.UID
}

// ControllerKey returns the key of CAPIBM's own identity. It is a function rather than a package variable, so that the
// managed reconcile path holds no package-level mutable value.
func ControllerKey() IdentityKey {
	return IdentityKey{Kind: KindController}
}

// String names the key in logs and errors, for example "Controller" or "SomeKind/name (uid 1234)". The key holds no
// secret material, so printing it is safe.
func (k IdentityKey) String() string {
	if k.Name == "" && k.UID == "" {
		return k.Kind
	}
	return fmt.Sprintf("%s/%s (uid %s)", k.Kind, k.Name, k.UID)
}

// Identity is one resolved identity: everything a client builder needs to authenticate as it, and everything a
// message needs to name it.
type Identity struct {
	// Key names the identity in every cache.
	Key IdentityKey
	// Version changes whenever a credential-forming input changes; an authenticator cached for another Version is rebuilt.
	// It is "" for the controller identity, whose mounted Secret file is re-read when the client builder rebuilds the
	// authenticator every hour.
	Version string
	// Description names the identity in condition messages and events, for example ControllerDescription. Never an IAM ID,
	// an account owner, an e-mail or a key fingerprint.
	Description string
	// Source builds a fresh authenticator. nil means the controller identity, which only the client builder builds,
	// with the factory cmd/main.go injects.
	Source CredentialSource
}

// Validate returns an error when the identity cannot be used to build a client: an empty key kind, an empty
// description, or a missing Source on any identity other than the controller identity. Callers get an Identity only
// from a Resolver, so a failure here is a programming error in that Resolver; the message says what it returned.
func (id Identity) Validate() error {
	var errs []error
	if id.Key.Kind == "" {
		errs = append(errs, errors.New("credentials: the identity has no Key.Kind; a Resolver must return a keyed identity, for example ControllerKey()"))
	}
	if id.Description == "" {
		errs = append(errs, fmt.Errorf("credentials: identity %s has no Description; messages name every identity, for example %q",
			id.Key, ControllerDescription))
	}
	if core.IsNil(id.Source) && id.Key.Kind != "" && id.Key != ControllerKey() {
		errs = append(errs, fmt.Errorf("credentials: identity %s has no Source; only the controller identity (%s) is built without one",
			id.Key, ControllerKey()))
	}
	return errors.Join(errs...)
}

// CredentialSource builds an authenticator with a go-sdk-core builder. This feature has no implementation; a later
// multi-tenancy release adds one per identity type (an API key or a trusted profile).
type CredentialSource interface {
	// NewAuthenticator returns a new authenticator for the identity. The client builder calls it when it has no
	// usable authenticator for the identity's key and caches the result; it never caches an error or a nil
	// authenticator.
	NewAuthenticator() (core.Authenticator, error)
}
