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

package managedtools

import (
	"fmt"
	"os"
	"slices"
)

// ExpandPlaceholders replaces every ${NAME} (and $NAME) in text with vars[NAME]: the probe manifests of
// test/e2e/data/managed are filled this way by the lanes, not by envsubst, so no environment variable can leak into
// a workload. It fails, naming them, when text uses placeholders vars does not define, so a typo cannot silently
// render an empty field.
//
// Parameters:
//   - text is the manifest.
//   - vars are the placeholder values.
func ExpandPlaceholders(text string, vars map[string]string) (string, error) {
	var missing []string
	// os.Expand only parses the placeholders and calls the mapping; it reads no environment variable.
	expanded := os.Expand(text, func(name string) string {
		value, ok := vars[name]
		if !ok && !slices.Contains(missing, name) {
			missing = append(missing, name)
		}
		return value
	})
	if len(missing) > 0 {
		slices.Sort(missing)
		return "", fmt.Errorf("the manifest uses placeholders without a value: %v", missing)
	}
	return expanded, nil
}
