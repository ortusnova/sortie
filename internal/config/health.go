package config

import (
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
)

const healthCredentialsField = "health.credentials"

// healthCredentialKeys are the keys of a health.credentials entry.
var healthCredentialKeys = []string{"name", "env", "expires_command", "login"}

// HealthConfig is the parsed health section: what Sortie's own health
// check reads beyond the rest of the configuration. The zero value reads
// nothing more.
type HealthConfig struct {
	// DeployState is the absolute path of the deploy state file a deploy
	// kit writes, with ~ and $VAR expanded; empty when no kit deploys
	// this Sortie.
	DeployState string

	// Credentials lists the credentials the health check watches, in
	// written order.
	Credentials []HealthCredential
}

// HealthCredential is a credential the runs use that the health check
// watches: that it is present and, when its expiry is known, that it
// does not expire soon.
type HealthCredential struct {
	// Name names the credential, matching [A-Za-z0-9_-]+, unique in the
	// list.
	Name string

	// Env optionally names a variable of Sortie's own environment that
	// holds the value, which the check finds set. Without ExpiresCommand
	// the expiry is read from the value when it is a JSON Web Token.
	Env string

	// ExpiresCommand optionally holds a shell command run on the host
	// whose standard output is when the value expires: an RFC 3339 time,
	// or Unix seconds or milliseconds.
	ExpiresCommand string

	// Login optionally names, as a person knows it, the login that
	// renews the value. The health check's person action names it.
	Login string
}

// buildHealthConfig parses the health section of raw. An absent section
// is the zero value.
func buildHealthConfig(raw map[string]any) (HealthConfig, error) {
	v, present := raw["health"]
	if !present || v == nil {
		return HealthConfig{}, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return HealthConfig{}, &ConfigError{Field: "health", Message: fmt.Sprintf("expected map, got %T", v)}
	}
	var cfg HealthConfig
	path, present, err := requireStringField(m, "deploy_state", "health.deploy_state")
	if err != nil {
		return HealthConfig{}, err
	}
	if present {
		if cfg.DeployState, err = parseDeployState(path); err != nil {
			return HealthConfig{}, err
		}
	}
	if cfg.Credentials, err = parseHealthCredentials(m["credentials"]); err != nil {
		return HealthConfig{}, err
	}
	return cfg, nil
}

// parseDeployState reads health.deploy_state, which must name an
// absolute path after expansion.
func parseDeployState(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", &ConfigError{Field: "health.deploy_state", Message: "must not be empty; remove the key when no kit deploys this Sortie"}
	}
	expanded, err := ExpandPath(path, true)
	if err != nil {
		return "", &ConfigError{Field: "health.deploy_state", Message: err.Error()}
	}
	if !filepath.IsAbs(expanded) {
		return "", &ConfigError{Field: "health.deploy_state", Message: fmt.Sprintf("%q is not an absolute path after ~ and $VAR expansion", expanded)}
	}
	return filepath.Clean(expanded), nil
}

// parseHealthCredentials reads health.credentials.
func parseHealthCredentials(value any) ([]HealthCredential, error) {
	if value == nil {
		return nil, nil
	}
	list, ok := value.([]any)
	if !ok {
		return nil, typeError(healthCredentialsField, "list", value)
	}
	var creds []HealthCredential
	seen := map[string]int{}
	for i, elem := range list {
		field := fmt.Sprintf("%s[%d]", healthCredentialsField, i)
		entry, ok := elem.(map[string]any)
		if !ok {
			return nil, typeError(field, "mapping", elem)
		}
		for _, key := range slices.Sorted(maps.Keys(entry)) {
			if !slices.Contains(healthCredentialKeys, key) {
				return nil, &ConfigError{Field: field + "." + key, Message: "unknown key; valid: " + strings.Join(healthCredentialKeys, ", ")}
			}
		}
		name, err := requiredString(entry, "name", field+".name")
		if err != nil {
			return nil, err
		}
		if !credentialNamePattern.MatchString(name) {
			return nil, &ConfigError{Field: field + ".name", Message: fmt.Sprintf("invalid credential name %q; use letters, digits, '_' and '-'", name)}
		}
		if first, dup := seen[name]; dup {
			return nil, &ConfigError{Field: field + ".name", Message: fmt.Sprintf("credential name %q is already used by %s[%d]", name, healthCredentialsField, first)}
		}
		seen[name] = i
		cred := HealthCredential{Name: name}
		env, envPresent, err := requireStringField(entry, "env", field+".env")
		if err != nil {
			return nil, err
		}
		if envPresent && !envVarNamePattern.MatchString(env) {
			return nil, &ConfigError{Field: field + ".env", Message: fmt.Sprintf("invalid environment variable name %q", env)}
		}
		cred.Env = env
		expires, expiresPresent, err := requireStringField(entry, "expires_command", field+".expires_command")
		if err != nil {
			return nil, err
		}
		if expiresPresent && strings.TrimSpace(expires) == "" {
			return nil, &ConfigError{Field: field + ".expires_command", Message: "must not be empty; remove the key when the expiry is not known"}
		}
		cred.ExpiresCommand = expires
		if !envPresent && !expiresPresent {
			return nil, &ConfigError{Field: field, Message: "sets neither env nor expires_command; the health check needs one to watch the credential"}
		}
		login, present, err := requireStringField(entry, "login", field+".login")
		if err != nil {
			return nil, err
		}
		cred.Login = strings.TrimSpace(login)
		if present && cred.Login == "" {
			return nil, &ConfigError{Field: field + ".login", Message: "must not be empty; remove the key to name the credential instead"}
		}
		creds = append(creds, cred)
	}
	return creds, nil
}
