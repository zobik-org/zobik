package deploy

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"

	"zobik.org/zobik/internal/console"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/secrets"
)

// The Secret Store in the console's directory (implementation §1.2.18): the age
// identity, stored apart from the root and without the password, because the
// handles are resolved with no one; and the store the console writes, which it
// copies into the secrets role's volume.
const (
	fileSecretsKey   = "secrets.key"
	fileSecretsStore = "secrets.age"
	unitSecrets      = "secrets"
)

// handleForm bounds a handle to what the panel and the command line name it
// with: up to 128 bytes in segments separated by /, none of them empty, . or ..
func handleForm(h string) bool { return len(h) <= 128 && handleSegments(h) }

var handleSegments = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9][A-Za-z0-9._-]*)*$`).MatchString

// secretsKey returns the age identity, generating it the first time.
func secretsKey(dir string) (string, error) {
	b, err := os.ReadFile(filepath.Join(dir, fileSecretsKey))
	if err == nil {
		return string(b), nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	key, err := secrets.NewKey()
	if err != nil {
		return "", err
	}
	return key, writeFile(dir, fileSecretsKey, []byte(key))
}

// secretsFiles are what the secrets role's volume receives: the key, the store
// as the console last wrote it, and the Entry Node's identity.
func secretsFiles(dir, entryNode string) ([]File, error) {
	key, err := secretsKey(dir)
	if err != nil {
		return nil, err
	}
	files := []File{
		{Path: secrets.FileKey, Mode: 0o600, Data: []byte(key)},
		{Path: secrets.FileEntryNode, Mode: 0o644, Data: []byte(entryNode)},
	}
	sealed, err := os.ReadFile(filepath.Join(dir, fileSecretsStore))
	if err == nil {
		files = append(files, File{Path: secrets.FileStore, Mode: 0o600, Data: sealed})
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	return files, nil
}

// WriteSecret sets the value behind handle, or removes it when value is nil, and
// copies the store into the secrets role's volume. It is an act of deployment:
// the value goes through neither the Bus nor the Context Store
// (implementation §6, The operator console). Rotating a credential is writing
// its handle again (architecture §3.16.6).
func WriteSecret(ctx context.Context, e *Engine, dir, network, image, handle string, value []byte) error {
	if !handleForm(handle) {
		return fmt.Errorf("%w: %q", console.ErrHandle, handle)
	}
	root, err := readRoot(dir)
	if err != nil {
		return err
	}
	if root == "" {
		return ErrNoPanel
	}
	key, err := secretsKey(dir)
	if err != nil {
		return err
	}
	sealed, err := os.ReadFile(filepath.Join(dir, fileSecretsStore))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	values, err := secrets.Unseal(key, sealed)
	if err != nil {
		return err
	}
	if value == nil {
		delete(values, handle)
	} else {
		values[handle] = value
	}
	if sealed, err = secrets.Seal(key, values); err != nil {
		return err
	}
	// The role's volume first: an act that fails leaves the console's copy as
	// the role serves it, and the next zobik init does not carry the change.
	n := names{network, root}
	if _, err := e.EnsureVolume(ctx, n.volume(unitSecrets), n.labels()); err != nil {
		return err
	}
	if err := e.WriteVolume(ctx, n.volume(unitSecrets), image, role.Dir,
		[]File{{Path: secrets.FileStore, Mode: 0o600, Data: sealed}}); err != nil {
		return err
	}
	return writeFile(dir, fileSecretsStore, sealed)
}
