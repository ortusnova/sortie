//go:build !linux

package hostview

// ReadCapacity returns nil: Sortie reads a host's capacity only on
// Linux.
func ReadCapacity(string) *Size { return nil }
