package config

import (
	"fmt"
	"strings"

	"github.com/spotinst/wave-operator/internal/version"
)

// userAgent represents a User-Agent header.
type userAgent struct {
	Product string
	Version string
	Comment []string
}

func (ua userAgent) toString() string {
	s := fmt.Sprintf("%s/%s", ua.Product, ua.Version)
	if len(ua.Comment) > 0 {
		s += fmt.Sprintf(" (%s)", strings.Join(ua.Comment, "; "))
	}
	return s
}

func GetUserAgent() string {
	return userAgent{
		Product: productName,
		Version: version.BuildVersion,
		Comment: []string{version.BuildDate},
	}.toString()
}
