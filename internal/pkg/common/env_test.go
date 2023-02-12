package common

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetEvnWithDefaultVal(t *testing.T) {
	assert.Equal(t, "default", GetEvnWithDefaultVal("NOT_EXIST", "default"))
	os.Setenv("EXIST", "exist")
	defer os.Unsetenv("EXIST")
	assert.Equal(t, "exist", GetEvnWithDefaultVal("EXIST", "default"))
}
