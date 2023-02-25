package common

import "os"

func GetEvnWithDefaultVal(key string, defaultVal string) string {
	val := os.Getenv(key)
	if val != "" {
		return val
	} else {
		return defaultVal
	}
}

func IsPrdEnv() bool {
	return os.Getenv("ENV") == "prd"
}

func IsDevEnv() bool {
	return os.Getenv("ENV") == "dev" || os.Getenv("ENV") == ""
}
