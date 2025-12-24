package env

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"
)

var (
	loadOnce   sync.Once
	loadedPath string
	loadErr    error
)

// Ensure loads the first .env file found from the current working directory up
// to the filesystem root. Subsequent calls are no-ops.
func Ensure() error {
	// Keep unit tests hermetic: avoid picking up developer-local `.env` by default.
	// Opt-in with GOTEST_LOAD_DOTENV=1 when running `go test`.
	if runningUnderGoTest() && os.Getenv("GOTEST_LOAD_DOTENV") != "1" {
		return nil
	}
	loadOnce.Do(func() {
		path, err := findDotEnv()
		if err != nil {
			loadErr = err
			log.Debug().Err(err).Msg("taskagent: search .env failed")
			return
		}
		if path == "" {
			return
		}
		if err := godotenv.Load(path); err != nil {
			loadErr = err
			log.Warn().Err(err).Str("dotenv", path).Msg("taskagent: load .env failed")
			return
		}
		loadedPath = path
		log.Debug().Str("dotenv", path).Msg("taskagent: loaded .env")
	})
	return loadErr
}

// LoadedPath returns the resolved .env path if one was loaded, otherwise "".
func LoadedPath() string {
	return loadedPath
}

func runningUnderGoTest() bool {
	if strings.HasSuffix(os.Args[0], ".test") {
		return true
	}
	for _, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "-test.") {
			return true
		}
	}
	return false
}

func findDotEnv() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		candidate := filepath.Join(wd, ".env")
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(wd)
		if parent == wd {
			return "", nil
		}
		wd = parent
	}
}
