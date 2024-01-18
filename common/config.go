package common

import (
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"

	"github.com/emersion/go-appdir"
	"github.com/jwalton/go-supportscolor"
	"github.com/lmittmann/tint"
	"github.com/spf13/viper"
)

type FileConfig struct {
	Controller struct {
		HostName string `mapstructure:"host_name"`
		Addr     string
		Port     uint16
	}
}

func IsDebug() bool {
	return os.Getenv("DEBUG") != ""
}

func SetupConfigNameAndPaths(Glog *slog.Logger, appName string, configFileName string) {
	viper.SetConfigName(configFileName)

	viper.AddConfigPath(".")

	dirs := appdir.New(appName)
	viper.AddConfigPath(dirs.UserConfig())

	switch runtime.GOOS {
	case "windows":
		Glog.Info("On Windows, system-wide config is not supported.")
	case "darwin", "ios":
		viper.AddConfigPath(filepath.Join("Library", "Application Support", appName))
	default:
		fhs_config := filepath.Join("etc", appName)
		if runtime.GOOS != "linux" && runtime.GOOS != "freebsd" && runtime.GOOS != "netbsd" && runtime.GOOS != "openbsd" {
			Glog.Warn(fmt.Sprintf("Unsupported os: %s. Will probe %s.", runtime.GOOS, fhs_config))
		}
		viper.AddConfigPath(fhs_config)
	}
}

func SetupLogger() *slog.Logger {
	if !IsDebug() {
		// mute the default logger if not in debug mode
		log.SetOutput(io.Discard)
	}

	var logger *slog.Logger
	if supportscolor.Stdout().SupportsColor {
		logger = slog.New(tint.NewHandler(os.Stdout, nil))
	} else {
		logger = slog.New(slog.NewTextHandler(os.Stdout, nil))
		logger.Debug("No color support detected. Using plain text to output.")
	}

	return logger
}
