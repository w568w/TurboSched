package common

import (
	"fmt"
	"github.com/dixonwille/wlog/v3"
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
		fhsConfig := filepath.Join("etc", appName)
		if runtime.GOOS != "linux" && runtime.GOOS != "freebsd" && runtime.GOOS != "netbsd" && runtime.GOOS != "openbsd" {
			Glog.Warn(fmt.Sprintf("Unsupported os: %s. Will probe %s.", runtime.GOOS, fhsConfig))
		}
		viper.AddConfigPath(fhsConfig)
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

// SetupCLILogger sets up a logger for command line interface (CLI) applications, e.g. turbo.
func SetupCLILogger() wlog.UI {
	if !IsDebug() {
		// mute the default logger if not in debug mode
		log.SetOutput(io.Discard)
	}

	var ui wlog.UI = wlog.New(os.Stdin, os.Stderr, os.Stderr)
	if runtime.GOOS == "windows" {
		// Windows console does not support some unicode characters
		ui = wlog.AddPrefix("?", "x", " ", "", "", "~", "$", "!", ui)
	} else {
		ui = wlog.AddPrefix("?", wlog.Cross, " ", "", "", "~", wlog.Check, "!", ui)
	}
	if supportscolor.Stdout().SupportsColor {
		ui = wlog.AddColor(wlog.White, wlog.Red, wlog.White, wlog.White, wlog.White, wlog.Magenta, wlog.Blue, wlog.Green, wlog.Yellow, ui)
	} else {
		ui.Info("No color support detected. Using plain text to output.")
	}

	ui = wlog.AddConcurrent(ui)
	return ui
}
