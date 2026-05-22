package config

import (
	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/urfave/cli/v2"
)

// New initializes a new Config struct with values from the CLI context.
func New(c *cli.Context) (*entity.Config, error) {
	maxDuration := c.Int("max-duration")
	if splitMinutes := c.Int("split-minutes"); splitMinutes > 0 {
		maxDuration = splitMinutes
	}

	return &entity.Config{
		Version:              c.App.Version,
		Username:             c.String("username"),
		Site:                 entity.NormalizeSite(c.String("site")),
		AdminUsername:        c.String("admin-username"),
		AdminPassword:        c.String("admin-password"),
		Framerate:            c.Int("framerate"),
		Resolution:           c.Int("resolution"),
		Pattern:              c.String("pattern"),
		MaxDuration:          maxDuration,
		MaxFilesize:          c.Int("max-filesize"),
		Port:                 c.String("port"),
		Interval:             c.Int("interval"),
		Cookies:              c.String("cookies"),
		UserAgent:            c.String("user-agent"),
		Domain:               c.String("domain"),
		CompletedDir:         c.String("completed-dir"),
		FinalizeMode:         entity.NormalizeFinalizeMode(c.String("finalize-mode")),
		FFmpegEncoder:        c.String("ffmpeg-encoder"),
		FFmpegContainer:      c.String("ffmpeg-container"),
		FFmpegQuality:        c.Int("ffmpeg-quality"),
		FFmpegPreset:         c.String("ffmpeg-preset"),
		Debug:                c.Bool("debug"),
		StripchatPDKey:       c.String("stripchat-pdkey"),
		NightOpsStartHour:    0,
		NightOpsEndHour:      6,
		NightOpsRetrySeconds: 5,
		RecoveryEnabled:      true,
		RecoveryUploadWindowHrs: 7 * 24,
		RecoveryMaxFFprobe:      24,
	}, nil
}
