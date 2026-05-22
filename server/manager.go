package server

import (
	"net/http"

	"github.com/HeapOfChaos/goondvr/entity"
)

var Manager IManager

type IManager interface {
	CreateChannel(conf *entity.ChannelConfig, shouldSave bool) error
	StopChannel(channelID string) error
	PauseChannel(channelID string) error
	ResumeChannel(channelID string) error
	SkipCurrentStream(channelID string) error
	CreateClip(channelID string, seconds int) (string, error)
	ListRecordings(channelID string) ([]string, error)
	ManualUploadRecording(channelID, filePath string) error
	ManualUploadAll() error
	ListClips(channelID string) ([]string, error)
	CreateClipFromRecording(channelID, source string, startSeconds, durationSeconds int, clipName string) (string, error)
	CombineClips(channelID string, clips []string, outputName string) (string, error)
	UpdateChannelSettings(channelID string, framerate, resolution int, pattern string, maxDuration, maxFilesize int) error
	ChannelInfo() []*entity.ChannelInfo
	Publish(name string, ch *entity.ChannelInfo)
	Subscriber(w http.ResponseWriter, r *http.Request)
	LoadConfig() error
	SaveConfig() error
	Shutdown()
	GetChannelThumb(channelID string) string
	GetChannelLiveThumb(channelID string) string
	ReportCFBlock(username string)
	ResetCFBlock(username string)
	GetStats() StatsResponse
	ExportSettingsJSON() ([]byte, error)
	ImportSettingsJSON(data []byte) error
	ExportChannelsJSON() ([]byte, error)
	ImportChannelsJSON(data []byte) error
	ValidateSettingsJSON(data []byte) error
	ValidateChannelsJSON(data []byte) error
	PreviewChannelsImportJSON(data []byte) (ChannelsImportPreview, error)
}

// ChannelsImportPreview summarizes the diff between runtime channels and an import file.
type ChannelsImportPreview struct {
	CurrentCount  int      `json:"current_count"`
	IncomingCount int      `json:"incoming_count"`
	Added         []string `json:"added"`
	Removed       []string `json:"removed"`
	Unchanged     []string `json:"unchanged"`
}

// StatsResponse holds system stats returned by the /api/stats endpoint.
type StatsResponse struct {
	DiskPath       string  `json:"disk_path"`
	DiskUsedBytes  uint64  `json:"disk_used_bytes"`
	DiskTotalBytes uint64  `json:"disk_total_bytes"`
	DiskPercent    float64 `json:"disk_percent"`
	UptimeSeconds  int64   `json:"uptime_seconds"`
	RecordingCount int     `json:"recording_count"`
}
