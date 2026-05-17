package router

import (
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/manager"
	"github.com/HeapOfChaos/goondvr/server"
	"github.com/HeapOfChaos/goondvr/uploader"
	"github.com/gin-gonic/gin"
)

type recordingItem struct {
	Path       string `json:"path"`
	Name       string `json:"name"`
	PreviewURL string `json:"preview_url"`
	ThumbURL   string `json:"thumb_url"`
}

type clipItem struct {
	Path       string `json:"path"`
	Name       string `json:"name"`
	PreviewURL string `json:"preview_url"`
}

// IndexData represents the data structure for the index page.
type IndexData struct {
	Config   *entity.Config
	Channels []*entity.ChannelInfo
	SMBLogs  []string
}

// Index renders the index page with channel information.
func Index(c *gin.Context) {
	c.HTML(200, "index.html", &IndexData{
		Config:   server.Config,
		Channels: server.Manager.ChannelInfo(),
		SMBLogs:  uploader.GetLogs(),
	})
}

// CreateChannelRequest represents the request body for creating a channel.
type CreateChannelRequest struct {
	Username    string `form:"username" binding:"required"`
	Site        string `form:"site"`
	Framerate   int    `form:"framerate" binding:"required"`
	Resolution  int    `form:"resolution" binding:"required"`
	Pattern     string `form:"pattern" binding:"required"`
	MaxDuration int    `form:"max_duration"`
	MaxFilesize int    `form:"max_filesize"`
}

// CreateChannel creates a new channel.
func CreateChannel(c *gin.Context) {
	var req *CreateChannelRequest
	if err := c.Bind(&req); err != nil {
		c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bind: %w", err))
		return
	}

	siteName := entity.NormalizeSite(req.Site)

	var errs []string
	for _, username := range strings.Split(req.Username, ",") {
		if err := server.Manager.CreateChannel(&entity.ChannelConfig{
			IsPaused:    false,
			Username:    username,
			Site:        siteName,
			Framerate:   req.Framerate,
			Resolution:  req.Resolution,
			Pattern:     req.Pattern,
			MaxDuration: req.MaxDuration,
			MaxFilesize: req.MaxFilesize,
			CreatedAt:   time.Now().Unix(),
		}, true); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		c.AbortWithError(http.StatusBadRequest, fmt.Errorf("%s", strings.Join(errs, "; ")))
		return
	}
	c.Redirect(http.StatusFound, "/")
}

// EditChannel updates the settings of an existing channel.
func EditChannel(c *gin.Context) {
	var req *CreateChannelRequest
	if err := c.Bind(&req); err != nil {
		c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bind: %w", err))
		return
	}

	if err := server.Manager.UpdateChannelSettings(
		c.Param("channelID"),
		req.Framerate,
		req.Resolution,
		req.Pattern,
		req.MaxDuration,
		req.MaxFilesize,
	); err != nil {
		c.AbortWithError(http.StatusBadRequest, fmt.Errorf("edit channel: %w", err))
		return
	}

	c.Redirect(http.StatusFound, "/")
}

// StopChannel stops a channel.
func StopChannel(c *gin.Context) {
	server.Manager.StopChannel(c.Param("channelID"))

	c.Redirect(http.StatusFound, "/")
}

// PauseChannel pauses a channel.
func PauseChannel(c *gin.Context) {
	server.Manager.PauseChannel(c.Param("channelID"))

	c.Redirect(http.StatusFound, "/")
}

// ResumeChannel resumes a paused channel.
func ResumeChannel(c *gin.Context) {
	server.Manager.ResumeChannel(c.Param("channelID"))

	c.Redirect(http.StatusFound, "/")
}

// ClipChannel creates a short clip from the currently recording file.
func ClipChannel(c *gin.Context) {
	seconds := 45
	if raw := strings.TrimSpace(c.PostForm("seconds")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			seconds = parsed
		}
	}
	if seconds < 10 {
		seconds = 10
	}
	if seconds > 300 {
		seconds = 300
	}

	if _, err := server.Manager.CreateClip(c.Param("channelID"), seconds); err != nil {
		c.AbortWithError(http.StatusBadRequest, fmt.Errorf("create clip: %w", err))
		return
	}

	c.Redirect(http.StatusFound, "/")
}

func ListRecordings(c *gin.Context) {
	items, err := server.Manager.ListRecordings(c.Param("channelID"))
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	out := make([]recordingItem, 0, len(items))
	for _, path := range items {
		name := filepath.Base(path)
		q := "?path=" + urlQueryEscape(path)
		out = append(out, recordingItem{
			Path:       path,
			Name:       name,
			PreviewURL: "/api/recording_file/" + c.Param("channelID") + q,
			ThumbURL:   "/api/recording_thumb/" + c.Param("channelID") + q,
		})
	}
	c.JSON(http.StatusOK, gin.H{"items": out})
}

func RecordingFile(c *gin.Context) {
	channelID := c.Param("channelID")
	path := strings.TrimSpace(c.Query("path"))
	if !isAllowedRecordingPath(channelID, path) {
		c.Status(http.StatusForbidden)
		return
	}
	f, err := os.Open(path)
	if err != nil {
		c.Status(http.StatusNotFound)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		c.Status(http.StatusNotFound)
		return
	}
	buf := make([]byte, 512)
	_, _ = f.Read(buf)
	_, _ = f.Seek(0, io.SeekStart)
	ctype := http.DetectContentType(buf)
	c.Header("Content-Type", ctype)
	http.ServeContent(c.Writer, c.Request, filepath.Base(path), info.ModTime(), f)
}

func RecordingThumb(c *gin.Context) {
	channelID := c.Param("channelID")
	path := strings.TrimSpace(c.Query("path"))
	if !isAllowedRecordingPath(channelID, path) {
		c.Status(http.StatusForbidden)
		return
	}
	thumb, err := ensureRecordingThumb(path)
	if err != nil {
		c.Status(http.StatusNotFound)
		return
	}
	c.File(thumb)
}

func ListClips(c *gin.Context) {
	items, err := server.Manager.ListClips(c.Param("channelID"))
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	out := make([]clipItem, 0, len(items))
	for _, path := range items {
		q := "?path=" + urlQueryEscape(path)
		out = append(out, clipItem{
			Path:       path,
			Name:       filepath.Base(path),
			PreviewURL: "/api/clip_file/" + c.Param("channelID") + q,
		})
	}
	c.JSON(http.StatusOK, gin.H{"items": out})
}

func ClipFile(c *gin.Context) {
	channelID := c.Param("channelID")
	path := strings.TrimSpace(c.Query("path"))
	if !isAllowedClipPath(channelID, path) {
		c.Status(http.StatusForbidden)
		return
	}
	f, err := os.Open(path)
	if err != nil {
		c.Status(http.StatusNotFound)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		c.Status(http.StatusNotFound)
		return
	}
	buf := make([]byte, 512)
	_, _ = f.Read(buf)
	_, _ = f.Seek(0, io.SeekStart)
	c.Header("Content-Type", http.DetectContentType(buf))
	http.ServeContent(c.Writer, c.Request, filepath.Base(path), info.ModTime(), f)
}

func ClipFromRecording(c *gin.Context) {
	start := 0
	if raw := strings.TrimSpace(c.PostForm("start_seconds")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			start = parsed
		}
	}
	duration := 45
	if raw := strings.TrimSpace(c.PostForm("duration_seconds")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			duration = parsed
		}
	}
	source := strings.TrimSpace(c.PostForm("source"))
	name := strings.TrimSpace(c.PostForm("name"))

	out, err := server.Manager.CreateClipFromRecording(c.Param("channelID"), source, start, duration, name)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"path": out})
}

func ClipBatch(c *gin.Context) {
	source := strings.TrimSpace(c.PostForm("source"))
	if source == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source recording is required"})
		return
	}
	starts := c.PostFormArray("start_seconds")
	durations := c.PostFormArray("duration_seconds")
	names := c.PostFormArray("name")
	if len(starts) == 0 || len(durations) == 0 || len(starts) != len(durations) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid clip batch payload"})
		return
	}

	created := make([]string, 0, len(starts))
	failures := make([]string, 0)
	for i := range starts {
		start, err := strconv.Atoi(strings.TrimSpace(starts[i]))
		if err != nil {
			failures = append(failures, fmt.Sprintf("clip %d: invalid start", i+1))
			continue
		}
		dur, err := strconv.Atoi(strings.TrimSpace(durations[i]))
		if err != nil {
			failures = append(failures, fmt.Sprintf("clip %d: invalid duration", i+1))
			continue
		}
		name := ""
		if i < len(names) {
			name = names[i]
		}
		out, err := server.Manager.CreateClipFromRecording(c.Param("channelID"), source, start, dur, name)
		if err != nil {
			failures = append(failures, fmt.Sprintf("clip %d: %s", i+1, err.Error()))
			continue
		}
		created = append(created, out)
	}
	c.JSON(http.StatusOK, gin.H{"created": created, "failures": failures})
}

func CombineClips(c *gin.Context) {
	clips := c.PostFormArray("clip")
	name := strings.TrimSpace(c.PostForm("name"))
	out, err := server.Manager.CombineClips(c.Param("channelID"), clips, name)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"path": out})
}

func isAllowedRecordingPath(channelID, path string) bool {
	if path == "" {
		return false
	}
	items, err := server.Manager.ListRecordings(channelID)
	if err != nil {
		return false
	}
	clean := filepath.Clean(path)
	for _, item := range items {
		if filepath.Clean(item) == clean {
			return true
		}
	}
	return false
}

func isAllowedClipPath(channelID, path string) bool {
	if path == "" {
		return false
	}
	items, err := server.Manager.ListClips(channelID)
	if err != nil {
		return false
	}
	clean := filepath.Clean(path)
	for _, item := range items {
		if filepath.Clean(item) == clean {
			return true
		}
	}
	return false
}

func ensureRecordingThumb(path string) (string, error) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return "", err
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(path))
	thumbDir := filepath.Join(filepath.Dir(path), ".thumbs")
	if err := os.MkdirAll(thumbDir, 0755); err != nil {
		return "", err
	}
	thumb := filepath.Join(thumbDir, fmt.Sprintf("%x.jpg", h.Sum64()))
	srcInfo, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if info, err := os.Stat(thumb); err == nil && info.ModTime().After(srcInfo.ModTime()) {
		return thumb, nil
	}
	tmp := thumb + ".tmp.jpg"
	_ = os.Remove(tmp)
	args := []string{"-nostdin", "-y", "-ss", "00:00:05", "-i", path, "-frames:v", "1", "-q:v", "3", tmp}
	cmd := exec.Command("ffmpeg", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("thumb generate failed: %s", strings.TrimSpace(string(out)))
	}
	if err := os.Rename(tmp, thumb); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	return thumb, nil
}

func urlQueryEscape(s string) string {
	r := strings.NewReplacer("%", "%25", " ", "%20", "#", "%23", "?", "%3F", "&", "%26", "+", "%2B")
	return r.Replace(s)
}

// ThumbProxy proxies the channel's summary card image from the CDN through the server.
// This avoids hotlink-protection issues when the browser requests the image directly.
func ThumbProxy(c *gin.Context) {
	imgURL := server.Manager.GetChannelThumb(c.Param("channelID"))
	if imgURL == "" {
		c.Status(http.StatusNotFound)
		return
	}

	req := internal.NewMediaReq()
	imgBytes, err := req.GetBytes(c.Request.Context(), imgURL)
	if err != nil {
		c.Status(http.StatusBadGateway)
		return
	}

	contentType := http.DetectContentType(imgBytes)
	c.Data(http.StatusOK, contentType, imgBytes)
}

// LiveThumbProxy proxies the channel's live-updating thumbnail from the CDN.
// For Stripchat this uses img.doppiocdn.net; for Chaturbate it falls back to
// the summary card image (the JS handles Chaturbate live thumbs directly).
func LiveThumbProxy(c *gin.Context) {
	imgURL := server.Manager.GetChannelLiveThumb(c.Param("channelID"))
	if imgURL == "" {
		c.Status(http.StatusNotFound)
		return
	}

	req := internal.NewMediaReqWithReferer("https://stripchat.com/")
	imgBytes, err := req.GetBytes(c.Request.Context(), imgURL)
	if err != nil {
		c.Status(http.StatusBadGateway)
		return
	}

	contentType := http.DetectContentType(imgBytes)
	c.Data(http.StatusOK, contentType, imgBytes)
}

// Updates handles the SSE connection for updates.
func Updates(c *gin.Context) {
	server.Manager.Subscriber(c.Writer, c.Request)
}

// Stats returns system stats as JSON for the header stats bar.
func Stats(c *gin.Context) {
	c.JSON(http.StatusOK, server.Manager.GetStats())
}

// UpdateConfigRequest represents the request body for updating configuration.
type UpdateConfigRequest struct {
	Cookies                string `form:"cookies"`
	UserAgent              string `form:"user_agent"`
	CompletedDir           string `form:"completed_dir"`
	FinalizeMode           string `form:"finalize_mode"`
	FFmpegEncoder          string `form:"ffmpeg_encoder"`
	FFmpegContainer        string `form:"ffmpeg_container"`
	FFmpegQuality          int    `form:"ffmpeg_quality"`
	FFmpegPreset           string `form:"ffmpeg_preset"`
	NtfyURL                string `form:"ntfy_url"`
	NtfyTopic              string `form:"ntfy_topic"`
	NtfyToken              string `form:"ntfy_token"`
	DiscordWebhookURL      string `form:"discord_webhook_url"`
	DiscordBotToken        string `form:"discord_bot_token"`
	DiscordStatusChannelID string `form:"discord_status_channel_id"`
	DiskWarningPercent     int    `form:"disk_warning_percent"`
	DiskCriticalPercent    int    `form:"disk_critical_percent"`
	CFChannelThreshold     int    `form:"cf_channel_threshold"`
	CFGlobalThreshold      int    `form:"cf_global_threshold"`
	NotifyCooldownHours    int    `form:"notify_cooldown_hours"`
	NotifyStreamOnline     bool   `form:"notify_stream_online"`
	N8NWebhookURL          string `form:"n8n_webhook_url"`
	N8NToken               string `form:"n8n_token"`
	NightOpsEnabled        bool   `form:"night_ops_enabled"`
	NightOpsStartHour      int    `form:"night_ops_start_hour"`
	NightOpsEndHour        int    `form:"night_ops_end_hour"`
	NightOpsRetrySeconds   int    `form:"night_ops_retry_seconds"`
	SMBUploadEnabled       bool   `form:"smb_upload_enabled"`
	SMBUploadHost          string `form:"smb_upload_host"`
	SMBUploadShare         string `form:"smb_upload_share"`
	SMBUploadUsername      string `form:"smb_upload_username"`
	SMBUploadPassword      string `form:"smb_upload_password"`
	SMBUploadDomain        string `form:"smb_upload_domain"`
	SMBUploadBaseDir       string `form:"smb_upload_base_dir"`
}

// UpdateConfig updates the server configuration.
func UpdateConfig(c *gin.Context) {
	var req *UpdateConfigRequest
	if err := c.Bind(&req); err != nil {
		c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bind: %w", err))
		return
	}

	server.Config.Cookies = req.Cookies
	server.Config.UserAgent = req.UserAgent
	server.Config.CompletedDir = req.CompletedDir
	server.Config.FinalizeMode = entity.NormalizeFinalizeMode(req.FinalizeMode)
	server.Config.FFmpegEncoder = req.FFmpegEncoder
	if req.FFmpegContainer == "mkv" {
		server.Config.FFmpegContainer = "mkv"
	} else {
		server.Config.FFmpegContainer = "mp4"
	}
	if req.FFmpegQuality > 0 {
		server.Config.FFmpegQuality = req.FFmpegQuality
	} else if server.Config.FFmpegQuality <= 0 {
		server.Config.FFmpegQuality = 23
	}
	server.Config.FFmpegPreset = req.FFmpegPreset
	if server.Config.FFmpegEncoder == "" {
		server.Config.FFmpegEncoder = "libx264"
	}
	if server.Config.FFmpegPreset == "" {
		server.Config.FFmpegPreset = "medium"
	}
	server.Config.NtfyURL = req.NtfyURL
	server.Config.NtfyTopic = req.NtfyTopic
	server.Config.NtfyToken = req.NtfyToken
	server.Config.DiscordWebhookURL = req.DiscordWebhookURL
	server.Config.DiscordBotToken = req.DiscordBotToken
	server.Config.DiscordStatusChannelID = req.DiscordStatusChannelID
	if req.DiscordStatusChannelID == "" {
		server.Config.DiscordStatusMessageID = ""
	}
	server.Config.DiskWarningPercent = req.DiskWarningPercent
	server.Config.DiskCriticalPercent = req.DiskCriticalPercent
	server.Config.CFChannelThreshold = req.CFChannelThreshold
	server.Config.CFGlobalThreshold = req.CFGlobalThreshold
	server.Config.NotifyCooldownHours = req.NotifyCooldownHours
	server.Config.NotifyStreamOnline = req.NotifyStreamOnline
	server.Config.N8NWebhookURL = strings.TrimSpace(req.N8NWebhookURL)
	server.Config.N8NToken = req.N8NToken
	server.Config.NightOpsEnabled = req.NightOpsEnabled
	server.Config.NightOpsStartHour = req.NightOpsStartHour
	server.Config.NightOpsEndHour = req.NightOpsEndHour
	server.Config.NightOpsRetrySeconds = req.NightOpsRetrySeconds
	if server.Config.NightOpsStartHour < 0 || server.Config.NightOpsStartHour > 23 {
		server.Config.NightOpsStartHour = 0
	}
	if server.Config.NightOpsEndHour < 0 || server.Config.NightOpsEndHour > 23 {
		server.Config.NightOpsEndHour = 6
	}
	if server.Config.NightOpsRetrySeconds <= 0 {
		server.Config.NightOpsRetrySeconds = 5
	}
	server.Config.SMBUploadEnabled = req.SMBUploadEnabled
	server.Config.SMBUploadHost = strings.TrimSpace(req.SMBUploadHost)
	server.Config.SMBUploadShare = strings.TrimSpace(req.SMBUploadShare)
	server.Config.SMBUploadUsername = strings.TrimSpace(req.SMBUploadUsername)
	server.Config.SMBUploadPassword = req.SMBUploadPassword
	server.Config.SMBUploadDomain = strings.TrimSpace(req.SMBUploadDomain)
	server.Config.SMBUploadBaseDir = strings.TrimSpace(req.SMBUploadBaseDir)

	if err := manager.SaveSettings(); err != nil {
		c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("save settings: %w", err))
		return
	}
	c.Redirect(http.StatusFound, "/")
}

type SMBTestRequest struct {
	Host     string `form:"host" json:"host"`
	Share    string `form:"share" json:"share"`
	Username string `form:"username" json:"username"`
	Password string `form:"password" json:"password"`
	Domain   string `form:"domain" json:"domain"`
	BaseDir  string `form:"base_dir" json:"base_dir"`
}

func TestSMB(c *gin.Context) {
	var req SMBTestRequest
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "message": "Ugyldig SMB test request"})
		return
	}

	target, err := uploader.TestConnection(uploader.SMBTestConfig{
		Host:     strings.TrimSpace(req.Host),
		Share:    strings.TrimSpace(req.Share),
		Username: strings.TrimSpace(req.Username),
		Password: req.Password,
		Domain:   strings.TrimSpace(req.Domain),
		BaseDir:  strings.TrimSpace(req.BaseDir),
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "message": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"ok": true, "message": fmt.Sprintf("SMB test OK (testfil oprettet/fjernet: %s)", target)})
}
