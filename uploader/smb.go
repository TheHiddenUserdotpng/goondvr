package uploader

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/HeapOfChaos/goondvr/server"
	"github.com/hirochachacha/go-smb2"
)

const (
	maxSMBRetries      = 6
	logBufferSize      = 250
	queueBufferSize    = 256
	completedQueueFile = "./conf/smb_completed_queue.json"
)

var errIntegrityCheck = errors.New("integrity check failed")

type uploadTask struct {
	ChannelID     string `json:"channel_id"`
	LocalPath     string `json:"local_path"`
	RemoteRelPath string `json:"remote_rel_path"`
	Attempt       int    `json:"attempt"`
	Persistent    bool   `json:"persistent,omitempty"`
}

type ChannelStatus struct {
	Level     string
	Text      string
	UpdatedAt time.Time
}

type SMBTestConfig struct {
	Host     string
	Share    string
	Username string
	Password string
	Domain   string
	BaseDir  string
}

var (
	startWorkersOnce sync.Once
	queueCh          chan uploadTask
	queueMu          sync.Mutex
	completedQueue   []uploadTask

	statusMu      sync.RWMutex
	channelStatus = map[string]ChannelStatus{}
	recentSMBLogs []string

	manualQueueMu sync.RWMutex
	manualQueue   []ManualQueueItem

	// StatusUpdateHook is called when a channel SMB status changes.
	StatusUpdateHook func(channelID string)
	// LogUpdateHook is called when the SMB log buffer changes.
	LogUpdateHook func()
)

// ManualQueueItem tracks the state of a manual SMB upload.
type ManualQueueItem struct {
	ID           string    `json:"id"`
	ChannelID    string    `json:"channel_id"`
	FileName     string    `json:"file_name"`
	LocalPath    string    `json:"local_path"`
	Status       string    `json:"status"` // "uploading", "done", "failed"
	ErrorMessage string    `json:"error,omitempty"`
	StartedAt    time.Time `json:"started_at"`
	CompletedAt  time.Time `json:"completed_at,omitempty"`
}

// UploadIfEnabled uploads localPath to TrueNAS SMB if upload is configured.
// remoteRelPath is appended under SMBUploadBaseDir.
func UploadIfEnabled(channelID, localPath, remoteRelPath string) {
	cfg := server.Config
	if cfg == nil || !cfg.SMBUploadEnabled {
		return
	}
	if cfg.SMBUploadHost == "" || cfg.SMBUploadShare == "" || cfg.SMBUploadUsername == "" {
		setChannelStatus(channelID, "bad", "SMB misconfigured (host/share/username mangler)")
		appendLog(channelID, "SMB upload skipped: mangler host/share/username")
		return
	}

	startWorkers()
	setChannelStatus(channelID, "warn", "SMB upload queued")
	task := uploadTask{ChannelID: channelID, LocalPath: localPath, RemoteRelPath: remoteRelPath, Attempt: 1}
	if isCompletedUpload(remoteRelPath) {
		task.Persistent = true
		if added := queueCompletedTask(task); added {
			appendLog(channelID, fmt.Sprintf("completed upload queued: %s", localPath))
		}
	}
	queueCh <- task
}

func startWorkers() {
	startWorkersOnce.Do(func() {
		queueCh = make(chan uploadTask, queueBufferSize)
		bootstrapCompletedQueue()
		go workerLoop()
	})
}

func bootstrapCompletedQueue() {
	queueMu.Lock()
	defer queueMu.Unlock()

	b, err := os.ReadFile(completedQueueFile)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		appendLog("", fmt.Sprintf("cannot read completed queue file: %v", err))
		return
	}

	var items []uploadTask
	if err := json.Unmarshal(b, &items); err != nil {
		appendLog("", fmt.Sprintf("cannot parse completed queue file: %v", err))
		return
	}

	for i := range items {
		items[i].Persistent = true
		if items[i].Attempt <= 0 {
			items[i].Attempt = 1
		}
	}
	completedQueue = items

	for _, task := range completedQueue {
		queueCh <- task
	}
	if len(completedQueue) > 0 {
		appendLog("", fmt.Sprintf("restored %d completed uploads from queue", len(completedQueue)))
	}
}

func workerLoop() {
	for task := range queueCh {
		setChannelStatus(task.ChannelID, "warn", fmt.Sprintf("SMB uploading (forsøg %d/%d)", task.Attempt, maxSMBRetries))
		err := upload(task.LocalPath, task.RemoteRelPath)
		if err == nil {
			setChannelStatus(task.ChannelID, "good", "SMB upload OK")
			if task.Persistent {
				dequeueCompletedTask(task)
			}
			appendLog(task.ChannelID, fmt.Sprintf("upload OK: %s -> %s", task.LocalPath, task.RemoteRelPath))
			continue
		}

		if task.Persistent {
			if !isRetryableNetworkError(err) {
				if errors.Is(err, errIntegrityCheck) {
					setChannelStatus(task.ChannelID, "bad", "Integrity check fejlede, lokalfil beholdt")
					appendLog(task.ChannelID, fmt.Sprintf("completed upload stoppet (integrity): %v", err))
				} else {
					setChannelStatus(task.ChannelID, "bad", "Completed upload fejlede")
					appendLog(task.ChannelID, fmt.Sprintf("completed upload FAILED (non-network): %v", err))
				}
				dequeueCompletedTask(task)
				continue
			}

			delay := backoffForAttempt(task.Attempt)
			task.Attempt++
			queueCompletedTask(task)
			pending := completedQueueLen()
			setChannelStatus(task.ChannelID, "warn", fmt.Sprintf("SMB utilgængelig, completed-kø venter (%d)", pending))
			appendLog(task.ChannelID, fmt.Sprintf("completed upload retry om %s (%d i kø): %v", delay.Round(time.Second), pending, err))
			go func(t uploadTask, d time.Duration) {
				timer := time.NewTimer(d)
				defer timer.Stop()
				<-timer.C
				queueCh <- t
			}(task, delay)
			continue
		}

		if task.Attempt < maxSMBRetries && isRetryableNetworkError(err) {
			delay := backoffForAttempt(task.Attempt)
			setChannelStatus(task.ChannelID, "warn", fmt.Sprintf("SMB netfejl, retry om %s", delay.Round(time.Second)))
			appendLog(task.ChannelID, fmt.Sprintf("upload netfejl (forsøg %d/%d): %v", task.Attempt, maxSMBRetries, err))
			next := task
			next.Attempt++
			go func(t uploadTask, d time.Duration) {
				timer := time.NewTimer(d)
				defer timer.Stop()
				<-timer.C
				queueCh <- t
			}(next, delay)
			continue
		}

		setChannelStatus(task.ChannelID, "bad", "SMB upload fejlede")
		appendLog(task.ChannelID, fmt.Sprintf("upload FAILED (forsøg %d/%d): %v", task.Attempt, maxSMBRetries, err))
	}
}

func isCompletedUpload(remoteRelPath string) bool {
	p := strings.TrimLeft(strings.ReplaceAll(remoteRelPath, "\\", "/"), "/")
	return strings.HasPrefix(p, "completed/")
}

func queueCompletedTask(task uploadTask) bool {
	if !task.Persistent {
		return false
	}
	queueMu.Lock()
	defer queueMu.Unlock()

	for i := range completedQueue {
		if completedQueue[i].LocalPath == task.LocalPath && completedQueue[i].RemoteRelPath == task.RemoteRelPath {
			if task.Attempt > completedQueue[i].Attempt {
				completedQueue[i].Attempt = task.Attempt
				_ = saveCompletedQueueLocked()
			}
			return false
		}
	}
	completedQueue = append(completedQueue, task)
	_ = saveCompletedQueueLocked()
	return true
}

func dequeueCompletedTask(task uploadTask) {
	queueMu.Lock()
	defer queueMu.Unlock()

	out := completedQueue[:0]
	for _, it := range completedQueue {
		if it.LocalPath == task.LocalPath && it.RemoteRelPath == task.RemoteRelPath {
			continue
		}
		out = append(out, it)
	}
	completedQueue = out
	_ = saveCompletedQueueLocked()
}

func completedQueueLen() int {
	queueMu.Lock()
	defer queueMu.Unlock()
	return len(completedQueue)
}

func saveCompletedQueueLocked() error {
	if len(completedQueue) == 0 {
		if err := os.Remove(completedQueueFile); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	if err := os.MkdirAll("./conf", 0700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(completedQueue, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(completedQueueFile, b, 0600)
}

func upload(localPath, remoteRelPath string) error {
	cfg := server.Config
	if cfg == nil {
		return fmt.Errorf("config unavailable")
	}

	if err := ensureLocalFileHealthy(localPath); err != nil {
		return err
	}

	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open local file: %w", err)
	}
	defer f.Close()

	conn, share, err := openShare(SMBTestConfig{
		Host:     cfg.SMBUploadHost,
		Share:    cfg.SMBUploadShare,
		Username: cfg.SMBUploadUsername,
		Password: cfg.SMBUploadPassword,
		Domain:   cfg.SMBUploadDomain,
		BaseDir:  cfg.SMBUploadBaseDir,
	})
	if err != nil {
		return err
	}
	defer conn.Logoff()
	defer share.Umount()

	remoteRelPath = strings.TrimLeft(strings.ReplaceAll(remoteRelPath, "\\", "/"), "/")
	baseDir := strings.Trim(strings.ReplaceAll(cfg.SMBUploadBaseDir, "\\", "/"), "/")
	remotePath := remoteRelPath
	if baseDir != "" {
		remotePath = path.Join(baseDir, remoteRelPath)
	}

	remoteDir := path.Dir(remotePath)
	if remoteDir != "." && remoteDir != "" {
		if err := share.MkdirAll(remoteDir, 0755); err != nil {
			return fmt.Errorf("mkdir remote dir %s: %w", remoteDir, err)
		}
	}

	out, err := share.OpenFile(remotePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open remote file %s: %w", remotePath, err)
	}

	localHash := sha256.New()
	localSize, err := io.Copy(out, io.TeeReader(f, localHash))
	if err != nil {
		_ = out.Close()
		return fmt.Errorf("copy to remote file: %w", err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close remote file %s: %w", remotePath, err)
	}

	if err := verifyRemoteIntegrity(share, remotePath, localSize, localHash.Sum(nil)); err != nil {
		return err
	}

	if isLikelyVideoFile(localPath) {
		if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove local file after verified upload: %w", err)
		}
	}
	return nil
}

func ensureLocalFileHealthy(localPath string) error {
	info1, err := os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("stat local file: %w", err)
	}
	if info1.Size() <= 0 {
		return fmt.Errorf("%w: local file is empty", errIntegrityCheck)
	}

	time.Sleep(1500 * time.Millisecond)
	info2, err := os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("restat local file: %w", err)
	}
	if info2.Size() != info1.Size() {
		return fmt.Errorf("local file still growing (%d -> %d bytes)", info1.Size(), info2.Size())
	}

	if !isLikelyVideoFile(localPath) {
		return nil
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		// Best-effort media validation; continue if ffprobe is unavailable.
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-show_entries", "format=duration,size", "-of", "default=noprint_wrappers=1:nokey=1", localPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg == "" {
			msg = err.Error()
		}
		return fmt.Errorf("%w: ffprobe check failed for %s: %s", errIntegrityCheck, localPath, msg)
	}
	return nil
}

func verifyRemoteIntegrity(share *smb2.Share, remotePath string, localSize int64, localSum []byte) error {
	if localSize <= 0 {
		return fmt.Errorf("%w: local size is 0", errIntegrityCheck)
	}

	info, err := share.Stat(remotePath)
	if err != nil {
		return fmt.Errorf("stat remote file %s: %w", remotePath, err)
	}
	if info.Size() != localSize {
		return fmt.Errorf("%w: size mismatch local=%d remote=%d", errIntegrityCheck, localSize, info.Size())
	}

	in, err := share.Open(remotePath)
	if err != nil {
		return fmt.Errorf("open remote file for verify %s: %w", remotePath, err)
	}
	defer in.Close()

	remoteHash := sha256.New()
	if _, err := io.Copy(remoteHash, in); err != nil {
		return fmt.Errorf("hash remote file %s: %w", remotePath, err)
	}
	if !equalBytes(localSum, remoteHash.Sum(nil)) {
		return fmt.Errorf("%w: sha256 mismatch for %s", errIntegrityCheck, remotePath)
	}
	return nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func isLikelyVideoFile(filePath string) bool {
	ext := strings.ToLower(path.Ext(filePath))
	return ext == ".mp4" || ext == ".mkv" || ext == ".ts" || ext == ".mov"
}

// ManualUpload triggers an immediate SMB upload of localPath with remoteRelPath.
// It runs in a background goroutine, tracks progress in the manual queue, and
// deletes the local file on success (same as the automatic uploader).
// Returns an error immediately if SMB is not configured or the file is missing.
func ManualUpload(channelID, localPath, remoteRelPath string) error {
	cfg := server.Config
	if cfg == nil || !cfg.SMBUploadEnabled {
		return fmt.Errorf("SMB upload er ikke aktiveret")
	}
	if cfg.SMBUploadHost == "" || cfg.SMBUploadShare == "" || cfg.SMBUploadUsername == "" {
		return fmt.Errorf("SMB er ikke konfigureret (host/share/username mangler)")
	}
	if _, err := os.Stat(localPath); err != nil {
		return fmt.Errorf("fil ikke fundet: %w", err)
	}

	item := ManualQueueItem{
		ID:        fmt.Sprintf("%d", time.Now().UnixNano()),
		ChannelID: channelID,
		FileName:  filepath.Base(localPath),
		LocalPath: localPath,
		Status:    "uploading",
		StartedAt: time.Now(),
	}
	addManualQueueItem(item)

	appendLog(channelID, fmt.Sprintf("manual upload startet: %s", localPath))
	setChannelStatus(channelID, "warn", "Manuel SMB upload i gang...")

	go func() {
		err := upload(localPath, remoteRelPath)
		if err != nil {
			setChannelStatus(channelID, "bad", "Manuel SMB upload fejlede")
			appendLog(channelID, fmt.Sprintf("manual upload FAILED: %s: %v", localPath, err))
			updateManualQueueItem(item.ID, "failed", err.Error())
			return
		}
		setChannelStatus(channelID, "good", "Manuel SMB upload OK")
		appendLog(channelID, fmt.Sprintf("manual upload OK: %s -> %s", localPath, remoteRelPath))
		updateManualQueueItem(item.ID, "done", "")
		if StatusUpdateHook != nil {
			StatusUpdateHook(channelID)
		}
		if LogUpdateHook != nil {
			LogUpdateHook()
		}
	}()
	return nil
}

func addManualQueueItem(item ManualQueueItem) {
	manualQueueMu.Lock()
	defer manualQueueMu.Unlock()
	manualQueue = append(manualQueue, item)
	// keep last 100 entries
	if len(manualQueue) > 100 {
		manualQueue = manualQueue[len(manualQueue)-100:]
	}
}

func updateManualQueueItem(id, status, errMsg string) {
	manualQueueMu.Lock()
	defer manualQueueMu.Unlock()
	for i := range manualQueue {
		if manualQueue[i].ID == id {
			manualQueue[i].Status = status
			manualQueue[i].ErrorMessage = errMsg
			manualQueue[i].CompletedAt = time.Now()
			return
		}
	}
}

// GetManualQueue returns a snapshot of the manual upload queue (most recent first).
// Items with status "done" are excluded after 15 seconds.
func GetManualQueue() []ManualQueueItem {
	manualQueueMu.RLock()
	defer manualQueueMu.RUnlock()
	const keepDoneFor = 15 * time.Second
	out := make([]ManualQueueItem, 0, len(manualQueue))
	for i := len(manualQueue) - 1; i >= 0; i-- {
		item := manualQueue[i]
		if item.Status == "done" && !item.CompletedAt.IsZero() && time.Since(item.CompletedAt) > keepDoneFor {
			continue
		}
		out = append(out, item)
	}
	return out
}

func TestConnection(testCfg SMBTestConfig) (string, error) {
	if strings.TrimSpace(testCfg.Host) == "" || strings.TrimSpace(testCfg.Share) == "" || strings.TrimSpace(testCfg.Username) == "" {
		return "", fmt.Errorf("host/share/username er påkrævet")
	}

	conn, share, err := openShare(testCfg)
	if err != nil {
		return "", err
	}
	defer conn.Logoff()
	defer share.Umount()

	baseDir := strings.Trim(strings.ReplaceAll(testCfg.BaseDir, "\\", "/"), "/")
	targetDir := "."
	if baseDir != "" {
		targetDir = path.Join(baseDir, "_goondvr_test")
	}
	if targetDir != "." {
		if err := share.MkdirAll(targetDir, 0755); err != nil {
			return "", fmt.Errorf("mkdir test dir %s: %w", targetDir, err)
		}
	}

	name := fmt.Sprintf("smb_test_%d.txt", time.Now().UnixNano())
	targetPath := name
	if targetDir != "." {
		targetPath = path.Join(targetDir, name)
	}

	out, err := share.OpenFile(targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return "", fmt.Errorf("open test file %s: %w", targetPath, err)
	}
	if _, err := io.WriteString(out, "goondvr smb connectivity test\n"); err != nil {
		_ = out.Close()
		return "", fmt.Errorf("write test file: %w", err)
	}
	if err := out.Close(); err != nil {
		return "", fmt.Errorf("close test file: %w", err)
	}
	if err := share.Remove(targetPath); err != nil {
		return "", fmt.Errorf("remove test file %s: %w", targetPath, err)
	}

	return targetPath, nil
}

func openShare(testCfg SMBTestConfig) (*smb2.Session, *smb2.Share, error) {
	host := strings.TrimSpace(testCfg.Host)
	if !strings.Contains(host, ":") {
		host += ":445"
	}

	tcpConn, err := net.DialTimeout("tcp", host, 8*time.Second)
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", host, err)
	}

	d := &smb2.Dialer{
		Initiator: &smb2.NTLMInitiator{
			User:     testCfg.Username,
			Password: testCfg.Password,
			Domain:   testCfg.Domain,
		},
	}

	s, err := d.Dial(tcpConn)
	if err != nil {
		_ = tcpConn.Close()
		return nil, nil, fmt.Errorf("smb auth: %w", err)
	}

	share, err := s.Mount(strings.TrimSpace(testCfg.Share))
	if err != nil {
		_ = s.Logoff()
		_ = tcpConn.Close()
		return nil, nil, fmt.Errorf("mount share %s: %w", strings.TrimSpace(testCfg.Share), err)
	}
	return s, share, nil
}

func backoffForAttempt(attempt int) time.Duration {
	base := 2 * time.Second
	max := 90 * time.Second
	if server.IsNightOpsActive() {
		base = 1 * time.Second
		max = 30 * time.Second
	}
	d := base * time.Duration(1<<(attempt-1))
	if d > max {
		d = max
	}
	jitter := time.Duration(rand.Intn(700)) * time.Millisecond
	return d + jitter
}

func isRetryableNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if os.IsTimeout(err) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() || netErr.Temporary() {
			return true
		}
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.ETIMEDOUT, syscall.EHOSTUNREACH, syscall.ENETUNREACH, syscall.EPIPE:
			return true
		}
	}
	s := strings.ToLower(err.Error())
	for _, token := range []string{"timeout", "timed out", "connection reset", "connection refused", "network is unreachable", "no route to host", "broken pipe", "dial tcp"} {
		if strings.Contains(s, token) {
			return true
		}
	}
	return false
}

func setChannelStatus(channelID, level, text string) {
	if channelID == "" {
		return
	}
	statusMu.Lock()
	channelStatus[channelID] = ChannelStatus{Level: level, Text: text, UpdatedAt: time.Now()}
	statusMu.Unlock()
	if StatusUpdateHook != nil {
		StatusUpdateHook(channelID)
	}
}

func appendLog(channelID, message string) {
	if message == "" {
		return
	}
	prefix := "[SMB]"
	if channelID != "" {
		prefix = "[SMB " + channelID + "]"
	}
	line := fmt.Sprintf("%s %s %s", time.Now().Format("15:04:05"), prefix, message)

	statusMu.Lock()
	recentSMBLogs = append(recentSMBLogs, line)
	if len(recentSMBLogs) > logBufferSize {
		recentSMBLogs = recentSMBLogs[len(recentSMBLogs)-logBufferSize:]
	}
	statusMu.Unlock()

	fmt.Println(line)
	if LogUpdateHook != nil {
		LogUpdateHook()
	}
}

func GetChannelStatus(channelID string) ChannelStatus {
	statusMu.RLock()
	defer statusMu.RUnlock()
	return channelStatus[channelID]
}

func GetLogs() []string {
	statusMu.RLock()
	defer statusMu.RUnlock()
	out := make([]string, len(recentSMBLogs))
	copy(out, recentSMBLogs)
	return out
}
