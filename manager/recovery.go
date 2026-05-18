package manager

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/notifier"
	"github.com/HeapOfChaos/goondvr/server"
	"github.com/HeapOfChaos/goondvr/uploader"
)

const (
	defaultRecoveryUploadWindowHrs = 7 * 24
	defaultRecoveryFFprobeMaxChecks = 24
	recoveryMinFileAge             = 20 * time.Second
	recoveryMaxReportItems         = 12
)

type RecoveryDuplicateGroup struct {
	HashPrefix string
	Size       int64
	Paths      []string
}

type RecoveryCorruptFile struct {
	Path   string
	Reason string
}

type RecoveryReport struct {
	Enabled         bool
	LastRunAt       time.Time
	Scanned         int
	Queued          int
	DuplicateGroups int
	CorruptFiles    int
	Duplicates      []RecoveryDuplicateGroup
	Corrupt         []RecoveryCorruptFile
}

type recoveryCandidate struct {
	ChannelID     string
	LocalPath     string
	RemoteRelPath string
	Size          int64
	ModTime       time.Time
}

var (
	recoveryReportMu sync.RWMutex
	recoveryReport   = RecoveryReport{Enabled: true}
)

// GetRecoveryReport returns a copy of the most recent startup recovery report.
func GetRecoveryReport() RecoveryReport {
	recoveryReportMu.RLock()
	defer recoveryReportMu.RUnlock()

	out := recoveryReport
	out.Duplicates = cloneDuplicateGroups(recoveryReport.Duplicates)
	out.Corrupt = cloneCorruptFiles(recoveryReport.Corrupt)
	return out
}

func cloneDuplicateGroups(in []RecoveryDuplicateGroup) []RecoveryDuplicateGroup {
	if len(in) == 0 {
		return nil
	}
	out := make([]RecoveryDuplicateGroup, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].Paths = append([]string(nil), in[i].Paths...)
	}
	return out
}

func cloneCorruptFiles(in []RecoveryCorruptFile) []RecoveryCorruptFile {
	if len(in) == 0 {
		return nil
	}
	out := make([]RecoveryCorruptFile, len(in))
	copy(out, in)
	return out
}

func setRecoveryReport(report RecoveryReport) {
	recoveryReportMu.Lock()
	defer recoveryReportMu.Unlock()
	recoveryReport = report
}

func recoveryUploadWindow() time.Duration {
	hours := defaultRecoveryUploadWindowHrs
	if cfg := server.Config; cfg != nil && cfg.RecoveryUploadWindowHrs > 0 {
		hours = cfg.RecoveryUploadWindowHrs
	}
	return time.Duration(hours) * time.Hour
}

func recoveryMaxFFprobeChecks() int {
	maxChecks := defaultRecoveryFFprobeMaxChecks
	if cfg := server.Config; cfg != nil && cfg.RecoveryMaxFFprobe > 0 {
		maxChecks = cfg.RecoveryMaxFFprobe
	}
	return maxChecks
}

func (m *Manager) runStartupRecovery(config []*entity.ChannelConfig) {
	if len(config) == 0 {
		return
	}
	if cfg := server.Config; cfg != nil && !cfg.RecoveryEnabled {
		setRecoveryReport(RecoveryReport{Enabled: false})
		fmt.Printf("[RECOVERY] disabled by settings\n")
		return
	}
	go m.recoverAfterRestart(config)
}

func (m *Manager) recoverAfterRestart(config []*entity.ChannelConfig) {
	candidates := collectRecoveryCandidates(config)
	queued := queueRecoverableUploads(candidates)
	dedupCount, corruptCount, duplicates, corrupt := detectIntegrityIssues(candidates)

	report := RecoveryReport{
		Enabled:         true,
		LastRunAt:       time.Now(),
		Scanned:         len(candidates),
		Queued:          queued,
		DuplicateGroups: dedupCount,
		CorruptFiles:    corruptCount,
		Duplicates:      duplicates,
		Corrupt:         corrupt,
	}
	setRecoveryReport(report)

	if len(candidates) == 0 {
		return
	}

	fmt.Printf("[RECOVERY] scanned=%d queued=%d duplicates=%d corrupt=%d\n", len(candidates), queued, dedupCount, corruptCount)

	if queued > 0 {
		notifier.Notify(
			"recovery_upload_queue",
			"Recovery mode: uploads resumed",
			fmt.Sprintf("Restart recovery queued %d completed recordings for SMB upload.", queued),
		)
	}
	if dedupCount > 0 || corruptCount > 0 {
		notifier.Notify(
			"recovery_integrity_summary",
			"Recovery mode: integrity findings",
			fmt.Sprintf("Found %d duplicate group(s) and %d potentially corrupt file(s).", dedupCount, corruptCount),
		)
	}
}

func collectRecoveryCandidates(config []*entity.ChannelConfig) []recoveryCandidate {
	now := time.Now()
	seen := make(map[string]struct{})
	out := make([]recoveryCandidate, 0, 64)

	for _, conf := range config {
		if conf == nil {
			continue
		}
		channelID := entity.ChannelID(conf.Site, conf.Username)
		completedDir := completedDirForPattern(conf.Pattern)
		if completedDir == "" {
			continue
		}
		cleanCompleted := filepath.Clean(completedDir)

		_ = filepath.WalkDir(cleanCompleted, func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			if !isLikelyVideoFile(p) {
				return nil
			}
			if !belongsToChannel(conf.Username, filepath.Base(p)) {
				return nil
			}

			info, statErr := d.Info()
			if statErr != nil {
				return nil
			}
			age := now.Sub(info.ModTime())
			if age < recoveryMinFileAge {
				return nil
			}

			cleanPath := filepath.Clean(p)
			if _, ok := seen[cleanPath]; ok {
				return nil
			}
			seen[cleanPath] = struct{}{}

			rel := filepath.Base(cleanPath)
			if r, relErr := filepath.Rel(cleanCompleted, cleanPath); relErr == nil && r != "" && r != "." && !strings.HasPrefix(r, "..") {
				rel = filepath.ToSlash(r)
			}

			out = append(out, recoveryCandidate{
				ChannelID:     channelID,
				LocalPath:     cleanPath,
				RemoteRelPath: path.Join("completed", rel),
				Size:          info.Size(),
				ModTime:       info.ModTime(),
			})
			return nil
		})
	}

	return out
}

func queueRecoverableUploads(candidates []recoveryCandidate) int {
	cfg := server.Config
	if cfg == nil || !cfg.SMBUploadEnabled {
		return 0
	}

	now := time.Now()
	window := recoveryUploadWindow()
	queued := 0
	for _, c := range candidates {
		if now.Sub(c.ModTime) > window {
			continue
		}
		uploader.UploadIfEnabled(c.ChannelID, c.LocalPath, c.RemoteRelPath)
		queued++
	}
	return queued
}

func detectIntegrityIssues(candidates []recoveryCandidate) (int, int, []RecoveryDuplicateGroup, []RecoveryCorruptFile) {
	if len(candidates) == 0 {
		return 0, 0, nil, nil
	}

	bySize := make(map[int64][]recoveryCandidate)
	corruptCount := 0
	ffprobeChecks := 0
	maxFFprobe := recoveryMaxFFprobeChecks()
	duplicates := make([]RecoveryDuplicateGroup, 0, recoveryMaxReportItems)
	corrupt := make([]RecoveryCorruptFile, 0, recoveryMaxReportItems)
	appendCorrupt := func(path, reason string) {
		if len(corrupt) < recoveryMaxReportItems {
			corrupt = append(corrupt, RecoveryCorruptFile{Path: path, Reason: reason})
		}
	}

	for _, c := range candidates {
		if c.Size <= 0 {
			corruptCount++
			fmt.Printf("[RECOVERY] corrupt(empty): %s\n", c.LocalPath)
			appendCorrupt(c.LocalPath, "empty file")
			continue
		}
		bySize[c.Size] = append(bySize[c.Size], c)

		if ffprobeChecks < maxFFprobe {
			ffprobeChecks++
			if err := probeMedia(c.LocalPath); err != nil {
				corruptCount++
				fmt.Printf("[RECOVERY] corrupt(ffprobe): %s: %v\n", c.LocalPath, err)
				appendCorrupt(c.LocalPath, err.Error())
			}
		}
	}

	dedupGroups := 0
	for _, group := range bySize {
		if len(group) < 2 {
			continue
		}
		hashMap := make(map[string][]recoveryCandidate)
		for _, c := range group {
			h, err := hashFileSHA256(c.LocalPath)
			if err != nil {
				fmt.Printf("[RECOVERY] hash error: %s: %v\n", c.LocalPath, err)
				continue
			}
			hashMap[h] = append(hashMap[h], c)
		}
		for hash, dupes := range hashMap {
			if len(dupes) < 2 {
				continue
			}
			dedupGroups++
			sort.Slice(dupes, func(i, j int) bool {
				return dupes[i].LocalPath < dupes[j].LocalPath
			})
			paths := make([]string, 0, len(dupes))
			for _, d := range dupes {
				paths = append(paths, d.LocalPath)
			}
			fmt.Printf("[RECOVERY] duplicate(hash=%s size=%d): %s\n", hash[:12], dupes[0].Size, strings.Join(paths, " | "))
			if len(duplicates) < recoveryMaxReportItems {
				duplicates = append(duplicates, RecoveryDuplicateGroup{
					HashPrefix: hash[:12],
					Size:       dupes[0].Size,
					Paths:      paths,
				})
			}
		}
	}

	return dedupGroups, corruptCount, duplicates, corrupt
}

func completedDirForPattern(pattern string) string {
	if cfg := server.Config; cfg != nil && strings.TrimSpace(cfg.CompletedDir) != "" {
		return strings.TrimSpace(cfg.CompletedDir)
	}
	return filepath.Join(recordingDirFromPattern(pattern), "completed")
}

func recordingDirFromPattern(pattern string) string {
	idx := strings.Index(pattern, "{{")
	if idx == -1 {
		return "."
	}
	dir := filepath.Dir(pattern[:idx])
	if dir == "" || dir == "." {
		return "."
	}
	return dir
}

func belongsToChannel(username, base string) bool {
	u := strings.ToLower(strings.TrimSpace(username))
	b := strings.ToLower(strings.TrimSpace(base))
	if u == "" || b == "" {
		return false
	}
	return strings.HasPrefix(b, u+"_") || strings.HasPrefix(b, u+".") || b == u
}

func isLikelyVideoFile(filePath string) bool {
	ext := strings.ToLower(path.Ext(filePath))
	return ext == ".mp4" || ext == ".mkv" || ext == ".ts" || ext == ".mov"
}

func probeMedia(localPath string) error {
	if _, err := exec.LookPath("ffprobe"); err != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-show_entries", "format=duration,size", "-of", "default=noprint_wrappers=1:nokey=1", localPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg == "" {
			msg = err.Error()
		}
		return fmt.Errorf("%s", msg)
	}
	return nil
}

func hashFileSHA256(localPath string) (string, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
