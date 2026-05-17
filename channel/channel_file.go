package channel

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/HeapOfChaos/goondvr/chaturbate"
	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/server"
	"github.com/HeapOfChaos/goondvr/uploader"
)

// Pattern holds the date/time and sequence information for the filename pattern
type Pattern struct {
	Username string
	Site     string
	Year     string
	Month    string
	Day      string
	Hour     string
	Minute   string
	Second   string
	Sequence int
}

// NextFile prepares the next file to be created, by cleaning up the last file and generating a new one.
// ext is the file extension to use (e.g. ".ts" or ".mp4").
func (ch *Channel) NextFile(ext string) error {
	ch.fileMu.Lock()
	defer ch.fileMu.Unlock()

	if err := ch.cleanupLocked(); err != nil {
		return err
	}
	filename, err := ch.generateFilenameLocked()
	if err != nil {
		return err
	}
	if err := ch.createNewFileLocked(filename, ext); err != nil {
		return err
	}

	// Increment the sequence number for the next file
	ch.Sequence++
	return nil
}

// Cleanup cleans the file and resets it, called when the stream errors out or before next file was created.
func (ch *Channel) Cleanup() error {
	ch.fileMu.Lock()
	defer ch.fileMu.Unlock()

	return ch.cleanupLocked()
}

func (ch *Channel) cleanupLocked() error {
	if ch.File == nil {
		return nil
	}
	filename := ch.File.Name()

	defer func() {
		ch.Filesize = 0
		ch.Duration = 0
	}()

	// Sync the file to ensure data is written to disk
	if err := ch.File.Sync(); err != nil && !errors.Is(err, os.ErrClosed) {
		return fmt.Errorf("sync file: %w", err)
	}
	if err := ch.File.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		return fmt.Errorf("close file: %w", err)
	}
	ch.File = nil

	// Delete the empty file
	fileInfo, err := os.Stat(filename)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("stat file delete zero file: %w", err)
	}
	if fileInfo != nil && fileInfo.Size() == 0 {
		if err := os.Remove(filename); err != nil {
			return fmt.Errorf("remove zero file: %w", err)
		}
		go ch.ScanTotalDiskUsage()
	} else if fileInfo != nil {
		ch.startFinalization()
		go ch.finalizeRecording(filename)
	}

	return nil
}

// GenerateFilename creates a filename based on the configured pattern and the current timestamp
func (ch *Channel) GenerateFilename() (string, error) {
	ch.fileMu.RLock()
	defer ch.fileMu.RUnlock()

	return ch.generateFilenameLocked()
}

func (ch *Channel) generateFilenameLocked() (string, error) {
	var buf bytes.Buffer

	// Parse the filename pattern defined in the channel's config
	tpl, err := template.New("filename").Parse(ch.Config.Pattern)
	if err != nil {
		return "", fmt.Errorf("filename pattern error: %w", err)
	}

	// Get the current time based on the Unix timestamp when the stream was started
	t := time.Unix(ch.StreamedAt, 0)
	pattern := &Pattern{
		Username: ch.Config.Username,
		Site:     ch.Config.Site,
		Sequence: ch.Sequence,
		Year:     t.Format("2006"),
		Month:    t.Format("01"),
		Day:      t.Format("02"),
		Hour:     t.Format("15"),
		Minute:   t.Format("04"),
		Second:   t.Format("05"),
	}

	if err := tpl.Execute(&buf, pattern); err != nil {
		return "", fmt.Errorf("template execution error: %w", err)
	}
	return buf.String(), nil
}

// CreateNewFile creates a new file for the channel using the given filename and extension.
func (ch *Channel) CreateNewFile(filename, ext string) error {
	ch.fileMu.Lock()
	defer ch.fileMu.Unlock()

	return ch.createNewFileLocked(filename, ext)
}

func (ch *Channel) createNewFileLocked(filename, ext string) error {

	// Ensure the directory exists before creating the file
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return fmt.Errorf("mkdir all: %w", err)
	}

	// Open the file in append mode, create it if it doesn't exist
	file, err := os.OpenFile(filename+ext, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("cannot open file: %s: %w", filename, err)
	}

	ch.File = file
	return nil
}

// recordingDirFromPattern extracts the base directory from a filename pattern
// like "videos/{{.Username}}_..." → "videos".
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

func completedDirForChannel(ch *Channel) string {
	if server.Config.CompletedDir != "" {
		return server.Config.CompletedDir
	}
	return filepath.Join(recordingDirFromPattern(ch.Config.Pattern), "completed")
}

func finalOutputExt(filename string) string {
	if server.Config.FFmpegContainer == "mkv" {
		return ".mkv"
	}
	if server.Config.FinalizeMode == "none" {
		return filepath.Ext(filename)
	}
	return ".mp4"
}

func finalOutputPath(filename string) string {
	base := strings.TrimSuffix(filename, filepath.Ext(filename))
	return base + finalOutputExt(filename)
}

// ScanTotalDiskUsage calculates the total bytes of all recordings for this channel
// by walking the recording directory for files whose name starts with the username.
// The result is stored in TotalDiskUsageBytes.
func (ch *Channel) ScanTotalDiskUsage() {
	recordingDir := filepath.Clean(recordingDirFromPattern(ch.Config.Pattern))
	dirs := []string{recordingDir}
	completedDir := completedDirForChannel(ch)
	cleanCompletedDir := filepath.Clean(completedDir)
	if cleanCompletedDir != "" &&
		cleanCompletedDir != recordingDir &&
		!strings.HasPrefix(cleanCompletedDir+string(os.PathSeparator), recordingDir+string(os.PathSeparator)) {
		dirs = append(dirs, completedDir)
	}
	prefix := ch.Config.Username
	var total int64
	for _, dir := range dirs {
		_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			if strings.HasPrefix(filepath.Base(path), prefix) {
				if info, err2 := d.Info(); err2 == nil {
					total += info.Size()
				}
			}
			return nil
		})
	}
	ch.fileMu.Lock()
	ch.TotalDiskUsageBytes = total
	ch.fileMu.Unlock()
}

// ShouldSwitchFile determines whether a new file should be created.
func (ch *Channel) ShouldSwitchFile() bool {
	ch.fileMu.RLock()
	defer ch.fileMu.RUnlock()

	return ch.shouldSwitchFileLocked()
}

func (ch *Channel) shouldSwitchFileLocked() bool {
	maxFilesizeBytes := ch.Config.MaxFilesize * 1024 * 1024
	maxDurationSeconds := ch.Config.MaxDuration * 60

	return (ch.Duration >= float64(maxDurationSeconds) && ch.Config.MaxDuration > 0) ||
		(ch.Filesize >= maxFilesizeBytes && ch.Config.MaxFilesize > 0)
}

// isMP4InitSegment reports whether b looks like an fMP4 init segment containing
// top-level ftyp/moov boxes and no media fragments yet.
func isMP4InitSegment(b []byte) bool {
	var hasFtyp bool
	var hasMoov bool

	for pos := 0; pos+8 <= len(b); {
		size := int(binary.BigEndian.Uint32(b[pos:]))
		if size < 8 || pos+size > len(b) {
			return false
		}

		switch string(b[pos+4 : pos+8]) {
		case "ftyp":
			hasFtyp = true
		case "moov":
			hasMoov = true
		case "moof", "mdat", "mfra":
			return false
		}
		pos += size
	}

	return hasFtyp && hasMoov
}

func (ch *Channel) finalizeRecording(filename string) {
	defer ch.finishFinalization()

	finalPath := filename
	if server.Config.FinalizeMode == "none" {
		if strings.HasSuffix(filename, ".mp4") {
			if err := chaturbate.BuildSeekIndex(filename); err != nil {
				log.Printf("WARN  seek index %s: %v", filename, err)
			}
		}
	} else {
		processedPath, err := ch.runFFmpegFinalizer(filename)
		if err != nil {
			ch.Error("ffmpeg %s failed for `%s`: %s", server.Config.FinalizeMode, filename, err.Error())
			ch.Info("keeping original recording because finalization failed")
		} else {
			if processedPath != filename {
				if err := os.Remove(filename); err != nil {
					ch.Error("remove original after ffmpeg finalization `%s`: %s", filename, err.Error())
				}
			}
			finalPath = processedPath
		}
	}

	completedDir := completedDirForChannel(ch)
	if completedDir != "" {
		dst, err := moveRecordingToDir(finalPath, recordingDirFromPattern(ch.Config.Pattern), completedDir)
		if err != nil {
			ch.Error("move completed recording `%s`: %s", finalPath, err.Error())
		} else {
			ch.Info("completed recording moved to `%s`", dst)
			rel := filepath.Base(dst)
			if r, relErr := filepath.Rel(completedDir, dst); relErr == nil && r != "" && r != "." && !strings.HasPrefix(r, "..") {
				rel = filepath.ToSlash(r)
			}
			uploader.UploadIfEnabled(entity.ChannelID(ch.Config.Site, ch.Config.Username), dst, path.Join("completed", rel))
		}
	}

	go ch.ScanTotalDiskUsage()
}

func moveRecordingToDir(src, recordingRoot, completedDir string) (string, error) {
	dstDir := completedDir

	srcDir := filepath.Dir(src)
	cleanRoot := filepath.Clean(recordingRoot)
	cleanSrcDir := filepath.Clean(srcDir)
	if relDir, err := filepath.Rel(cleanRoot, cleanSrcDir); err == nil && relDir != ".." && !strings.HasPrefix(relDir, ".."+string(os.PathSeparator)) {
		if relDir != "." {
			dstDir = filepath.Join(completedDir, relDir)
		}
	}

	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return "", fmt.Errorf("mkdir completed dir: %w", err)
	}

	dst := filepath.Join(dstDir, filepath.Base(src))
	if src == dst {
		return dst, nil
	}

	if err := os.Rename(src, dst); err == nil {
		return dst, nil
	} else if !isCrossDeviceRename(err) {
		return "", fmt.Errorf("rename completed file: %w", err)
	}

	if err := copyFile(src, dst); err != nil {
		return "", err
	}
	if err := os.Remove(src); err != nil {
		return "", fmt.Errorf("remove source after copy: %w", err)
	}
	return dst, nil
}

func isCrossDeviceRename(err error) bool {
	linkErr := &os.LinkError{}
	return errors.As(err, &linkErr) && errors.Is(linkErr.Err, syscall.EXDEV)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source file: %w", err)
	}
	defer in.Close()

	info, err := in.Stat()
	if err != nil {
		return fmt.Errorf("stat source file: %w", err)
	}

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
	if err != nil {
		return fmt.Errorf("create destination file: %w", err)
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copy file: %w", err)
	}
	if err := out.Sync(); err != nil {
		return fmt.Errorf("sync destination file: %w", err)
	}
	return nil
}

// CreateClipLastSeconds creates a clip from the tail of the currently open
// recording file. It returns the created clip path.
func (ch *Channel) CreateClipLastSeconds(seconds int) (string, error) {
	if seconds <= 0 {
		seconds = 45
	}
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return "", fmt.Errorf("ffmpeg not found in PATH")
	}

	ch.fileMu.RLock()
	var src string
	if ch.File != nil {
		src = ch.File.Name()
	}
	ch.fileMu.RUnlock()
	if src == "" {
		src = latestChannelRecording(ch)
		if src == "" {
			return "", fmt.Errorf("no recording file found for clip")
		}
	}

	if _, err := os.Stat(src); err != nil {
		return "", fmt.Errorf("stat source file: %w", err)
	}

	clipDir := filepath.Join(recordingDirFromPattern(ch.Config.Pattern), "clips", ch.Config.Site, ch.Config.Username)
	if err := os.MkdirAll(clipDir, 0755); err != nil {
		return "", fmt.Errorf("mkdir clip dir: %w", err)
	}

	now := time.Now().UTC().Format("20060102_150405")
	clipName := fmt.Sprintf("%s_clip_%ds_%s.mp4", ch.Config.Username, seconds, now)
	outPath := filepath.Join(clipDir, clipName)
	tmpPath := outPath + ".tmp.mp4"
	_ = os.Remove(tmpPath)

	args := []string{
		"-nostdin", "-y",
		"-sseof", fmt.Sprintf("-%d", seconds),
		"-i", src,
		"-map", "0",
		"-c", "copy",
		"-movflags", "+faststart",
		tmpPath,
	}
	cmd := exec.Command("ffmpeg", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("ffmpeg clip failed: %w (%s)", err, strings.TrimSpace(string(output)))
	}

	if err := os.Rename(tmpPath, outPath); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("finalize clip file: %w", err)
	}

	ch.Info("clip created: `%s`", outPath)
	uploader.UploadIfEnabled(entity.ChannelID(ch.Config.Site, ch.Config.Username), outPath, path.Join("clips", ch.Config.Site, ch.Config.Username, filepath.Base(outPath)))
	go ch.ScanTotalDiskUsage()
	return outPath, nil
}

func latestChannelRecording(ch *Channel) string {
	recordingDir := filepath.Clean(recordingDirFromPattern(ch.Config.Pattern))
	dirs := []string{recordingDir, completedDirForChannel(ch)}
	prefix := ch.Config.Username
	var newest string
	var newestMod time.Time
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			base := filepath.Base(path)
			if !strings.HasPrefix(base, prefix) {
				return nil
			}
			ext := strings.ToLower(filepath.Ext(base))
			if ext != ".mp4" && ext != ".mkv" && ext != ".ts" {
				return nil
			}
			if strings.Contains(path, string(filepath.Separator)+"clips"+string(filepath.Separator)) {
				return nil
			}
			info, infoErr := d.Info()
			if infoErr != nil {
				return nil
			}
			if info.ModTime().After(newestMod) {
				newestMod = info.ModTime()
				newest = path
			}
			return nil
		})
	}
	return newest
}

func clipsDirForChannel(ch *Channel) string {
	return filepath.Join(recordingDirFromPattern(ch.Config.Pattern), "clips", ch.Config.Site, ch.Config.Username)
}

func (ch *Channel) ListRecordings() []string {
	return listChannelMediaFiles(ch, false)
}

func (ch *Channel) ListClips() []string {
	dir := clipsDirForChannel(ch)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	paths := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(e.Name()))
		if ext != ".mp4" && ext != ".mkv" {
			continue
		}
		paths = append(paths, filepath.Join(dir, e.Name()))
	}
	sort.Slice(paths, func(i, j int) bool {
		infoI, errI := os.Stat(paths[i])
		infoJ, errJ := os.Stat(paths[j])
		if errI != nil || errJ != nil {
			return paths[i] > paths[j]
		}
		return infoI.ModTime().After(infoJ.ModTime())
	})
	return paths
}

func (ch *Channel) CreateClipFromRecording(source string, startSeconds, durationSeconds int, clipName string) (string, error) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return "", fmt.Errorf("ffmpeg not found in PATH")
	}
	if durationSeconds <= 0 {
		durationSeconds = 45
	}
	if durationSeconds > 3600 {
		durationSeconds = 3600
	}
	if startSeconds < 0 {
		startSeconds = 0
	}

	if source == "" {
		return "", fmt.Errorf("source recording is required")
	}
	if !ch.isAllowedRecordingPath(source) {
		return "", fmt.Errorf("recording is outside allowed directories")
	}
	if _, err := os.Stat(source); err != nil {
		return "", fmt.Errorf("source recording does not exist")
	}

	clipDir := clipsDirForChannel(ch)
	if err := os.MkdirAll(clipDir, 0755); err != nil {
		return "", fmt.Errorf("mkdir clip dir: %w", err)
	}

	base := sanitizeClipName(clipName)
	if base == "" {
		base = ch.Config.Username + "_moment_" + time.Now().UTC().Format("20060102_150405")
	}
	outPath := filepath.Join(clipDir, base+".mp4")
	tmpPath := outPath + ".tmp.mp4"
	_ = os.Remove(tmpPath)

	args := []string{
		"-nostdin", "-y",
		"-ss", strconv.Itoa(startSeconds),
		"-i", source,
		"-t", strconv.Itoa(durationSeconds),
		"-map", "0",
		"-c", "copy",
		"-movflags", "+faststart",
		tmpPath,
	}
	cmd := exec.Command("ffmpeg", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("ffmpeg clip failed: %w (%s)", err, strings.TrimSpace(string(output)))
	}
	if err := os.Rename(tmpPath, outPath); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("finalize clip file: %w", err)
	}

	ch.Info("clip created from recording: `%s`", outPath)
	uploader.UploadIfEnabled(entity.ChannelID(ch.Config.Site, ch.Config.Username), outPath, path.Join("clips", ch.Config.Site, ch.Config.Username, filepath.Base(outPath)))
	return outPath, nil
}

func (ch *Channel) CombineClips(clips []string, outputName string) (string, error) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return "", fmt.Errorf("ffmpeg not found in PATH")
	}
	if len(clips) < 2 {
		return "", fmt.Errorf("select at least 2 clips")
	}

	allowed := make(map[string]struct{})
	for _, p := range ch.ListClips() {
		allowed[filepath.Clean(p)] = struct{}{}
	}

	validated := make([]string, 0, len(clips))
	for _, c := range clips {
		clean := filepath.Clean(c)
		if _, ok := allowed[clean]; !ok {
			return "", fmt.Errorf("invalid clip selection")
		}
		validated = append(validated, clean)
	}

	clipDir := clipsDirForChannel(ch)
	if err := os.MkdirAll(clipDir, 0755); err != nil {
		return "", fmt.Errorf("mkdir clip dir: %w", err)
	}

	base := sanitizeClipName(outputName)
	if base == "" {
		base = ch.Config.Username + "_highlights_" + time.Now().UTC().Format("20060102_150405")
	}
	outPath := filepath.Join(clipDir, base+".mp4")
	tmpOut := outPath + ".tmp.mp4"
	listFile := filepath.Join(clipDir, ".concat_"+time.Now().UTC().Format("20060102150405")+".txt")

	var b strings.Builder
	for _, p := range validated {
		escaped := strings.ReplaceAll(p, "'", "'\\''")
		b.WriteString("file '")
		b.WriteString(escaped)
		b.WriteString("'\n")
	}
	if err := os.WriteFile(listFile, []byte(b.String()), 0600); err != nil {
		return "", fmt.Errorf("write concat list: %w", err)
	}
	defer os.Remove(listFile)
	_ = os.Remove(tmpOut)

	args := []string{"-nostdin", "-y", "-f", "concat", "-safe", "0", "-i", listFile, "-c", "copy", "-movflags", "+faststart", tmpOut}
	cmd := exec.Command("ffmpeg", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("ffmpeg concat failed: %w (%s)", err, strings.TrimSpace(string(output)))
	}
	if err := os.Rename(tmpOut, outPath); err != nil {
		_ = os.Remove(tmpOut)
		return "", fmt.Errorf("finalize combined clip: %w", err)
	}

	ch.Info("combined clip created: `%s`", outPath)
	uploader.UploadIfEnabled(entity.ChannelID(ch.Config.Site, ch.Config.Username), outPath, path.Join("clips", ch.Config.Site, ch.Config.Username, filepath.Base(outPath)))
	return outPath, nil
}

func (ch *Channel) isAllowedRecordingPath(path string) bool {
	cleanPath := filepath.Clean(path)
	recordingRoot := filepath.Clean(recordingDirFromPattern(ch.Config.Pattern))
	completedRoot := filepath.Clean(completedDirForChannel(ch))
	inside := func(root string) bool {
		if root == "" {
			return false
		}
		return cleanPath == root || strings.HasPrefix(cleanPath+string(os.PathSeparator), root+string(os.PathSeparator)) || strings.HasPrefix(cleanPath, root+string(os.PathSeparator))
	}
	if !inside(recordingRoot) && !inside(completedRoot) {
		return false
	}
	if strings.Contains(cleanPath, string(filepath.Separator)+"clips"+string(filepath.Separator)) {
		return false
	}
	return true
}

func sanitizeClipName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	name = strings.TrimSuffix(name, filepath.Ext(name))
	var out strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			out.WriteRune(r)
			continue
		}
		if r == ' ' {
			out.WriteRune('_')
		}
	}
	res := strings.Trim(out.String(), "_-")
	if len(res) > 80 {
		res = res[:80]
	}
	return res
}

func listChannelMediaFiles(ch *Channel, includeClips bool) []string {
	recordingDir := filepath.Clean(recordingDirFromPattern(ch.Config.Pattern))
	completedDir := filepath.Clean(completedDirForChannel(ch))
	dirs := []string{recordingDir, completedDir}
	prefix := ch.Config.Username
	type candidate struct {
		path string
		mod  time.Time
	}
	items := make([]candidate, 0)
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			base := filepath.Base(path)
			if !strings.HasPrefix(base, prefix) {
				return nil
			}
			ext := strings.ToLower(filepath.Ext(base))
			if ext != ".mp4" && ext != ".mkv" && ext != ".ts" {
				return nil
			}
			if !includeClips && strings.Contains(path, string(filepath.Separator)+"clips"+string(filepath.Separator)) {
				return nil
			}
			info, infoErr := d.Info()
			if infoErr != nil {
				return nil
			}
			items = append(items, candidate{path: path, mod: info.ModTime()})
			return nil
		})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].mod.After(items[j].mod)
	})
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.path)
	}
	return out
}

func (ch *Channel) runFFmpegFinalizer(filename string) (string, error) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return "", fmt.Errorf("ffmpeg not found in PATH")
	}

	outExt := finalOutputExt(filename)
	finalPath := finalOutputPath(filename)
	tempOutput := strings.TrimSuffix(filename, filepath.Ext(filename)) + ".finalizing" + outExt
	_ = os.Remove(tempOutput)

	args := []string{"-nostdin", "-y", "-i", filename}
	switch server.Config.FinalizeMode {
	case "remux":
		args = append(args, "-c", "copy")
		if outExt == ".mp4" {
			args = append(args, "-movflags", "+faststart")
		}
	case "transcode":
		encoder := strings.TrimSpace(server.Config.FFmpegEncoder)
		if encoder == "" {
			encoder = "libx264"
		}
		args = append(args, "-c:v", encoder)
		args = append(args, qualityArgsForEncoder(encoder, server.Config.FFmpegQuality)...)
		if preset := strings.TrimSpace(server.Config.FFmpegPreset); preset != "" {
			args = append(args, "-preset", preset)
		}
		args = append(args, "-c:a", "copy")
		if outExt == ".mp4" {
			args = append(args, "-movflags", "+faststart")
		}
	default:
		return "", fmt.Errorf("unsupported finalization mode %q", server.Config.FinalizeMode)
	}
	args = append(args, tempOutput)

	ch.Info("running ffmpeg %s for `%s`", server.Config.FinalizeMode, filepath.Base(filename))
	cmd := exec.Command("ffmpeg", args...)
	outputBytes, err := cmd.CombinedOutput()
	if err != nil {
		_ = os.Remove(tempOutput)
		msg := strings.TrimSpace(string(outputBytes))
		if msg == "" {
			msg = err.Error()
		}
		return "", fmt.Errorf("%s", msg)
	}
	if finalPath == filename {
		if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
			_ = os.Remove(tempOutput)
			return "", fmt.Errorf("remove original before replace: %w", err)
		}
	}
	if err := os.Rename(tempOutput, finalPath); err != nil {
		_ = os.Remove(tempOutput)
		return "", fmt.Errorf("rename finalized output: %w", err)
	}
	return finalPath, nil
}

func qualityArgsForEncoder(encoder string, quality int) []string {
	if quality <= 0 {
		quality = 23
	}
	lower := strings.ToLower(strings.TrimSpace(encoder))
	switch {
	case strings.Contains(lower, "nvenc"):
		return []string{"-cq", fmt.Sprintf("%d", quality)}
	case strings.Contains(lower, "qsv"), strings.Contains(lower, "vaapi"), strings.Contains(lower, "amf"):
		return []string{"-global_quality", fmt.Sprintf("%d", quality)}
	default:
		return []string{"-crf", fmt.Sprintf("%d", quality)}
	}
}
