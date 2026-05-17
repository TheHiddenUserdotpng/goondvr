package notifier

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/HeapOfChaos/goondvr/server"
)

const discordAPIBase = "https://discord.com/api/v10"

// BotChannel carries the minimal info needed to render the status embed.
type BotChannel struct {
	Username  string
	Site      string
	RoomTitle string
	IsOnline  bool
}

// BotChannelsHook is set by the manager to return the current channel list.
// It must be safe to call from any goroutine.
var BotChannelsHook func() []BotChannel

// BotMessageIDHook is called whenever the bot posts a new status message so
// the ID can be persisted (prevents duplicate messages across restarts).
var BotMessageIDHook func(id string)

// StatusBot is the package-level singleton for the Discord status embed bot.
var StatusBot = &discordStatusBot{}

type discordStatusBot struct {
	mu       sync.Mutex
	debounce *time.Timer
}

// Refresh schedules a debounced embed update (500 ms) so rapid status flips
// are batched into a single Discord API call.
func (b *discordStatusBot) Refresh() {
	cfg := server.Config
	if cfg == nil || cfg.DiscordBotToken == "" || cfg.DiscordStatusChannelID == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.debounce != nil {
		b.debounce.Stop()
	}
	b.debounce = time.AfterFunc(500*time.Millisecond, b.publish)
}

func (b *discordStatusBot) publish() {
	cfg := server.Config
	if cfg == nil {
		return
	}
	token := cfg.DiscordBotToken
	channelID := cfg.DiscordStatusChannelID
	messageID := cfg.DiscordStatusMessageID

	if token == "" || channelID == "" {
		return
	}

	var channels []BotChannel
	if BotChannelsHook != nil {
		channels = BotChannelsHook()
	}

	payload := map[string]any{
		"embeds": []any{buildStatusEmbed(channels)},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		fmt.Printf("[WARN] discord bot: marshal embed: %v\n", err)
		return
	}

	var (
		resp   *http.Response
		method string
		url    string
	)

	if messageID == "" {
		method = http.MethodPost
		url = fmt.Sprintf("%s/channels/%s/messages", discordAPIBase, channelID)
	} else {
		method = http.MethodPatch
		url = fmt.Sprintf("%s/channels/%s/messages/%s", discordAPIBase, channelID, messageID)
	}

	resp, err = b.doRequest(method, url, token, body)
	if err != nil {
		fmt.Printf("[WARN] discord bot: %v\n", err)
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	// If the previously stored message was deleted, fall back to posting a new one.
	if resp.StatusCode == http.StatusNotFound && messageID != "" {
		if BotMessageIDHook != nil {
			BotMessageIDHook("") // clear stored ID
		}
		server.Config.DiscordStatusMessageID = ""
		resp2, err2 := b.doRequest(http.MethodPost,
			fmt.Sprintf("%s/channels/%s/messages", discordAPIBase, channelID),
			token, body)
		if err2 != nil {
			fmt.Printf("[WARN] discord bot: retry post: %v\n", err2)
			return
		}
		defer resp2.Body.Close()
		respBody, _ = io.ReadAll(resp2.Body)
		resp = resp2
		messageID = ""
	}

	if resp.StatusCode >= 300 {
		fmt.Printf("[WARN] discord bot: HTTP %d: %s\n", resp.StatusCode, string(respBody))
		return
	}

	// Persist the message ID when we posted a brand-new message.
	if messageID == "" {
		var msg struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(respBody, &msg); err == nil && msg.ID != "" {
			server.Config.DiscordStatusMessageID = msg.ID
			if BotMessageIDHook != nil {
				BotMessageIDHook(msg.ID)
			}
		}
	}
}

func (b *discordStatusBot) doRequest(method, url, token string, body []byte) (*http.Response, error) {
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bot "+token)
	return http.DefaultClient.Do(req)
}

func buildStatusEmbed(channels []BotChannel) map[string]any {
	now := time.Now().UTC()
	live := make([]BotChannel, 0, len(channels))
	offline := make([]BotChannel, 0, len(channels))

	for _, ch := range channels {
		if ch.IsOnline {
			live = append(live, ch)
			continue
		}
		offline = append(offline, ch)
	}

	sort.Slice(live, func(i, j int) bool {
		return strings.ToLower(live[i].Username) < strings.ToLower(live[j].Username)
	})
	sort.Slice(offline, func(i, j int) bool {
		return strings.ToLower(offline[i].Username) < strings.ToLower(offline[j].Username)
	})

	statusEmoji := "⚫"
	statusText := "Idle"
	color := 0x99AAB5 // grey
	if len(live) > 0 {
		statusEmoji = "🔴"
		statusText = "Recording"
		color = 0xED4245 // discord red
	}

	fields := []map[string]any{
		{
			"name":  "Overview",
			"value": fmt.Sprintf("%s **%s**\nLive: **%d** | Offline: **%d** | Total: **%d**", statusEmoji, statusText, len(live), len(offline), len(channels)),
		},
	}

	if len(live) == 0 {
		fields = append(fields, map[string]any{
			"name":  "Live Now",
			"value": "No channels are live at the moment.",
		})
	} else {
		maxLiveLines := 10
		liveLines := make([]string, 0, maxLiveLines)
		for i, ch := range live {
			if i >= maxLiveLines {
				break
			}
			roomTitle := truncateDiscord(ch.RoomTitle, 56)
			if roomTitle == "" {
				roomTitle = "Live now"
			}
			liveLines = append(liveLines, fmt.Sprintf("• **%s** (%s) - %s", ch.Username, siteLabel(ch.Site), roomTitle))
		}
		if len(live) > maxLiveLines {
			liveLines = append(liveLines, fmt.Sprintf("...and **%d** more", len(live)-maxLiveLines))
		}

		fields = append(fields, map[string]any{
			"name":  fmt.Sprintf("Live Now (%d)", len(live)),
			"value": truncateDiscord(strings.Join(liveLines, "\n"), 1024),
		})
	}

	if len(offline) > 0 {
		maxOffline := 12
		offlineNames := make([]string, 0, maxOffline)
		for i, ch := range offline {
			if i >= maxOffline {
				break
			}
			offlineNames = append(offlineNames, ch.Username)
		}
		offlineText := strings.Join(offlineNames, ", ")
		if len(offline) > maxOffline {
			offlineText += fmt.Sprintf(" (+%d)", len(offline)-maxOffline)
		}

		fields = append(fields, map[string]any{
			"name":   "Offline Watchlist",
			"value":  truncateDiscord(offlineText, 1024),
			"inline": false,
		})
	}

	return map[string]any{
		"title":       "GOONDVR LIVE STATUS",
		"description": fmt.Sprintf("Auto-updating status board for monitored channels.\nUpdated <t:%d:R>.", now.Unix()),
		"color":       color,
		"fields":      fields,
		"timestamp":   now.Format(time.RFC3339),
		"footer": map[string]any{
			"text": fmt.Sprintf("Last update • %s UTC", now.Format("2006-01-02 15:04:05")),
		},
	}
}

func siteLabel(site string) string {
	switch strings.ToLower(strings.TrimSpace(site)) {
	case "stripchat":
		return "Stripchat"
	default:
		return "Chaturbate"
	}
}

func truncateDiscord(s string, max int) string {
	s = strings.TrimSpace(s)
	if max <= 0 || len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return strings.TrimSpace(s[:max-3]) + "..."
}
