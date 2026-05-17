package server

import "time"

// IsNightOpsActive returns true when Night Ops mode is enabled and current local
// time is inside the configured Night Ops window.
func IsNightOpsActive() bool {
	cfg := Config
	if cfg == nil || !cfg.NightOpsEnabled {
		return false
	}
	start := cfg.NightOpsStartHour
	end := cfg.NightOpsEndHour
	if start < 0 || start > 23 {
		start = 0
	}
	if end < 0 || end > 23 {
		end = 6
	}
	nowHour := time.Now().Hour()
	if start == end {
		return true
	}
	if start < end {
		return nowHour >= start && nowHour < end
	}
	return nowHour >= start || nowHour < end
}
