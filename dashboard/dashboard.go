// Package dashboard provides a TUI monitoring dashboard for redi.go clients.
//
// It is built with github.com/charmbracelet/bubbletea and
// github.com/charmbracelet/bubbles and displays live Redis server
// information (INFO stats) and a spinning activity indicator.
//
// Usage:
//
//	d := dashboard.New(redisClient)
//	if err := d.Run(); err != nil {
//	    log.Fatal(err)
//	}
package dashboard

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/redis/go-redis/v9"
)

// refreshInterval controls how often the dashboard polls Redis INFO.
const refreshInterval = 2 * time.Second

// tickMsg is sent every refreshInterval to trigger a data refresh.
type tickMsg time.Time

// infoMsg carries the freshly fetched Redis INFO string.
type infoMsg string

// errMsg wraps a fetch error.
type errMsg struct{ err error }

// styles holds pre-built lipgloss styles.
var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("205")).
			Padding(0, 1)

	headerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("39"))

	footerStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("240")).
			Italic(true)

	errorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("196")).
			Bold(true)
)

// Model is the Bubble Tea model for the dashboard.
type Model struct {
	rc      redis.UniversalClient
	spinner spinner.Model
	table   table.Model
	info    map[string]string
	err     error
	width   int
	height  int
	ready   bool

	tab      int // 0 = INFO, 1 = Locks, 2 = Rate limiters
	locks    []table.Row
	limiters []table.Row
	// lockPatterns narrows the Locks tab scan (Redisson lock keys are the
	// raw name, so a pattern is the only way to scope the keyspace).
	lockPatterns []string
}

// New creates a new dashboard Model for the given redis.UniversalClient.
func New(rc redis.UniversalClient) Model {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))

	columns := []table.Column{
		{Title: "Metric", Width: 30},
		{Title: "Value", Width: 40},
	}
	t := table.New(
		table.WithColumns(columns),
		table.WithFocused(true),
		table.WithHeight(20),
	)
	ts := table.DefaultStyles()
	ts.Header = ts.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(true)
	ts.Selected = ts.Selected.
		Foreground(lipgloss.Color("229")).
		Background(lipgloss.Color("57")).
		Bold(false)
	t.SetStyles(ts)

	return Model{
		rc:      rc,
		spinner: s,
		table:   t,
	}
}

// Init implements tea.Model.
func (m Model) Init() tea.Cmd {
	return tea.Batch(
		m.spinner.Tick,
		tickCmd(),
	)
}

func tickCmd() tea.Cmd {
	return tea.Tick(refreshInterval, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func fetchInfo(rc redis.UniversalClient) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		raw, err := rc.Info(ctx, "all").Result()
		if err != nil {
			return errMsg{err: err}
		}
		return infoMsg(raw)
	}
}

// Update implements tea.Model.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "1", "left":
			m.tab = (m.tab + 2) % 3
			m.applyTab()
		case "2", "right", "3":
			m.tab = (m.tab + 1) % 3
			m.applyTab()
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		if !m.ready {
			m.ready = true
		}

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		cmds = append(cmds, cmd)

	case tickMsg:
		cmds = append(cmds, fetchInfo(m.rc), fetchLocks(m.rc, m.lockPatterns), fetchLimiters(m.rc), tickCmd())

	case infoMsg:
		m.err = nil
		m.info = parseInfo(string(msg))
		if m.tab == 0 {
			m.table.SetRows(buildRows(m.info))
		}

	case locksMsg:
		m.locks = buildLockRows(msg)

	case limitersMsg:
		m.limiters = buildLimiterRows(msg)

	case errMsg:
		m.err = msg.err
	}

	var tableCmd tea.Cmd
	m.table, tableCmd = m.table.Update(msg)
	cmds = append(cmds, tableCmd)

	return m, tea.Batch(cmds...)
}

// View implements tea.Model.
func (m Model) View() string {
	if !m.ready {
		return fmt.Sprintf("\n  %s Initialising…", m.spinner.View())
	}

	header := titleStyle.Render("redi.go Dashboard") +
		"  " + m.spinner.View() +
		"  " + tabBar(m.tab)

	var body string
	if m.err != nil {
		body = errorStyle.Render(fmt.Sprintf("Error: %v", m.err))
	} else {
		body = m.table.View()
	}

	footer := footerStyle.Render("1/2/3 or ←/→ switch tab • ↑/↓ scroll • q quit")

	return lipgloss.JoinVertical(lipgloss.Left,
		header,
		headerStyle.Render(strings.Repeat("─", m.width)),
		body,
		headerStyle.Render(strings.Repeat("─", m.width)),
		footer,
	)
}

// applyTab refreshes the table for the active tab.
func (m *Model) applyTab() {
	switch m.tab {
	case 0:
		m.table.SetRows(buildRows(m.info))
	case 1:
		m.table.SetRows(m.locks)
	case 2:
		m.table.SetRows(m.limiters)
	}
}

func tabBar(active int) string {
	tabs := []string{"INFO", "Locks", "Limiters"}
	for i := range tabs {
		if i == active {
			tabs[i] = activeTabStyle.Render("[" + tabs[i] + "]")
		} else {
			tabs[i] = tabStyle.Render(tabs[i])
		}
	}
	return strings.Join(tabs, " ")
}

var (
	tabStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	activeTabStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Bold(true)
)

// Run starts the Bubble Tea program and blocks until the user quits.
// The Locks tab scans the given lock-name glob patterns (default: "*").
func Run(rc redis.UniversalClient, lockPatterns ...string) error {
	if len(lockPatterns) == 0 {
		lockPatterns = []string{"*"}
	}
	m := New(rc)
	m.lockPatterns = lockPatterns
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}

// parseInfo parses the Redis INFO output into a map[section_key]value.
func parseInfo(raw string) map[string]string {
	result := make(map[string]string)
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if k, v, ok := strings.Cut(line, ":"); ok {
			result[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return result
}

// buildRows converts the INFO map to table rows, sorted by key.
func buildRows(info map[string]string) []table.Row {
	// Prioritised keys to show at the top.
	priority := []string{
		"redis_version",
		"uptime_in_seconds",
		"connected_clients",
		"used_memory_human",
		"used_memory_peak_human",
		"total_commands_processed",
		"instantaneous_ops_per_sec",
		"role",
		"rdb_last_bgsave_status",
		"aof_enabled",
	}

	seen := make(map[string]bool)
	rows := make([]table.Row, 0, len(info))

	for _, k := range priority {
		if v, ok := info[k]; ok {
			rows = append(rows, table.Row{k, v})
			seen[k] = true
		}
	}
	for k, v := range info {
		if !seen[k] {
			rows = append(rows, table.Row{k, v})
		}
	}
	return rows
}

// lockSample is one observed distributed lock (Redisson RLock wire format).
type lockSample struct {
	name    string
	holders string
	ttlMs   int64
}

// locksMsg carries the observed locks.
type locksMsg []lockSample

// limiterSample is one observed rate limiter config (HASH {name}).
type limiterSample struct {
	name     string
	rate     string
	interval string
	value    string
}

// limitersMsg carries the observed rate limiters.
type limitersMsg []limiterSample

// fetchLocks scans for lock HASHes matching the patterns and reports
// holders + TTL. Redisson lock layout: HASH with holder fields + optional
// "mode" field for read-write locks.
func fetchLocks(rc redis.UniversalClient, patterns []string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		var out []lockSample
		var cursor uint64
		for _, pattern := range patterns {
			for {
				keys, next, err := rc.Scan(ctx, cursor, pattern, 100).Result()
				if err != nil {
					return errMsg{err: err}
				}
				for _, key := range keys {
					fields, err := rc.HGetAll(ctx, key).Result()
					if err != nil || len(fields) == 0 {
						continue
					}
					holders := formatHolders(fields)
					if holders == "" {
						continue // not a lock-shaped hash
					}
					ttl, _ := rc.PTTL(ctx, key).Result()
					out = append(out, lockSample{
						name:    key,
						holders: holders,
						ttlMs:   ttl.Milliseconds(),
					})
				}
				cursor = next
				if cursor == 0 {
					break
				}
			}
		}
		return locksMsg(out)
	}
}

// formatHolders renders a lock hash's holder fields ("mode" and other
// non-holder fields skipped); "" when the hash holds no lock entries.
func formatHolders(fields map[string]string) string {
	var holders []string
	for f, cnt := range fields {
		if f == "mode" {
			continue
		}
		if cnt == "1" {
			holders = append(holders, f)
		} else {
			holders = append(holders, f+"×"+cnt)
		}
	}
	return strings.Join(holders, ", ")
}

// fetchLimiters scans for Redisson rate limiter config HASHes.
func fetchLimiters(rc redis.UniversalClient) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		var out []limiterSample
		var cursor uint64
		for {
			keys, next, err := rc.Scan(ctx, cursor, "{*}:value", 100).Result()
			if err != nil {
				return errMsg{err: err}
			}
			for _, valueKey := range keys {
				name := strings.TrimSuffix(strings.TrimPrefix(valueKey, "{"), "}:value")
				cfg, err := rc.HGetAll(ctx, name).Result()
				if err != nil || len(cfg) == 0 {
					continue
				}
				// Only actual limiter configs have a rate field.
				if _, ok := cfg["rate"]; !ok {
					continue
				}
				value, _ := rc.Get(ctx, valueKey).Result()
				out = append(out, limiterSample{
					name:     name,
					rate:     cfg["rate"],
					interval: cfg["interval"],
					value:    value,
				})
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		return limitersMsg(out)
	}
}

func buildLockRows(locks []lockSample) []table.Row {
	rows := make([]table.Row, 0, len(locks))
	if len(locks) == 0 {
		return []table.Row{{"(no locks matched scan patterns)", ""}}
	}
	for _, l := range locks {
		ttl := "no expiry"
		if l.ttlMs > 0 {
			ttl = fmt.Sprintf("%dms", l.ttlMs)
		}
		rows = append(rows, table.Row{l.name, l.holders + " • TTL " + ttl})
	}
	return rows
}

func buildLimiterRows(limiters []limiterSample) []table.Row {
	rows := make([]table.Row, 0, len(limiters))
	if len(limiters) == 0 {
		return []table.Row{{"(no rate limiters found)", ""}}
	}
	for _, l := range limiters {
		rows = append(rows, table.Row{
			l.name,
			fmt.Sprintf("rate %s / %sms • available %s", l.rate, l.interval, l.value),
		})
	}
	return rows
}
