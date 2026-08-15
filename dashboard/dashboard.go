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
	"github.com/charmbracelet/bubbles/viewport"
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
	rc       *redis.Client
	spinner  spinner.Model
	table    table.Model
	viewport viewport.Model
	info     map[string]string
	err      error
	width    int
	height   int
	ready    bool
}

// New creates a new dashboard Model for the given *redis.Client.
func New(rc *redis.Client) Model {
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

func fetchInfo(rc *redis.Client) tea.Cmd {
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
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		if !m.ready {
			m.viewport = viewport.New(msg.Width, msg.Height-6)
			m.ready = true
		} else {
			m.viewport.Width = msg.Width
			m.viewport.Height = msg.Height - 6
		}

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		cmds = append(cmds, cmd)

	case tickMsg:
		cmds = append(cmds, fetchInfo(m.rc), tickCmd())

	case infoMsg:
		m.err = nil
		m.info = parseInfo(string(msg))
		m.table.SetRows(buildRows(m.info))

	case errMsg:
		m.err = msg.err
	}

	var tableCmd, vpCmd tea.Cmd
	m.table, tableCmd = m.table.Update(msg)
	m.viewport, vpCmd = m.viewport.Update(msg)
	cmds = append(cmds, tableCmd, vpCmd)

	return m, tea.Batch(cmds...)
}

// View implements tea.Model.
func (m Model) View() string {
	if !m.ready {
		return fmt.Sprintf("\n  %s Initialising…", m.spinner.View())
	}

	header := titleStyle.Render("redi.go Dashboard") +
		"  " + m.spinner.View()

	var body string
	if m.err != nil {
		body = errorStyle.Render(fmt.Sprintf("Error: %v", m.err))
	} else {
		body = m.table.View()
	}

	footer := footerStyle.Render("↑/↓ scroll • q quit")

	return lipgloss.JoinVertical(lipgloss.Left,
		header,
		headerStyle.Render(strings.Repeat("─", m.width)),
		body,
		headerStyle.Render(strings.Repeat("─", m.width)),
		footer,
	)
}

// Run starts the Bubble Tea program and blocks until the user quits.
func Run(rc *redis.Client) error {
	p := tea.NewProgram(New(rc), tea.WithAltScreen())
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
