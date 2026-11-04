package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/workspacekit"
)

const (
	// pullRequestSuggestionFile is the control file in which the agent
	// may suggest the title and body of the issue's pull request.
	pullRequestSuggestionFile = "pr.json"

	// pullRequestSuggestionMaxBytes caps how much of the suggestion
	// file Sortie reads. A larger file is ignored whole.
	pullRequestSuggestionMaxBytes = 64 << 10

	// pullRequestTitleMaxRunes caps a pull request title in characters.
	pullRequestTitleMaxRunes = 256
)

// pullRequestText is the title and body of a pull request Sortie opens.
type pullRequestText struct {
	Title string
	Body  string
}

// pullRequestTextFor returns the title and body of the pull request for
// issue: those the agent suggests in the workspace's .sortie/pr.json,
// or, when the file is absent or invalid, the title
// "<identifier>: <issue title>" and a body that links the issue.
//
// The file is agent-authored text. It is read with the link refusal of
// every control file and at most 64 KiB; a larger file is invalid. It is
// valid when it holds a JSON object whose title and body are strings
// and whose title is not blank once cleaned. Control characters other
// than newline and tab are removed from both, and a title is cut to 256
// characters with its newlines and tabs made spaces. An invalid file is
// logged at warn level; an absent one is not.
func pullRequestTextFor(workspacePath string, issue domain.Issue, logger *slog.Logger) pullRequestText {
	if text, ok := readPullRequestSuggestion(workspacePath, logger); ok {
		return text
	}
	return defaultPullRequestText(issue)
}

// pullRequestSuggestion returns the bytes of the workspace's
// .sortie/pr.json, nil when there is none or it cannot be read.
func pullRequestSuggestion(workspacePath string) []byte {
	data, err := workspacekit.ReadSortieFile(workspacePath, pullRequestSuggestionFile, pullRequestSuggestionMaxBytes)
	if err != nil || len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	return data
}

// readPullRequestSuggestion reads the agent's suggestion and reports
// whether it is valid.
func readPullRequestSuggestion(workspacePath string, logger *slog.Logger) (pullRequestText, bool) {
	data, err := workspacekit.ReadSortieFile(workspacePath, pullRequestSuggestionFile, pullRequestSuggestionMaxBytes)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			logger.Warn("ignoring the pull request suggestion in .sortie/pr.json",
				slog.String("workspace", workspacePath), slog.Any("error", err))
		}
		return pullRequestText{}, false
	}
	var suggestion struct {
		Title *string `json:"title"`
		Body  *string `json:"body"`
	}
	if err := json.Unmarshal(data, &suggestion); err != nil || suggestion.Title == nil || suggestion.Body == nil {
		if err == nil {
			err = errors.New("want a JSON object with string title and body")
		}
		logger.Warn("ignoring the malformed pull request suggestion in .sortie/pr.json",
			slog.String("workspace", workspacePath), slog.Any("error", err))
		return pullRequestText{}, false
	}
	title := cleanPullRequestTitle(*suggestion.Title)
	if title == "" {
		logger.Warn("ignoring the pull request suggestion in .sortie/pr.json: its title is blank",
			slog.String("workspace", workspacePath))
		return pullRequestText{}, false
	}
	return pullRequestText{Title: title, Body: stripControl(*suggestion.Body)}, true
}

// defaultPullRequestText returns the title and body of the pull request
// for issue when the agent suggests none.
func defaultPullRequestText(issue domain.Issue) pullRequestText {
	identifier := stripControl(issue.Identifier)
	title := identifier
	if t := stripControl(issue.Title); strings.TrimSpace(t) != "" {
		title += ": " + t
	}
	body := "Opened by Sortie for " + identifier + "."
	if url := strings.TrimSpace(stripControl(issue.URL)); url != "" {
		body += "\n\nIssue: " + url
	}
	return pullRequestText{Title: cleanPullRequestTitle(title), Body: body}
}

// cleanPullRequestTitle removes control characters from s, turns its
// newlines and tabs into spaces, trims it, and cuts it to the title
// limit.
func cleanPullRequestTitle(s string) string {
	s = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' {
			return ' '
		}
		return r
	}, stripControl(s))
	s = strings.TrimSpace(s)
	if utf8.RuneCountInString(s) > pullRequestTitleMaxRunes {
		s = strings.TrimSpace(string([]rune(s)[:pullRequestTitleMaxRunes]))
	}
	return s
}

// stripControl removes every control character from s except newline
// and tab.
func stripControl(s string) string {
	return strings.Map(func(r rune) rune {
		if r != '\n' && r != '\t' && unicode.IsControl(r) {
			return -1
		}
		return r
	}, s)
}

// pullRequestTarget is where the pull request of an exported issue
// branch is opened.
type pullRequestTarget struct {
	// Project is the repository's owner/repo path.
	Project string

	// Base is the branch the pull request merges into.
	Base string

	// Opener returns the adapter that opens it.
	Opener func() (domain.PullRequestOpener, error)
}

// ensurePullRequest ensures the pull request from branch to the target's
// base for issue and records it in the workspace's SCM record.
func ensurePullRequest(ctx context.Context, target pullRequestTarget, workspacePath, branch string, issue domain.Issue, logger *slog.Logger) error {
	owner, repo, ok := splitProject(target.Project)
	if !ok {
		return fmt.Errorf("repository project %q is not an owner/repo path", target.Project)
	}
	if target.Opener == nil {
		return errors.New("no SCM adapter can open the pull request")
	}
	opener, err := target.Opener()
	if err != nil {
		return err
	}
	text := pullRequestTextFor(workspacePath, issue, logger)
	pr, err := opener.EnsurePullRequest(ctx, owner, repo, branch, target.Base, text.Title, text.Body)
	if err != nil {
		return fmt.Errorf("ensure the pull request into %s: %w", target.Base, err)
	}
	if pr.Number <= 0 {
		return fmt.Errorf("ensure the pull request into %s: the provider returned pull request number %d", target.Base, pr.Number)
	}
	logger.InfoContext(ctx, "pull request ensured for the issue branch",
		slog.String("branch", branch), slog.Int("pr_number", pr.Number),
		slog.String("url", pr.URL), slog.Bool("created", pr.Created))
	if err := updateSCMRecord(workspacePath, scmRecordUpdate{PRNumber: pr.Number, Project: target.Project}, logger); err != nil {
		return fmt.Errorf("record pull request %d: %w", pr.Number, err)
	}
	return nil
}

// pullRequestTargetFor returns the pull request target of cfg, whose
// opener openers returns, or nil when cfg's repository names no
// provider.
func pullRequestTargetFor(cfg config.ServiceConfig, openers PullRequestOpenerFunc) *pullRequestTarget {
	if RepositoryProvider(cfg.Environment) == "" {
		return nil
	}
	repo := cfg.Environment.RepositorySet().Repository
	target := &pullRequestTarget{Project: repo.Project, Base: repo.Base}
	if openers != nil {
		target.Opener = func() (domain.PullRequestOpener, error) { return openers(cfg) }
	}
	return target
}
