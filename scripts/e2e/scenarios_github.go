package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jixoai/keel-board/protocol"
)

// scenarioGitHub proves the GitHub adapter's REST side and webhook side against
// the fake api.github.com: board → comment/edit/delete/reaction, and
// comment webhook → board.
func (e *e2eEnv) scenarioGitHub() {
	sc := e.report.Scenario("github")
	var root protocol.MessageView

	if err := sc.Step("attach board thread 到 GitHub PR（thread target，不自动发帖）",
		"POST /sync/threads {key: github, target: acme/www:123}",
		"200，kind=thread，fake GitHub 无写入", func(step *Step) error {
			e.fake.ResetGitHub()
			created, err := e.postMessage(step, "github", "github board root")
			if err != nil {
				return err
			}
			root = created
			view, err := e.attachThread(step, "github", root.ID, "acme/www:123")
			if err != nil {
				return err
			}
			step.CheckEQ("attach kind", "thread", view.Kind)
			step.CheckEQ("attach target", "github:acme/www:123", view.Target)
			time.Sleep(250 * time.Millisecond)
			step.CheckEQ("attach thread target 不发帖", 0, len(e.fake.GitHubWrites()))
			return nil
		}); err != nil {
		return
	}

	var outbound protocol.MessageView
	if err := sc.Step("出站：board reply → GitHub issue comment（带隐藏 marker）",
		"fake GitHub POST /repos/acme/www/issues/123/comments",
		"201，body 含文本与 keel-board marker", func(step *Step) error {
			e.fake.ResetGitHub()
			created, err := e.replyMessage(step, root.ID, "github outbound comment")
			if err != nil {
				return err
			}
			outbound = created
			err = waitFor(10*time.Second, func() error {
				if len(e.fake.GitHubWrites()) == 0 {
					return fmt.Errorf("fake GitHub 还未收到评论")
				}
				return nil
			})
			if err != nil {
				return err
			}
			writes := e.fake.GitHubWrites()
			first := writes[0]
			step.CheckEQ("method", http.MethodPost, first.Method)
			step.CheckEQ("path", "/repos/acme/www/issues/123/comments", first.Path)
			step.CheckEQ("status", http.StatusCreated, first.Status)
			step.CheckContains("body 文本", "github outbound comment", first.Body)
			step.CheckContains("隐藏 marker", "keel-board:sync key=github", first.Body)
			step.Actual = fmt.Sprintf("comment body=%s", trimBody(first.Body))
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("出站变更：edit → PATCH，reaction → POST reactions，delete → DELETE",
		"board edit/react/delete 已投递的 reply", "fake GitHub 收到对应 REST 调用且 body/emoji 正确", func(step *Step) error {
			e.fake.ResetGitHub()
			if err := e.editMessage(step, outbound.ID, "github outbound edited", "local:user:dev"); err != nil {
				return err
			}
			err := waitFor(10*time.Second, func() error {
				for _, write := range e.fake.GitHubWrites() {
					if write.Method == http.MethodPatch && strings.HasPrefix(write.Path, "/repos/acme/www/issues/comments/") {
						return nil
					}
				}
				return fmt.Errorf("没有 PATCH comment")
			})
			if err != nil {
				return err
			}
			var patch githubWrite
			for _, write := range e.fake.GitHubWrites() {
				if write.Method == http.MethodPatch {
					patch = write
				}
			}
			step.CheckContains("PATCH body", "github outbound edited", patch.Body)
			if err := e.reactMessage(step, outbound.ID, "thumbs_up", false, "local:user:dev"); err != nil {
				return err
			}
			err = waitFor(10*time.Second, func() error {
				for _, write := range e.fake.GitHubWrites() {
					if write.Method == http.MethodPost && strings.HasSuffix(write.Path, "/reactions") {
						return nil
					}
				}
				return fmt.Errorf("没有 reactions POST")
			})
			if err != nil {
				return err
			}
			var reaction githubWrite
			for _, write := range e.fake.GitHubWrites() {
				if write.Method == http.MethodPost && strings.HasSuffix(write.Path, "/reactions") {
					reaction = write
				}
			}
			step.CheckContains("reaction content", `"content":"+1"`, strings.ReplaceAll(reaction.Body, " ", ""))
			if err := e.deleteMessage(step, outbound.ID, "local:user:dev"); err != nil {
				return err
			}
			err = waitFor(10*time.Second, func() error {
				for _, write := range e.fake.GitHubWrites() {
					if write.Method == http.MethodDelete && strings.HasPrefix(write.Path, "/repos/acme/www/issues/comments/") {
						return nil
					}
				}
				return fmt.Errorf("没有 DELETE comment")
			})
			if err != nil {
				return err
			}
			step.CheckEQ("GitHub 写调用数", 3, len(e.fake.GitHubWrites()))
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("入站：GitHub issue_comment → board 线程回复",
		"POST /webhooks/github (X-GitHub-Event=issue_comment, signed)",
		"board 线程出现 from=github 的回复", func(step *Step) error {
			e.fake.ResetGitHub()
			body := githubCommentBody("created", 9200, "github inbound comment", "")
			status, _, err := e.webhook(step, "github comment created", "github", body, githubSignedHeaders(body, "issue_comment", "gh-delivery-1"))
			if err != nil {
				return err
			}
			step.CheckEQ("webhook 状态", http.StatusOK, status)
			err = waitFor(10*time.Second, func() error {
				thread, findErr := e.getThread(step, root.ID)
				if findErr != nil {
					return findErr
				}
				if _, ok := findReply(thread, "github inbound comment"); !ok {
					return fmt.Errorf("回复尚未出现")
				}
				return nil
			})
			if err != nil {
				return err
			}
			thread, err := e.getThread(step, root.ID)
			if err != nil {
				return err
			}
			reply, _ := findReply(thread, "github inbound comment")
			step.CheckBool("from=github", true, reply.From != nil && *reply.From == "github")
			step.CheckEQ("未回调 GitHub REST", 0, len(e.fake.GitHubWrites()))
			step.Actual = fmt.Sprintf("reply=%s", reply.ID)
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("入站 edit/delete：issue_comment edited/deleted → board 更新/tombstone",
		"POST signed issue_comment action=edited, then action=deleted",
		"board 文本更新，随后回复删除", func(step *Step) error {
			edited := githubCommentBody("edited", 9200, "github inbound edited", "2026-01-02T04:05:06Z")
			status, _, err := e.webhook(step, "github comment edited", "github", edited, githubSignedHeaders(edited, "issue_comment", "gh-delivery-2"))
			if err != nil {
				return err
			}
			step.CheckEQ("edit webhook 状态", http.StatusOK, status)
			err = waitFor(10*time.Second, func() error {
				thread, findErr := e.getThread(step, root.ID)
				if findErr != nil {
					return findErr
				}
				if _, ok := findReply(thread, "github inbound edited"); !ok {
					return fmt.Errorf("未看到编辑后的文本")
				}
				return nil
			})
			if err != nil {
				return err
			}
			deleted := githubCommentBody("deleted", 9200, "", "")
			status, _, err = e.webhook(step, "github comment deleted", "github", deleted, githubSignedHeaders(deleted, "issue_comment", "gh-delivery-3"))
			if err != nil {
				return err
			}
			step.CheckEQ("delete webhook 状态", http.StatusOK, status)
			err = waitFor(10*time.Second, func() error {
				thread, findErr := e.getThread(step, root.ID)
				if findErr != nil {
					return findErr
				}
				for _, candidate := range thread.Replies {
					if candidate.Deleted {
						return nil
					}
				}
				return fmt.Errorf("没有 tombstone")
			})
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("重复投递与 comment 身份去重（delivery vs comment）",
		"同一 delivery 重投；再换一个 delivery 投同一条 comment", "两种重投都只有 1 条回复", func(step *Step) error {
			body := githubCommentBody("created", 9300, "github duplicate comment", "")
			sameDelivery := githubSignedHeaders(body, "issue_comment", "gh-delivery-dup")
			status, _, err := e.webhook(step, "same delivery first", "github", body, sameDelivery)
			if err != nil {
				return err
			}
			step.CheckEQ("第一次状态", http.StatusOK, status)
			status, _, err = e.webhook(step, "same delivery second", "github", body, sameDelivery)
			if err != nil {
				return err
			}
			step.CheckEQ("同 delivery 重投状态", http.StatusOK, status)
			time.Sleep(300 * time.Millisecond)
			afterSameDelivery, err := e.getThread(step, root.ID)
			if err != nil {
				return err
			}
			step.CheckEQ("同 delivery 重投只 1 条回复", 1, countReplies(afterSameDelivery, "github duplicate comment"))
			otherDelivery := githubSignedHeaders(body, "issue_comment", "gh-delivery-dup-different")
			status, _, err = e.webhook(step, "same comment, new delivery", "github", body, otherDelivery)
			if err != nil {
				return err
			}
			step.CheckEQ("换 delivery 重投状态", http.StatusOK, status)
			time.Sleep(400 * time.Millisecond)
			thread, err := e.getThread(step, root.ID)
			if err != nil {
				return err
			}
			step.CheckEQ("匹配回复数（同 comment 换 delivery 仍去重）", 1, countReplies(thread, "github duplicate comment"))
			step.Note("GitHub comment 的去重键是 comment ID（MessageDedupe），不是 X-GitHub-Delivery；同 delivery 重投与同 comment 换 delivery 都只入库一次。")
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("GitHub 签名错误：401 且不入库",
		"POST /webhooks/github with bad signature", "401，无匹配回复", func(step *Step) error {
			body := githubCommentBody("created", 9400, "github unsigned comment", "")
			headers := githubSignedHeaders(body, "issue_comment", "gh-delivery-bad")
			headers["X-Hub-Signature-256"] = "sha256=deadbeef"
			status, _, err := e.webhook(step, "bad github signature", "github", body, headers)
			if err != nil {
				return err
			}
			step.CheckEQ("webhook 状态", http.StatusUnauthorized, status)
			time.Sleep(250 * time.Millisecond)
			thread, err := e.getThread(step, root.ID)
			if err != nil {
				return err
			}
			step.CheckEQ("未入库", 0, countReplies(thread, "github unsigned comment"))
			return nil
		}); err != nil {
		return
	}
}

// githubCommentBody builds the subset of an issue_comment payload the adapter
// reads. The issue is a PR (pull_request is present), so its channel is
// acme/www:pulls and the thread is acme/www:123.
func githubCommentBody(action string, id int64, text, updated string) []byte {
	issue := map[string]any{
		"number": 123, "title": "checkout retry", "body": "PR body", "state": "open",
		"html_url":     "https://github.com/acme/www/pull/123",
		"pull_request": map[string]any{"url": "https://api.github.com/repos/acme/www/pulls/123"},
		"user":         map[string]any{"login": "ada", "id": 1001, "type": "User"},
		"labels":       []any{}, "assignees": []any{},
	}
	comment := map[string]any{
		"id": id, "body": text,
		"user":       map[string]any{"login": "ada", "id": 1001, "type": "User"},
		"created_at": "2026-01-02T03:04:05Z",
		"updated_at": firstNonEmptyString(updated, "2026-01-02T03:04:05Z"),
	}
	payload := map[string]any{
		"action": action, "issue": issue, "comment": comment,
		"repository": map[string]any{
			"id": 1001, "full_name": "acme/www", "name": "www", "private": false,
			"owner": map[string]any{"login": "acme"},
		},
		"sender": map[string]any{"login": "ada", "id": 1001, "type": "User"},
	}
	encoded, _ := json.Marshal(payload)
	return encoded
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
