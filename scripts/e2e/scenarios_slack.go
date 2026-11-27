package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jixoai/keel-board/protocol"
	boardsync "github.com/jixoai/keel-board/sync"
)

// Slack fixture identities. They are stable so a webhook redelivery is a real
// dedupe test rather than a new message.
const (
	slackRootTS    = "1700000100.000100"
	slackReplyTS   = "1700000100.000200"
	slackFileTS    = "1700000200.000100"
	slackRetryRoot = "1700000300.000100"
	slackRetryTS   = "1700000300.000200"
	slackFailRoot  = "1700000400.000100"
	slackRestartRT = "1700000500.000100"
)

// scenarioSlackMirror proves platform webhook → inbox → board → outbound →
// fake Slack, including dedupe, echo suppression, inbound file metadata and
// signature refusal.
func (e *e2eEnv) scenarioSlackMirror() {
	sc := e.report.Scenario("slack-mirror")
	var root protocol.MessageView

	if err := sc.Step("入站 root：Slack webhook → inbox → board",
		"POST /webhooks/slack (signed message event C_SUPPORT)",
		"200，board support 频道出现 root，from=slack，无回声", func(step *Step) error {
			e.fake.ResetSlack()
			body := slackMessageEvent("Ev-root-100", "C_SUPPORT", "U_ALICE", "slack root inbound", slackRootTS, "", "", nil)
			status, response, err := e.webhook(step, "signed Slack root", "slack", body, slackSignedHeaders(body))
			if err != nil {
				return err
			}
			step.CheckEQ("webhook 状态", http.StatusOK, status)
			step.Check("响应可为空", len(response) < 64, "短响应", trimBody(string(response)))
			err = waitFor(10*time.Second, func() error {
				thread, findErr := e.findThread(step, "support", "slack root inbound")
				if findErr != nil {
					return findErr
				}
				root = thread.Message
				if len(thread.Replies) != 0 {
					return fmt.Errorf("root 还带着 %d 条回复", len(thread.Replies))
				}
				return nil
			})
			if err != nil {
				return err
			}
			step.CheckContains("root 文本", "slack root inbound", root.Text)
			step.CheckBool("from=slack", true, root.From != nil && *root.From == "slack")
			origin, _ := root.Origin.(map[string]any)
			step.CheckEQ("origin.message", slackRootTS, fmt.Sprint(origin["message"]))
			step.CheckEQ("origin.thread", "slack:C_SUPPORT:"+slackRootTS, fmt.Sprint(origin["thread"]))
			var slackInbox int
			err = waitFor(10*time.Second, func() error {
				statusBody, statusErr := e.syncStatus(step)
				if statusErr != nil {
					return statusErr
				}
				for _, key := range statusBody.Keys {
					if key.Key == "slack" {
						slackInbox = key.Inbox.Done
					}
				}
				if slackInbox < 1 {
					return fmt.Errorf("inbox done=%d", slackInbox)
				}
				return nil
			})
			if err != nil {
				return err
			}
			step.Check("slack inbox done ≥ 1", slackInbox >= 1, ">=1", fmt.Sprint(slackInbox))
			step.CheckEQ("未向 Slack 回发 root（CameFrom 跳过）", 0, len(e.fake.SlackPosts()))
			step.Actual = fmt.Sprintf("root=%s，inbox done=%d，fake posts=0", root.ID, slackInbox)
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("重复投递：同一 event_id 只导入一次",
		"同一 signed Slack root webhook 再投一次", "200，不新增 board 根", func(step *Step) error {
			body := slackMessageEvent("Ev-root-100", "C_SUPPORT", "U_ALICE", "slack root inbound", slackRootTS, "", "", nil)
			status, _, err := e.webhook(step, "duplicate root webhook", "slack", body, slackSignedHeaders(body))
			if err != nil {
				return err
			}
			step.CheckEQ("重复 webhook 状态", http.StatusOK, status)
			time.Sleep(200 * time.Millisecond)
			page, err := e.threads(step, "support", "slack root inbound")
			if err != nil {
				return err
			}
			step.CheckEQ("匹配线程数", 1, len(page.Threads))
			thread, err := e.getThread(step, root.ID)
			if err != nil {
				return err
			}
			step.CheckEQ("回复数仍为 0", 0, len(thread.Replies))
			return nil
		}); err != nil {
		return
	}

	var reply protocol.MessageView
	if err := sc.Step("入站 reply → board 线程",
		"POST /webhooks/slack (thread_ts=root ts)",
		"board 线程出现 1 条 from=slack 的回复", func(step *Step) error {
			body := slackMessageEvent("Ev-reply-100", "C_SUPPORT", "U_BOB", "slack reply inbound", slackReplyTS, slackRootTS, "", nil)
			status, _, err := e.webhook(step, "signed Slack reply", "slack", body, slackSignedHeaders(body))
			if err != nil {
				return err
			}
			step.CheckEQ("webhook 状态", http.StatusOK, status)
			err = waitFor(10*time.Second, func() error {
				thread, findErr := e.getThread(step, root.ID)
				if findErr != nil {
					return findErr
				}
				found, ok := findReply(thread, "slack reply inbound")
				if !ok {
					return fmt.Errorf("回复尚未出现")
				}
				reply = found
				return nil
			})
			if err != nil {
				return err
			}
			step.CheckBool("reply from=slack", true, reply.From != nil && *reply.From == "slack")
			step.Actual = fmt.Sprintf("reply=%s", reply.ID)
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("回声抑制：bot 自己的消息不入库",
		"POST signed Slack message，user=UBOT（BotUserID）", "200 但 board 无匹配线程", func(step *Step) error {
			body := slackMessageEvent("Ev-own-100", "C_SUPPORT", "UBOT", "bot echo must be dropped", "1700000100.000900", "", "", nil)
			status, _, err := e.webhook(step, "own bot message", "slack", body, slackSignedHeaders(body))
			if err != nil {
				return err
			}
			step.CheckEQ("webhook 状态", http.StatusOK, status)
			time.Sleep(250 * time.Millisecond)
			page, err := e.threads(step, "support", "bot echo must be dropped")
			if err != nil {
				return err
			}
			step.CheckEQ("匹配线程数", 0, len(page.Threads))
			return nil
		}); err != nil {
		return
	}

	var fileRoot protocol.MessageView
	if err := sc.Step("入站文件事件：元数据保留，非白名单 URL 被剥离且不抓取",
		"POST signed Slack file_share（files.slack.com + 169.254.169.254）",
		"board 附件 2 个元数据（合法 URL 保留、非白名单 URL 不落库），无抓取、无回发", func(step *Step) error {
			e.fake.ResetCalls()
			files := []any{
				map[string]any{
					"id": "F_SAFE", "name": "run.log", "title": "Run log", "mimetype": "text/plain",
					"filetype": "text", "size": 2048,
					"url_private_download": "https://files.slack.com/files-pri/T-F/run.log",
					"permalink":            "https://acme.slack.com/files/U/F_SAFE/run.log",
				},
				map[string]any{
					"id": "F_UNSAFE", "name": "unsafe.bin", "mimetype": "application/octet-stream",
					"size": 4096, "url_private_download": "https://169.254.169.254/latest/meta-data/",
				},
			}
			body := slackMessageEvent("Ev-file-100", "C_SUPPORT", "U_ALICE", "file share inbound", slackFileTS, "", "file_share", files)
			status, _, err := e.webhook(step, "signed file_share", "slack", body, slackSignedHeaders(body))
			if err != nil {
				return err
			}
			step.CheckEQ("webhook 状态", http.StatusOK, status)
			err = waitFor(10*time.Second, func() error {
				thread, findErr := e.findThread(step, "support", "file share inbound")
				if findErr != nil {
					return findErr
				}
				fileRoot = thread.Message
				return nil
			})
			if err != nil {
				return err
			}
			if len(fileRoot.Attachments) == 1 {
				step.CheckEQ("附件名", "run.log", fileRoot.Attachments[0].Name)
				step.CheckEQ("附件大小", int64(2048), fileRoot.Attachments[0].Size)
			}
			step.CheckEQ("附件数（元数据都保留；非白名单 URL 在 adapter/Host 被剥离）", 2, len(fileRoot.Attachments))
			// Parse the same payload through the real adapter once more and
			// assert the unsafe URL is cleared before the Host ever sees it.
			provider := e.harness.adapters["slack"]
			headers := slackSignedHeaders(body)
			webhook := &boardsync.WebhookRequest{Method: http.MethodPost, Header: http.Header{}, Body: body}
			for name, value := range headers {
				webhook.Header.Set(name, value)
			}
			intake, intakeErr := provider.Receive(e.ctx, "slack", webhook)
			if intakeErr != nil {
				return intakeErr
			}
			step.CheckEQ("adapter 归一化记录数", 1, len(intake.Records))
			if len(intake.Records) == 1 && len(intake.Records[0].Event.Files) == 2 {
				files := intake.Records[0].Event.Files
				step.CheckEQ("安全文件 URL 保留", "https://files.slack.com/files-pri/T-F/run.log", files[0].URL)
				step.CheckEQ("非白名单文件 URL 已清空", "", files[1].URL)
			}
			step.CheckEQ("未触发文件下载（Host 只存元数据）", 0, len(e.fake.CallsTo("files.slack.com", "/")))
			step.CheckEQ("未向 Slack 回发", 0, len(e.fake.SlackPosts()))
			step.Note("文件字节抓取是 SYNC.md 的 open item；本步骤只验证元数据/安全过滤。")
			step.Actual = fmt.Sprintf("attachments=%v", fileRoot.Attachments)
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("签名错误：401 且不进入 inbox",
		"POST /webhooks/slack，X-Slack-Signature=v0=deadbeef", "401，无 board 消息", func(step *Step) error {
			body := slackMessageEvent("Ev-bad-100", "C_SUPPORT", "U_ALICE", "unsigned must not import", "1700000100.000800", "", "", nil)
			status, _, err := e.webhook(step, "bad signature", "slack", body, slackBadSignatureHeaders(body))
			if err != nil {
				return err
			}
			step.CheckEQ("webhook 状态", http.StatusUnauthorized, status)
			time.Sleep(200 * time.Millisecond)
			page, err := e.threads(step, "support", "unsigned must not import")
			if err != nil {
				return err
			}
			step.CheckEQ("匹配线程数", 0, len(page.Threads))
			return nil
		}); err != nil {
		return
	}

	var outbound protocol.MessageView
	if err := sc.Step("出站：board reply → outbound → fake Slack",
		"POST /messages {re: root}，等待 fake chat.postMessage",
		"fake 收到 1 条 C_SUPPORT 线程内消息，board 无 undelivered", func(step *Step) error {
			e.fake.ResetSlack()
			created, err := e.replyMessage(step, root.ID, "board reply outbound")
			if err != nil {
				return err
			}
			outbound = created
			err = waitFor(10*time.Second, func() error {
				for _, post := range e.fake.SlackPosts() {
					if strings.Contains(post.Text, "board reply outbound") {
						return nil
					}
				}
				return fmt.Errorf("fake Slack 尚未收到")
			})
			if err != nil {
				return err
			}
			posts := e.fake.SlackPosts()
			var found *slackPost
			for index := range posts {
				if strings.Contains(posts[index].Text, "board reply outbound") {
					found = &posts[index]
					break
				}
			}
			if found == nil {
				return fmt.Errorf("没有匹配的 Slack post")
			}
			step.CheckEQ("Slack channel", "C_SUPPORT", found.Channel)
			step.CheckEQ("Slack thread_ts", slackRootTS, found.ThreadTS)
			step.CheckContains("Slack 文本", "board reply outbound", found.Text)
			step.CheckEQ("只发一次", 1, countSlackPosts(posts, "board reply outbound"))
			thread, err := e.getThread(step, root.ID)
			if err != nil {
				return err
			}
			step.CheckBool("board reply 无 undelivered", true, len(thread.Replies) > 0 && len(thread.Replies[len(thread.Replies)-1].Undelivered) == 0)
			var drained syncStatusResponse
			err = waitFor(10*time.Second, func() error {
				statusBody, statusErr := e.syncStatus(step)
				if statusErr != nil {
					return statusErr
				}
				drained = statusBody
				if statusBody.Outbox.Pending+statusBody.Outbox.Leased != 0 {
					return fmt.Errorf("outbox pending=%d leased=%d", statusBody.Outbox.Pending, statusBody.Outbox.Leased)
				}
				return nil
			})
			if err != nil {
				return err
			}
			step.CheckEQ("outbox pending", 0, drained.Outbox.Pending)
			step.CheckEQ("outbox leased", 0, drained.Outbox.Leased)
			step.Actual = fmt.Sprintf("slack ts=%s", found.TS)
			return nil
		}); err != nil {
		return
	}
	_ = outbound
}

// scenarioSlackChanges proves edit/delete/reaction in both directions.
func (e *e2eEnv) scenarioSlackChanges() {
	sc := e.report.Scenario("slack-changes")
	root, err := e.findThreadQuiet("support", "slack root inbound")
	if err == nil {
		if _, found := findReply(root, "slack reply inbound"); !found {
			err = fmt.Errorf("slack-mirror 的 reply 未导入")
		}
	}
	if err != nil {
		sc.Skip("Slack 变更场景", "edit/delete/reaction", "前置 slack-mirror 未完成："+err.Error())
		return
	}

	if err := sc.Step("入站 edit：Slack message_changed → board 文本更新",
		"POST signed message_changed (ts=root)", "board root 文本更新，fake 无 chat.update（不回发）", func(step *Step) error {
			e.fake.ResetSlack()
			event := map[string]any{
				"type": "message", "subtype": "message_changed", "channel": "C_SUPPORT",
				"message": map[string]any{
					"type": "message", "user": "U_ALICE", "text": "slack root edited inbound",
					"ts":     slackRootTS,
					"edited": map[string]any{"user": "U_ALICE", "ts": "1700000105.000000"},
				},
			}
			body := slackEnvelope("Ev-edit-100", event)
			status, _, err := e.webhook(step, "inbound edit", "slack", body, slackSignedHeaders(body))
			if err != nil {
				return err
			}
			step.CheckEQ("webhook 状态", http.StatusOK, status)
			err = waitFor(10*time.Second, func() error {
				thread, findErr := e.getThread(step, root.Message.ID)
				if findErr != nil {
					return findErr
				}
				if !strings.Contains(thread.Message.Text, "slack root edited inbound") {
					return fmt.Errorf("board 文本尚未更新：%q", thread.Message.Text)
				}
				return nil
			})
			if err != nil {
				return err
			}
			step.CheckEQ("无 outbound edit（CameFrom 跳过）", 0, len(e.fake.SlackEdits()))
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("入站 delete：Slack message_deleted → board tombstone",
		"POST signed message_deleted (ts=reply)", "board reply Deleted=true 且文本清空", func(step *Step) error {
			event := map[string]any{
				"type": "message", "subtype": "message_deleted", "channel": "C_SUPPORT",
				"deleted_ts": slackReplyTS,
				"previous_message": map[string]any{
					"type": "message", "user": "U_BOB", "text": "slack reply inbound",
					"ts": slackReplyTS, "thread_ts": slackRootTS,
				},
			}
			body := slackEnvelope("Ev-delete-100", event)
			status, _, err := e.webhook(step, "inbound delete", "slack", body, slackSignedHeaders(body))
			if err != nil {
				return err
			}
			step.CheckEQ("webhook 状态", http.StatusOK, status)
			err = waitFor(10*time.Second, func() error {
				thread, findErr := e.getThread(step, root.Message.ID)
				if findErr != nil {
					return findErr
				}
				// A tombstone keeps its ID but has empty text, so find it by Deleted.
				for _, reply := range thread.Replies {
					if reply.Deleted {
						if reply.Text != "" {
							return fmt.Errorf("tombstone 仍有文本：%q", reply.Text)
						}
						return nil
					}
				}
				return fmt.Errorf("找不到已删除的 reply")
			})
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("入站 reaction：add/remove → board reactions",
		"POST signed reaction_added / reaction_removed (ts=root)", "board reactions 先 +1 再清空，不回发", func(step *Step) error {
			e.fake.ResetSlack()
			added := map[string]any{
				"type": "reaction_added", "user": "U_BOB", "reaction": "thumbsup",
				"item":     map[string]any{"type": "message", "channel": "C_SUPPORT", "ts": slackRootTS},
				"event_ts": "1700000106.000000",
			}
			body := slackEnvelope("Ev-react-add-100", added)
			status, _, err := e.webhook(step, "inbound reaction add", "slack", body, slackSignedHeaders(body))
			if err != nil {
				return err
			}
			step.CheckEQ("add webhook 状态", http.StatusOK, status)
			err = waitFor(10*time.Second, func() error {
				thread, findErr := e.getThread(step, root.Message.ID)
				if findErr != nil {
					return findErr
				}
				if reactionCount(thread.Message.Reactions, "thumbs_up") != 1 {
					return fmt.Errorf("reactions=%v", thread.Message.Reactions)
				}
				return nil
			})
			if err != nil {
				return err
			}
			removed := map[string]any{
				"type": "reaction_removed", "user": "U_BOB", "reaction": "thumbsup",
				"item":     map[string]any{"type": "message", "channel": "C_SUPPORT", "ts": slackRootTS},
				"event_ts": "1700000107.000000",
			}
			body = slackEnvelope("Ev-react-del-100", removed)
			status, _, err = e.webhook(step, "inbound reaction remove", "slack", body, slackSignedHeaders(body))
			if err != nil {
				return err
			}
			step.CheckEQ("remove webhook 状态", http.StatusOK, status)
			err = waitFor(10*time.Second, func() error {
				thread, findErr := e.getThread(step, root.Message.ID)
				if findErr != nil {
					return findErr
				}
				if reactionCount(thread.Message.Reactions, "thumbs_up") != 0 {
					return fmt.Errorf("reactions=%v", thread.Message.Reactions)
				}
				return nil
			})
			if err != nil {
				return err
			}
			step.CheckEQ("无 outbound reaction（来源消息不回发）", 0, len(e.fake.SlackReactions()))
			return nil
		}); err != nil {
		return
	}

	var outbound protocol.MessageView
	if err := sc.Step("出站 edit/reaction/delete：board → fake Slack chat.update / reactions / chat.delete",
		"发 board reply → 编辑 → 加/取消反应 → 删除", "fake 依次收到 post、update、add、remove、delete 且 ts 一致", func(step *Step) error {
			e.fake.ResetSlack()
			created, err := e.replyMessage(step, root.Message.ID, "board outbound change target")
			if err != nil {
				return err
			}
			outbound = created
			var ts string
			err = waitFor(10*time.Second, func() error {
				for _, post := range e.fake.SlackPosts() {
					if strings.Contains(post.Text, "board outbound change target") {
						ts = post.TS
						return nil
					}
				}
				return fmt.Errorf("post 未出现")
			})
			if err != nil {
				return err
			}
			step.Check("post ts 非空", ts != "", "非空", ts)
			if err := e.editMessage(step, outbound.ID, "board outbound changed", "local:user:dev"); err != nil {
				return err
			}
			err = waitFor(10*time.Second, func() error {
				for _, edit := range e.fake.SlackEdits() {
					if edit.TS == ts && strings.Contains(edit.Text, "board outbound changed") {
						return nil
					}
				}
				return fmt.Errorf("chat.update 未出现")
			})
			if err != nil {
				return err
			}
			if err := e.reactMessage(step, outbound.ID, "thumbs_up", false, "local:user:dev"); err != nil {
				return err
			}
			err = waitFor(10*time.Second, func() error {
				for _, reaction := range e.fake.SlackReactions() {
					if reaction.TS == ts && reaction.Name == "thumbsup" && reaction.Added {
						return nil
					}
				}
				return fmt.Errorf("reactions.add 未出现")
			})
			if err != nil {
				return err
			}
			if err := e.reactMessage(step, outbound.ID, "thumbs_up", true, "local:user:dev"); err != nil {
				return err
			}
			err = waitFor(10*time.Second, func() error {
				for _, reaction := range e.fake.SlackReactions() {
					if reaction.TS == ts && reaction.Name == "thumbsup" && !reaction.Added {
						return nil
					}
				}
				return fmt.Errorf("reactions.remove 未出现")
			})
			if err != nil {
				return err
			}
			if err := e.deleteMessage(step, outbound.ID, "local:user:dev"); err != nil {
				return err
			}
			err = waitFor(10*time.Second, func() error {
				for _, deleted := range e.fake.SlackDeletes() {
					if deleted.TS == ts {
						return nil
					}
				}
				return fmt.Errorf("chat.delete 未出现")
			})
			if err != nil {
				return err
			}
			step.CheckEQ("edit 次数", 1, len(e.fake.SlackEdits()))
			step.CheckEQ("reaction 调用数", 2, len(e.fake.SlackReactions()))
			step.CheckEQ("delete 次数", 1, len(e.fake.SlackDeletes()))
			step.Actual = fmt.Sprintf("ts=%s", ts)
			return nil
		}); err != nil {
		return
	}
	_ = outbound
}

// scenarioSlackAttach proves attach channel root + backfill and future-only
// thread targets.
func (e *e2eEnv) scenarioSlackAttach() {
	sc := e.report.Scenario("slack-attach")

	if err := sc.Step("attach channel target：root + backfill（跳过已删除回复）",
		"board backfill root + 2 live replies + 1 deleted reply；POST /sync/threads target=C0BACKFILL",
		"fake Slack 依序收到 root、reply1、reply2，且无已删除回复", func(step *Step) error {
			e.fake.ResetSlack()
			root, err := e.postMessage(step, "backfill", "backfill root")
			if err != nil {
				return err
			}
			first, err := e.replyMessage(step, root.ID, "backfill reply one")
			if err != nil {
				return err
			}
			if _, err := e.replyMessage(step, root.ID, "backfill reply two"); err != nil {
				return err
			}
			deleted, err := e.replyMessage(step, root.ID, "backfill reply deleted")
			if err != nil {
				return err
			}
			if err := e.deleteMessage(step, deleted.ID, "local:user:dev"); err != nil {
				return err
			}
			// Let the board subscription drain the pre-attach events first: a
			// mapping created while a posted event is still queued would let the
			// subscription deliver that historical message outside the backfill.
			time.Sleep(600 * time.Millisecond)
			step.CheckEQ("attach 前无 outbound（StartChannels 只含 C_SUPPORT）", 0, len(e.fake.SlackPosts()))
			view, err := e.attachThread(step, "slack", root.ID, "C0BACKFILL")
			if err != nil {
				return err
			}
			step.CheckEQ("attach kind", "channel", view.Kind)
			step.CheckEQ("attach board thread", root.ID, view.BoardThread)
			err = waitFor(15*time.Second, func() error {
				posts := e.fake.SlackPosts()
				if len(posts) < 3 {
					return fmt.Errorf("posts=%d", len(posts))
				}
				return nil
			})
			if err != nil {
				return err
			}
			time.Sleep(300 * time.Millisecond) // allow any extra (duplicate) post to appear
			posts := e.fake.SlackPosts()
			for _, post := range posts {
				step.Note("slack post ts=%s thread=%s text=%q", post.TS, post.ThreadTS, post.Text)
			}
			step.CheckEQ("总 post 数（root + 2 live replies）", 3, len(posts))
			if len(posts) >= 3 {
				step.CheckContains("第一条是 root", "backfill root", posts[0].Text)
				step.CheckEQ("root 无 thread_ts", "", posts[0].ThreadTS)
				step.CheckContains("第二条是 reply one", "backfill reply one", posts[1].Text)
				step.CheckEQ("reply one 在线程内", posts[0].TS, posts[1].ThreadTS)
				step.CheckContains("第三条是 reply two", "backfill reply two", posts[2].Text)
				step.CheckEQ("reply two 在线程内", posts[0].TS, posts[2].ThreadTS)
				step.CheckEQ("已删除回复未回填", 0, countSlackPosts(posts, "backfill reply deleted"))
			}
			mappings, err := e.syncThreads(step, root.ID)
			if err != nil {
				return err
			}
			step.Check("mapping 已记录", len(mappings.Threads) >= 1, ">=1", fmt.Sprint(len(mappings.Threads)))
			step.CheckContains("mapping target", "C0BACKFILL", mustSample(mappings.Threads))
			step.Note("first reply ID=%s", first.ID)
			step.Actual = fmt.Sprintf("posts=%d", len(posts))
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("attach thread target：future-only",
		"board future root + 2 旧回复；attach C0FUTURE:<ts>；再发一条新回复",
		"attach 不回发旧消息；新回复只发一次", func(step *Step) error {
			e.fake.ResetSlack()
			root, err := e.postMessage(step, "future", "future root")
			if err != nil {
				return err
			}
			if _, err := e.replyMessage(step, root.ID, "future old reply one"); err != nil {
				return err
			}
			if _, err := e.replyMessage(step, root.ID, "future old reply two"); err != nil {
				return err
			}
			view, err := e.attachThread(step, "slack", root.ID, "C0FUTURE:1700009000.000100")
			if err != nil {
				return err
			}
			step.CheckEQ("attach kind", "thread", view.Kind)
			time.Sleep(400 * time.Millisecond)
			step.CheckEQ("旧消息未回发", 0, len(e.fake.SlackPosts()))
			if _, err := e.replyMessage(step, root.ID, "future after attach"); err != nil {
				return err
			}
			err = waitFor(10*time.Second, func() error {
				posts := e.fake.SlackPosts()
				if len(posts) != 1 {
					return fmt.Errorf("posts=%d", len(posts))
				}
				return nil
			})
			if err != nil {
				return err
			}
			posts := e.fake.SlackPosts()
			step.CheckContains("新回复文本", "future after attach", posts[0].Text)
			step.CheckEQ("新回复线程", "1700009000.000100", posts[0].ThreadTS)
			step.CheckEQ("旧回复未回发", 0, countSlackPosts(posts, "future old reply"))
			thread, err := e.getThread(step, root.ID)
			if err != nil {
				return err
			}
			step.CheckEQ("board 线程回复数", 3, len(thread.Replies))
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("future-only 线程的入站回复落到同一 board 线程",
		"POST signed Slack message（C0FUTURE thread_ts=attach 目标）", "board future 线程新增 1 条 from=slack 回复", func(step *Step) error {
			page, err := e.threads(step, "future", "future root")
			if err != nil {
				return err
			}
			if len(page.Threads) != 1 {
				return fmt.Errorf("future root 数=%d", len(page.Threads))
			}
			root := page.Threads[0].Message
			body := slackMessageEvent("Ev-future-100", "C0FUTURE", "U_CAROL", "future inbound reply", "1700009000.000200", "1700009000.000100", "", nil)
			status, _, err := e.webhook(step, "future inbound", "slack", body, slackSignedHeaders(body))
			if err != nil {
				return err
			}
			step.CheckEQ("webhook 状态", http.StatusOK, status)
			err = waitFor(10*time.Second, func() error {
				thread, findErr := e.getThread(step, root.ID)
				if findErr != nil {
					return findErr
				}
				if _, ok := findReply(thread, "future inbound reply"); !ok {
					return fmt.Errorf("入站回复尚未出现")
				}
				return nil
			})
			if err != nil {
				return err
			}
			step.CheckEQ("回发数（CameFrom 跳过来源线程）", 1, len(e.fake.SlackPosts()))
			return nil
		}); err != nil {
		return
	}
}

// scenarioSlackFailures proves transient retry, permanent dead-letter with
// manual retry, inbound retry and inbound dead-letter.
func (e *e2eEnv) scenarioSlackFailures() {
	sc := e.report.Scenario("slack-failures")

	if err := sc.Step("出站瞬时失败：500 后重试成功",
		"fake chat.postMessage 对 TRANSIENT-RETRY 首次返回 500", "同一文本最终只成功投递一次，无 undelivered", func(step *Step) error {
			e.fake.ResetSlack()
			body := slackMessageEvent("Ev-fail-root", "C_SUPPORT", "U_ALICE", "failure root inbound", slackFailRoot, "", "", nil)
			status, _, err := e.webhook(step, "failure root", "slack", body, slackSignedHeaders(body))
			if err != nil {
				return err
			}
			step.CheckEQ("failure root webhook 状态", http.StatusOK, status)
			var root protocol.MessageView
			err = waitFor(10*time.Second, func() error {
				thread, findErr := e.findThread(step, "support", "failure root inbound")
				if findErr != nil {
					return findErr
				}
				root = thread.Message
				return nil
			})
			if err != nil {
				return err
			}
			e.fake.SlackFailText("TRANSIENT-RETRY", 1)
			reply, err := e.replyMessage(step, root.ID, "TRANSIENT-RETRY board reply")
			if err != nil {
				return err
			}
			err = waitFor(15*time.Second, func() error {
				for _, post := range e.fake.SlackPosts() {
					if strings.Contains(post.Text, "TRANSIENT-RETRY") {
						return nil
					}
				}
				return fmt.Errorf("重试后仍未投递")
			})
			if err != nil {
				return err
			}
			attempts := len(e.fake.CallsTo("slack.com", "/api/chat.postMessage"))
			step.CheckEQ("chat.postMessage 尝试次数（1 fail + 1 success）", 2, attempts)
			step.CheckEQ("成功投递次数", 1, countSlackPosts(e.fake.SlackPosts(), "TRANSIENT-RETRY"))
			thread, err := e.getThread(step, root.ID)
			if err != nil {
				return err
			}
			for _, candidate := range thread.Replies {
				if candidate.ID == reply.ID {
					step.CheckBool("无 undelivered", true, len(candidate.Undelivered) == 0)
				}
			}
			step.CheckContains("服务端记录了重试", "sync delivery failed", e.harness.Logs())
			return nil
		}); err != nil {
		return
	}

	var deadMessage protocol.MessageView
	if err := sc.Step("出站永久失败：重试耗尽 → dead letter → 手动 retry 成功",
		"fake 对 DEAD-LETTER 始终 500；GET failures；POST /sync/outbox/{id}/retry",
		"failures 列出该 delivery（attempts≥3），undelivered 非空；retry 后投递且清理", func(step *Step) error {
			root, err := e.findThreadQuiet("support", "failure root inbound")
			if err != nil {
				return err
			}
			e.fake.ResetSlack()
			e.fake.SlackFailText("DEAD-LETTER", -1)
			reply, err := e.replyMessage(step, root.Message.ID, "DEAD-LETTER board reply")
			if err != nil {
				return err
			}
			deadMessage = reply
			var failure syncFailureResponse
			err = waitFor(20*time.Second, func() error {
				view, listErr := e.syncFailures(step, "slack")
				if listErr != nil {
					return listErr
				}
				for _, item := range view.Failures {
					if item.Message == reply.ID {
						failure = view
						return nil
					}
				}
				return fmt.Errorf("failures 里还没有 message %s：%s", reply.ID, mustSample(view.Failures))
			})
			if err != nil {
				return err
			}
			var deadDelivery string
			var attempts int
			for _, item := range failure.Failures {
				if item.Message == reply.ID {
					deadDelivery, attempts = item.Delivery, item.Attempts
					step.Check("error class 非空", item.Error != "", "非空", item.Error)
					step.CheckContains("target 指向 C_SUPPORT 线程", "C_SUPPORT", item.Target)
				}
			}
			step.Check("attempts ≥ 3", attempts >= 3, ">=3", fmt.Sprint(attempts))
			step.Check("delivery ID 非空", deadDelivery != "", "非空", deadDelivery)
			thread, err := e.getThread(step, root.Message.ID)
			if err != nil {
				return err
			}
			for _, candidate := range thread.Replies {
				if candidate.ID == reply.ID {
					step.Check("undelivered 非空", len(candidate.Undelivered) > 0, ">0", mustSample(candidate.Undelivered))
				}
			}
			e.fake.SlackClearFailures()
			status, body, err := e.api(step, "retry dead outbox", http.MethodPost, "/sync/outbox/"+deadDelivery+"/retry", nil)
			if err != nil {
				return err
			}
			step.CheckEQ("retry 状态", http.StatusOK, status)
			step.CheckContains("retry 响应", `"retried":true`, string(body))
			err = waitFor(15*time.Second, func() error {
				for _, post := range e.fake.SlackPosts() {
					if strings.Contains(post.Text, "DEAD-LETTER board reply") {
						return nil
					}
				}
				return fmt.Errorf("retry 后仍未投递")
			})
			if err != nil {
				return err
			}
			err = waitFor(10*time.Second, func() error {
				view, listErr := e.syncFailures(step, "slack")
				if listErr != nil {
					return listErr
				}
				if len(view.Failures) != 0 {
					return fmt.Errorf("failures 仍非空：%s", mustSample(view.Failures))
				}
				return nil
			})
			if err != nil {
				return err
			}
			thread, err = e.getThread(step, root.Message.ID)
			if err != nil {
				return err
			}
			for _, candidate := range thread.Replies {
				if candidate.ID == reply.ID {
					step.CheckEQ("retry 后 undelivered 清空", 0, len(candidate.Undelivered))
				}
			}
			step.Actual = fmt.Sprintf("delivery=%s attempts=%d", deadDelivery, attempts)
			return nil
		}); err != nil {
		return
	}
	_ = deadMessage

	if err := sc.Step("入站瞬时失败：reaction 先于 copy 到达 → 重试成功",
		"先发 reply 的 reaction webhook，再发 reply webhook", "board reply 最终带 thumbs_up，inbox 未 dead", func(step *Step) error {
			body := slackMessageEvent("Ev-retry-root", "C_SUPPORT", "U_ALICE", "retry root inbound", slackRetryRoot, "", "", nil)
			status, _, err := e.webhook(step, "retry root", "slack", body, slackSignedHeaders(body))
			if err != nil {
				return err
			}
			step.CheckEQ("root webhook 状态", http.StatusOK, status)
			var root protocol.MessageView
			err = waitFor(10*time.Second, func() error {
				thread, findErr := e.findThread(step, "support", "retry root inbound")
				if findErr != nil {
					return findErr
				}
				root = thread.Message
				return nil
			})
			if err != nil {
				return err
			}
			reaction := map[string]any{
				"type": "reaction_added", "user": "U_BOB", "reaction": "thumbsup",
				"item":     map[string]any{"type": "message", "channel": "C_SUPPORT", "ts": slackRetryTS},
				"event_ts": "1700000305.000000",
			}
			reactionBody := slackEnvelope("Ev-retry-react", reaction)
			status, _, err = e.webhook(step, "reaction before copy", "slack", reactionBody, slackSignedHeaders(reactionBody))
			if err != nil {
				return err
			}
			step.CheckEQ("reaction webhook 状态", http.StatusOK, status)
			time.Sleep(80 * time.Millisecond) // let the first attempt fail on the missing copy
			replyBody := slackMessageEvent("Ev-retry-reply", "C_SUPPORT", "U_BOB", "retry reply inbound", slackRetryTS, slackRetryRoot, "", nil)
			status, _, err = e.webhook(step, "reply after reaction", "slack", replyBody, slackSignedHeaders(replyBody))
			if err != nil {
				return err
			}
			step.CheckEQ("reply webhook 状态", http.StatusOK, status)
			err = waitFor(20*time.Second, func() error {
				thread, findErr := e.getThread(step, root.ID)
				if findErr != nil {
					return findErr
				}
				for _, candidate := range thread.Replies {
					if strings.Contains(candidate.Text, "retry reply inbound") {
						if reactionCount(candidate.Reactions, "thumbs_up") != 1 {
							return fmt.Errorf("reaction 尚未落到回复：%v", candidate.Reactions)
						}
						return nil
					}
				}
				return fmt.Errorf("reply 尚未出现")
			})
			if err != nil {
				return err
			}
			dead, err := e.syncDeadInbox(step, "slack")
			if err != nil {
				return err
			}
			deadLettered := false
			for _, item := range dead.Dead {
				if strings.Contains(item.ID, "Ev-retry-react") {
					deadLettered = true
				}
			}
			step.Check("inbox 未把该 reaction 判死", !deadLettered, "absent", fmt.Sprint(deadLettered))
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("入站 dead letter：永不存在的 copy → dead → retry 端点接受",
		"reaction_added 指向从未导入的 ts", "dead inbox 列出该 event（attempts≥5）；POST retry 返回 retried", func(step *Step) error {
			reaction := map[string]any{
				"type": "reaction_added", "user": "U_BOB", "reaction": "thumbsup",
				"item":     map[string]any{"type": "message", "channel": "C_SUPPORT", "ts": "1700009999.000100"},
				"event_ts": "1700009999.000200",
			}
			body := slackEnvelope("Ev-never-copy", reaction)
			status, _, err := e.webhook(step, "reaction with no copy", "slack", body, slackSignedHeaders(body))
			if err != nil {
				return err
			}
			step.CheckEQ("webhook 状态", http.StatusOK, status)
			var deadID string
			var attempts int
			err = waitFor(20*time.Second, func() error {
				view, listErr := e.syncDeadInbox(step, "slack")
				if listErr != nil {
					return listErr
				}
				for _, item := range view.Dead {
					if strings.Contains(item.ID, "Ev-never-copy") {
						deadID, attempts = item.ID, item.Attempts
						return nil
					}
				}
				return fmt.Errorf("尚未 dead：%s", mustSample(view.Dead))
			})
			if err != nil {
				return err
			}
			step.Check("attempts ≥ 5", attempts >= 5, ">=5", fmt.Sprint(attempts))
			status, response, err := e.api(step, "retry dead inbox", http.MethodPost, "/sync/inbox/"+deadID+"/retry?key=slack", nil)
			if err != nil {
				return err
			}
			step.CheckEQ("retry 状态", http.StatusOK, status)
			step.CheckContains("retry 响应", `"retried":true`, string(response))
			step.Actual = fmt.Sprintf("event=%s attempts=%d", deadID, attempts)
			return nil
		}); err != nil {
		return
	}
}

// scenarioSlackRestart stops every process-local object, writes through a
// fresh log handle while sync is down, then restarts and proves both the
// blocked delivery and the new event land exactly once.
func (e *e2eEnv) scenarioSlackRestart() {
	sc := e.report.Scenario("slack-restart")

	if err := sc.Step("重启恢复：停服期间的 outbox 与 cursor 都在 PostgreSQL",
		"阻塞 fake post → Stop → 另一 log handle 写回复 → Start → 两件事都投递一次", "RESTART-BLOCK 与 WRITTEN-WHILE-DOWN 各成功一次", func(step *Step) error {
			root, err := e.findThreadQuiet("support", "retry root inbound")
			if err != nil {
				return err
			}
			e.fake.ResetSlack()
			started, release := e.fake.SlackBlock("RESTART-BLOCK")
			if _, err := e.replyMessage(step, root.Message.ID, "RESTART-BLOCK board reply"); err != nil {
				return err
			}
			select {
			case <-started:
				step.Note("fake Slack 已收到并挂起 POST，准备停服")
			case <-time.After(15 * time.Second):
				release()
				return fmt.Errorf("fake Slack 未收到 RESTART-BLOCK")
			}
			e.harness.Stop()
			release()
			step.Note("harness 已停止（server/store/workers/registry 全部丢弃）")
			downID, err := appendBoardReply(e.ctx, e.dsn, e.harnessSchema, parseRef(root.Message.ID), "WRITTEN-WHILE-DOWN")
			if err != nil {
				return err
			}
			step.Note("停服期间用独立 log.Board 写入 reply=%d", downID)
			if err := e.harness.Start(e.ctx); err != nil {
				return err
			}
			step.Note("harness 已用同一 schema 重启（fresh store/board/registry/adapters/workers）")
			err = waitFor(25*time.Second, func() error {
				posts := e.fake.SlackPosts()
				blocked := countSlackPosts(posts, "RESTART-BLOCK board reply")
				down := countSlackPosts(posts, "WRITTEN-WHILE-DOWN")
				if blocked != 1 || down != 1 {
					return fmt.Errorf("blocked=%d down=%d", blocked, down)
				}
				return nil
			})
			if err != nil {
				return err
			}
			posts := e.fake.SlackPosts()
			step.CheckEQ("RESTART-BLOCK 成功投递次数", 1, countSlackPosts(posts, "RESTART-BLOCK board reply"))
			step.CheckEQ("停服期间写入的回复投递次数", 1, countSlackPosts(posts, "WRITTEN-WHILE-DOWN"))
			thread, err := e.getThread(step, root.Message.ID)
			if err != nil {
				return err
			}
			step.Check("停服写入的回复已回读到 board", countReplies(thread, "WRITTEN-WHILE-DOWN") == 1, "1", fmt.Sprint(countReplies(thread, "WRITTEN-WHILE-DOWN")))
			var drained syncStatusResponse
			err = waitFor(10*time.Second, func() error {
				statusBody, statusErr := e.syncStatus(step)
				if statusErr != nil {
					return statusErr
				}
				drained = statusBody
				if statusBody.Outbox.Pending+statusBody.Outbox.Leased != 0 {
					return fmt.Errorf("outbox pending=%d leased=%d", statusBody.Outbox.Pending, statusBody.Outbox.Leased)
				}
				return nil
			})
			if err != nil {
				return err
			}
			step.CheckEQ("重启后 outbox pending", 0, drained.Outbox.Pending)
			step.CheckEQ("重启后 outbox leased", 0, drained.Outbox.Leased)
			return nil
		}); err != nil {
		return
	}
}

// scenarioAdapterTyping exercises the Slack adapter's typing path directly;
// board-level typing has no server seam (recorded as a skip).
func (e *e2eEnv) scenarioAdapterTyping() {
	sc := e.report.Scenario("adapter-typing")
	if err := sc.Step("Slack typing（adapter 级）：assistant.threads.setStatus",
		"slack.Provider.Typing(C_SUPPORT:<root>)", "fake Slack 收到 assistant.threads.setStatus，且 board 无 wiring", func(step *Step) error {
			typist, ok := e.harness.adapters["slack"].(interface {
				Typing(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, latest, status string) error
			})
			if !ok {
				return fmt.Errorf("slack adapter 未实现 Typist")
			}
			e.fake.ResetSlack()
			err := typist.Typing(e.ctx, "slack", boardsync.ThreadRef{
				Key: "slack", Platform: "slack", Channel: "C_SUPPORT", Thread: "C_SUPPORT:" + slackRootTS,
			}, "", "Typing…")
			if err != nil {
				return err
			}
			err = waitFor(5*time.Second, func() error {
				if len(e.fake.SlackTyping()) == 0 {
					return fmt.Errorf("fake 未收到 typing")
				}
				return nil
			})
			if err != nil {
				return err
			}
			typing := e.fake.SlackTyping()[0]
			step.CheckEQ("typing channel", "C_SUPPORT", typing.Channel)
			step.CheckEQ("typing thread_ts", slackRootTS, typing.Thread)
			step.CheckContains("typing status", "Typing", typing.Status)
			step.Note("board 级 typing 未接线：已记录为 SKIP。")
			return nil
		}); err != nil {
		return
	}
}

// findThreadQuiet finds a thread without a step, for precondition checks.
func (e *e2eEnv) findThreadQuiet(channel, text string) (threadResponse, error) {
	status, body, err := e.harness.API(http.MethodGet, "/threads?channel="+urlQueryEscape(channel)+"&text="+urlQueryEscape(text), nil)
	if err != nil {
		return threadResponse{}, err
	}
	if status != http.StatusOK {
		return threadResponse{}, fmt.Errorf("threads status %d: %s", status, clip(string(body), 300))
	}
	var page protocol.ThreadsPage
	if err := jsonUnmarshal(body, &page); err != nil {
		return threadResponse{}, err
	}
	if len(page.Threads) == 0 {
		return threadResponse{}, fmt.Errorf("no thread matching %q", text)
	}
	status, body, err = e.harness.API(http.MethodGet, "/messages/"+page.Threads[0].Message.ID, nil)
	if err != nil {
		return threadResponse{}, err
	}
	var thread threadResponse
	if err := jsonUnmarshal(body, &thread); err != nil {
		return threadResponse{}, err
	}
	return thread, nil
}

func countSlackPosts(posts []slackPost, text string) int {
	count := 0
	for _, post := range posts {
		if strings.Contains(post.Text, text) {
			count++
		}
	}
	return count
}

func parseRef(value string) uint64 {
	var id uint64
	_, _ = fmt.Sscan(strings.TrimPrefix(value, "#"), &id)
	return id
}

// reactionCount counts a reaction under its canonical name or Slack's own
// imported alias, since an imported event stores the platform's spelling.
func reactionCount(reactions map[string][]protocol.Ref, canonical string) int {
	count := len(reactions[canonical])
	if canonical == "thumbs_up" {
		count += len(reactions["thumbsup"])
	}
	return count
}
