package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jixoai/keel-board/protocol"
)

const linearIssueID = "11111111-1111-1111-1111-111111111111"

// scenarioLinear proves the Linear adapter's GraphQL side and webhook side
// against the fake api.linear.app/graphql.
func (e *e2eEnv) scenarioLinear() {
	sc := e.report.Scenario("linear")
	var root protocol.MessageView

	if err := sc.Step("attach board thread 到 Linear issue（thread target，不建 issue）",
		"POST /sync/threads {key: linear, target: ENG:<uuid>}",
		"200，kind=thread，fake GraphQL 无写入", func(step *Step) error {
			e.fake.ResetLinear()
			created, err := e.postMessage(step, "linear", "linear board root")
			if err != nil {
				return err
			}
			root = created
			view, err := e.attachThread(step, "linear", root.ID, "ENG:"+linearIssueID)
			if err != nil {
				return err
			}
			step.CheckEQ("attach kind", "thread", view.Kind)
			step.CheckEQ("attach target", "linear:ENG:"+linearIssueID, view.Target)
			time.Sleep(250 * time.Millisecond)
			step.CheckEQ("attach thread target 不写 GraphQL", 0, len(e.fake.LinearWrites()))
			return nil
		}); err != nil {
		return
	}

	var outbound protocol.MessageView
	if err := sc.Step("出站：board reply → Linear commentCreate",
		"fake GraphQL mutation CommentCreate", "success，variables 含 issueId/body", func(step *Step) error {
			e.fake.ResetLinear()
			created, err := e.replyMessage(step, root.ID, "linear outbound comment")
			if err != nil {
				return err
			}
			outbound = created
			err = waitFor(10*time.Second, func() error {
				for _, write := range e.fake.LinearWrites() {
					if strings.Contains(write.Query, "commentCreate") {
						return nil
					}
				}
				return fmt.Errorf("没有 commentCreate")
			})
			if err != nil {
				return err
			}
			writes := e.fake.LinearWrites()
			var createdWrite linearWrite
			for _, write := range writes {
				if strings.Contains(write.Query, "commentCreate") {
					createdWrite = write
				}
			}
			step.CheckContains("variables.issueId", linearIssueID, createdWrite.Variables)
			step.CheckContains("variables.body", "linear outbound comment", createdWrite.Variables)
			step.CheckEQ("GraphQL 调用数", 1, len(writes))
			step.Actual = "commentCreate ok"
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("出站变更：commentUpdate / reactionCreate / commentDelete",
		"board edit/react/delete 已投递的 reply", "fake GraphQL 依次收到三个 mutation", func(step *Step) error {
			e.fake.ResetLinear()
			if err := e.editMessage(step, outbound.ID, "linear outbound edited", "local:user:dev"); err != nil {
				return err
			}
			err := waitFor(10*time.Second, func() error {
				for _, write := range e.fake.LinearWrites() {
					if strings.Contains(write.Query, "commentUpdate") {
						return nil
					}
				}
				return fmt.Errorf("没有 commentUpdate")
			})
			if err != nil {
				return err
			}
			if err := e.reactMessage(step, outbound.ID, "thumbs_up", false, "local:user:dev"); err != nil {
				return err
			}
			err = waitFor(10*time.Second, func() error {
				for _, write := range e.fake.LinearWrites() {
					if strings.Contains(write.Query, "reactionCreate") {
						return nil
					}
				}
				return fmt.Errorf("没有 reactionCreate")
			})
			if err != nil {
				return err
			}
			if err := e.deleteMessage(step, outbound.ID, "local:user:dev"); err != nil {
				return err
			}
			err = waitFor(10*time.Second, func() error {
				for _, write := range e.fake.LinearWrites() {
					if strings.Contains(write.Query, "commentDelete") {
						return nil
					}
				}
				return fmt.Errorf("没有 commentDelete")
			})
			if err != nil {
				return err
			}
			step.CheckEQ("GraphQL 调用数", 3, len(e.fake.LinearWrites()))
			step.CheckContains("commentUpdate body", "linear outbound edited", mustSample(e.fake.LinearWrites()))
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("入站：Linear Comment create → board 线程回复",
		"POST /webhooks/linear (signed, embedded issue)", "board 回复 from=linear", func(step *Step) error {
			e.fake.ResetLinear()
			body := linearCommentBody("create", "comment-9200", "linear inbound comment")
			status, _, err := e.webhook(step, "linear comment created", "linear", body, linearSignedHeaders(body))
			if err != nil {
				return err
			}
			step.CheckEQ("webhook 状态", http.StatusOK, status)
			err = waitFor(10*time.Second, func() error {
				thread, findErr := e.getThread(step, root.ID)
				if findErr != nil {
					return findErr
				}
				if _, ok := findReply(thread, "linear inbound comment"); !ok {
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
			reply, _ := findReply(thread, "linear inbound comment")
			step.CheckBool("from=linear", true, reply.From != nil && *reply.From == "linear")
			step.CheckEQ("无 GraphQL 读取（payload 内嵌 issue）", 0, len(e.fake.LinearWrites()))
			step.Actual = fmt.Sprintf("reply=%s", reply.ID)
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("入站 edit/remove：Comment update/remove → board 更新/tombstone",
		"POST signed Comment update, then remove", "board 文本更新，随后回复删除", func(step *Step) error {
			updated := linearCommentBody("update", "comment-9200", "linear inbound edited")
			status, _, err := e.webhook(step, "linear comment updated", "linear", updated, linearSignedHeaders(updated))
			if err != nil {
				return err
			}
			step.CheckEQ("update webhook 状态", http.StatusOK, status)
			err = waitFor(10*time.Second, func() error {
				thread, findErr := e.getThread(step, root.ID)
				if findErr != nil {
					return findErr
				}
				if _, ok := findReply(thread, "linear inbound edited"); !ok {
					return fmt.Errorf("未看到编辑后的文本")
				}
				return nil
			})
			if err != nil {
				return err
			}
			removed := linearCommentBody("remove", "comment-9200", "")
			status, _, err = e.webhook(step, "linear comment removed", "linear", removed, linearSignedHeaders(removed))
			if err != nil {
				return err
			}
			step.CheckEQ("remove webhook 状态", http.StatusOK, status)
			err = waitFor(10*time.Second, func() error {
				thread, findErr := e.getThread(step, root.ID)
				if findErr != nil {
					return findErr
				}
				for _, reply := range thread.Replies {
					if reply.Deleted {
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

	if err := sc.Step("Linear 签名错误：401 且不入库",
		"POST /webhooks/linear with bad signature", "401，无匹配回复", func(step *Step) error {
			body := linearCommentBody("create", "comment-9400", "linear unsigned comment")
			headers := linearSignedHeaders(body)
			headers["Linear-Signature"] = strings.Repeat("00", 32)
			status, _, err := e.webhook(step, "bad linear signature", "linear", body, headers)
			if err != nil {
				return err
			}
			step.CheckEQ("webhook 状态", http.StatusUnauthorized, status)
			time.Sleep(250 * time.Millisecond)
			thread, err := e.getThread(step, root.ID)
			if err != nil {
				return err
			}
			step.CheckEQ("未入库", 0, countReplies(thread, "linear unsigned comment"))
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("Linear 过期 timestamp：401（窗口校验）",
		"POST signed /webhooks/linear with webhookTimestamp=1000ms", "401，无匹配回复", func(step *Step) error {
			body := linearCommentBodyWithTimestamp("create", "comment-9500", "linear stale comment", 1000)
			headers := linearSignedHeaders(body)
			headers["Linear-Timestamp"] = "1000"
			status, _, err := e.webhook(step, "stale linear timestamp", "linear", body, headers)
			if err != nil {
				return err
			}
			step.CheckEQ("webhook 状态", http.StatusUnauthorized, status)
			time.Sleep(250 * time.Millisecond)
			thread, err := e.getThread(step, root.ID)
			if err != nil {
				return err
			}
			step.CheckEQ("未入库", 0, countReplies(thread, "linear stale comment"))
			return nil
		}); err != nil {
		return
	}
}

// linearCommentBody builds a Comment webhook with an embedded issue, so the
// adapter needs no GraphQL read to place it.
func linearCommentBody(action, id, text string) []byte {
	return linearCommentBodyWithTimestamp(action, id, text, time.Now().UnixMilli())
}

func linearCommentBodyWithTimestamp(action, id, text string, timestamp int64) []byte {
	payload := map[string]any{
		"action": action, "type": "Comment",
		"data": map[string]any{
			"id": id, "body": text, "issueId": linearIssueID,
			"issue": map[string]any{
				"id": linearIssueID, "identifier": "ENG-123",
				"team": map[string]any{"id": "team-1", "key": "ENG", "name": "Engineering"},
			},
			"userId": "user-2", "createdAt": "2026-01-02T05:00:00Z",
			"updatedAt": "2026-01-02T05:00:00Z",
		},
		"actor":     map[string]any{"id": "user-2", "name": "julian", "type": "user"},
		"webhookId": "wh-linear-e2e", "webhookTimestamp": timestamp,
	}
	encoded, _ := json.Marshal(payload)
	return encoded
}
