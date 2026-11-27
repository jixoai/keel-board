package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jixoai/keel-board/protocol"
	boardsync "github.com/jixoai/keel-board/sync"
)

// Source-read fixture identities. They are stable so the fake Slack and Linear
// stores answer the same read every run.
const (
	slackSourceChannel = "C0SOURCES"
	slackSourceRoot    = "1700001000.000100"
	slackSourceSecond  = "1700001000.000200"
	slackSourceReply   = "1700001000.000300"
	slackJoinChannel   = "C0JOIN"
	slackPrivateChan   = "C0PRIVATE"
	linearSourceIssue  = "ENG-123"
)

// scenarioSources proves the design's §14.10 source routes: workspace-managed
// on-demand reads and the opt-in join, over both native adapters.
func (e *e2eEnv) scenarioSources() {
	sc := e.report.Scenario("sources")

	if err := sc.Step("source read：Slack 频道按需读取，不写 board",
		"GET /v1/board/sources/slack/read?target=C0SOURCES&limit=10（fake conversations.info/history）",
		"200，归一化消息 oldest first，board head 不变，平台调用 info/history 各 1 次", func(step *Step) error {
			e.fake.ResetSlack()
			e.fake.SlackSeedChannel(slackSourceChannel, "sources-e2e", true, false)
			e.fake.SlackSeedMessage(slackSourceChannel, slackSourceRoot, "U_ALICE", "source read hello", "")
			e.fake.SlackSeedMessage(slackSourceChannel, slackSourceSecond, "U_BOB", "source read second", "")
			if err := e.waitSyncQuiet(); err != nil {
				return err
			}
			before, err := e.harness.board.Head(e.harness.tenant)
			if err != nil {
				return err
			}
			status, body, err := e.api(step, "slack source read", http.MethodGet,
				"/sources/slack/read?target="+slackSourceChannel+"&limit=10", nil)
			if err != nil {
				return err
			}
			step.CheckEQ("read 状态", http.StatusOK, status)
			var read boardsync.SourceRead
			if err := json.Unmarshal(body, &read); err != nil {
				return fmt.Errorf("decode source read: %w (%s)", err, clip(string(body), 500))
			}
			step.CheckEQ("kind", "channel", read.Kind)
			step.CheckEQ("channel", slackSourceChannel, read.Channel)
			step.CheckEQ("name", "sources-e2e", read.Name)
			step.CheckBool("more", false, read.More)
			step.CheckEQ("消息数", 2, len(read.Messages))
			if len(read.Messages) == 2 {
				step.CheckEQ("oldest first：第一条 ID", slackSourceRoot, read.Messages[0].ID)
				step.CheckEQ("第一条 authorId", "slack:user:U_ALICE", read.Messages[0].AuthorID)
				step.CheckEQ("第一条文本", "source read hello", read.Messages[0].Text)
				step.CheckEQ("第二条 authorId", "slack:user:U_BOB", read.Messages[1].AuthorID)
				step.CheckEQ("第二条 thread", slackSourceChannel+":"+slackSourceSecond, read.Messages[1].Thread)
			}
			after, err := e.harness.board.Head(e.harness.tenant)
			if err != nil {
				return err
			}
			step.CheckEQ("board head 不变", before, after)
			step.CheckEQ("conversations.info 调用", 1, len(e.fake.CallsTo("slack.com", "/api/conversations.info")))
			step.CheckEQ("conversations.history 调用", 1, len(e.fake.CallsTo("slack.com", "/api/conversations.history")))
			step.Actual = fmt.Sprintf("messages=%d，head=%d", len(read.Messages), after)
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("source read：Slack thread 目标",
		"GET /v1/board/sources/slack/read?target=C0SOURCES:"+slackSourceRoot,
		"200，kind=thread，根 + 回复共 2 条 oldest first", func(step *Step) error {
			e.fake.SlackSeedMessage(slackSourceChannel, slackSourceReply, "U_CAROL", "source read reply", slackSourceRoot)
			target := slackSourceChannel + ":" + slackSourceRoot
			status, body, err := e.api(step, "slack thread read", http.MethodGet,
				"/sources/slack/read?target="+target, nil)
			if err != nil {
				return err
			}
			step.CheckEQ("read 状态", http.StatusOK, status)
			var read boardsync.SourceRead
			if err := json.Unmarshal(body, &read); err != nil {
				return fmt.Errorf("decode thread read: %w (%s)", err, clip(string(body), 500))
			}
			step.CheckEQ("kind", "thread", read.Kind)
			step.CheckEQ("thread", target, read.Thread)
			step.CheckEQ("消息数", 2, len(read.Messages))
			if len(read.Messages) == 2 {
				step.CheckEQ("根 ID", slackSourceRoot, read.Messages[0].ID)
				step.CheckEQ("回复作者", "U_CAROL", read.Messages[1].Author)
				step.CheckEQ("回复文本", "source read reply", read.Messages[1].Text)
			}
			step.CheckEQ("conversations.replies 调用", 1, len(e.fake.CallsTo("slack.com", "/api/conversations.replies")))
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("source read：缺失/未知/私有/未知 key 的稳定错误",
		"GET read 缺 target、未知频道、私有频道、未注册 key",
		"400 invalid_request、404 not_found、501 unsupported、404 not_found（均无平台 raw body）", func(step *Step) error {
			e.fake.SlackSeedChannel(slackPrivateChan, "private-e2e", true, true)
			cases := []struct {
				name string
				path string
				want int
				code protocol.Code
			}{
				{"缺 target", "/sources/slack/read", http.StatusBadRequest, protocol.CodeInvalidRequest},
				{"未知频道", "/sources/slack/read?target=C0UNKNOWN", http.StatusNotFound, protocol.CodeNotFound},
				{"私有频道", "/sources/slack/read?target=" + slackPrivateChan, http.StatusNotImplemented, protocol.CodeUnsupported},
				{"未注册 key", "/sources/missing/read?target=" + slackSourceChannel, http.StatusNotFound, protocol.CodeNotFound},
			}
			for _, tc := range cases {
				status, body, err := e.api(step, tc.name, http.MethodGet, tc.path, nil)
				if err != nil {
					return err
				}
				step.CheckEQ(tc.name+" 状态", tc.want, status)
				var failure protocol.ErrorBody
				if err := json.Unmarshal(body, &failure); err != nil {
					return fmt.Errorf("%s decode: %w (%s)", tc.name, err, clip(string(body), 300))
				}
				step.CheckEQ(tc.name+" code", tc.code, failure.Error.Code)
				if strings.Contains(string(body), "channel_not_found") || strings.Contains(string(body), "RAW") {
					return fmt.Errorf("%s leaked a platform body: %s", tc.name, clip(string(body), 300))
				}
			}
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("source join：Slack opt-in join 幂等",
		"POST /v1/board/sources/slack/join {target: C0JOIN} 两次",
		"首次 200 joined=true 且 fake 收到 conversations.join；再次 200 joined=false", func(step *Step) error {
			e.fake.ResetSlack()
			e.fake.SlackSeedChannel(slackJoinChannel, "join-e2e", false, false)
			status, body, err := e.api(step, "slack join", http.MethodPost,
				"/sources/slack/join", map[string]any{"target": slackJoinChannel})
			if err != nil {
				return err
			}
			step.CheckEQ("join 状态", http.StatusOK, status)
			var join boardsync.SourceJoin
			if err := json.Unmarshal(body, &join); err != nil {
				return fmt.Errorf("decode join: %w (%s)", err, clip(string(body), 300))
			}
			step.CheckBool("joined", true, join.Joined)
			step.CheckEQ("join channel", slackJoinChannel, join.Channel)
			step.CheckEQ("join name", "join-e2e", join.Name)
			step.CheckEQ("fake conversations.join 调用", 1, len(e.fake.SlackJoins()))
			status, body, err = e.api(step, "slack rejoin", http.MethodPost,
				"/sources/slack/join", map[string]any{"target": slackJoinChannel})
			if err != nil {
				return err
			}
			step.CheckEQ("再次 join 状态", http.StatusOK, status)
			if err := json.Unmarshal(body, &join); err != nil {
				return fmt.Errorf("decode rejoin: %w (%s)", err, clip(string(body), 300))
			}
			step.CheckBool("再次 joined=false", false, join.Joined)
			step.CheckEQ("fake join 调用仍为 1", 1, len(e.fake.SlackJoins()))
			status, body, err = e.api(step, "join without target", http.MethodPost,
				"/sources/slack/join", map[string]any{})
			if err != nil {
				return err
			}
			step.CheckEQ("缺 target 状态", http.StatusBadRequest, status)
			var failure protocol.ErrorBody
			if err := json.Unmarshal(body, &failure); err != nil {
				return fmt.Errorf("decode empty join: %w (%s)", err, clip(string(body), 300))
			}
			step.CheckEQ("缺 target code", protocol.CodeInvalidRequest, failure.Error.Code)
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("source read/join：Linear issue 读取与未实现 join",
		"GET /sources/linear/read?target=ENG-123；POST /sources/linear/join",
		"200 kind=issue 根+评论；未注册 joiner 返回 501 unsupported；未授权 team 404", func(step *Step) error {
			e.fake.ResetLinear()
			status, body, err := e.api(step, "linear issue read", http.MethodGet,
				"/sources/linear/read?target="+linearSourceIssue+"&limit=10", nil)
			if err != nil {
				return err
			}
			step.CheckEQ("read 状态", http.StatusOK, status)
			var read boardsync.SourceRead
			if err := json.Unmarshal(body, &read); err != nil {
				return fmt.Errorf("decode linear read: %w (%s)", err, clip(string(body), 500))
			}
			step.CheckEQ("kind", "issue", read.Kind)
			step.CheckEQ("channel", "ENG", read.Channel)
			step.CheckEQ("消息数（根 + 1 评论）", 2, len(read.Messages))
			if len(read.Messages) == 2 {
				step.CheckEQ("根 ID", linearSourceIssue, read.Messages[0].ID)
				step.CheckContains("根文本含标题", "ENG-123: Checkout retry", read.Messages[0].Text)
				step.CheckEQ("评论作者", "julian", read.Messages[1].Author)
				step.CheckEQ("评论文本", "source read comment", read.Messages[1].Text)
			}
			step.CheckEQ("GraphQL 无 mutation", 0, mutationCalls(e.fake.LinearWrites()))
			status, body, err = e.api(step, "linear unlisted team", http.MethodGet,
				"/sources/linear/read?target=OPS", nil)
			if err != nil {
				return err
			}
			step.CheckEQ("未授权 team 状态", http.StatusNotFound, status)
			status, body, err = e.api(step, "linear join", http.MethodPost,
				"/sources/linear/join", map[string]any{"target": "ENG"})
			if err != nil {
				return err
			}
			step.CheckEQ("linear join 状态", http.StatusNotImplemented, status)
			var failure protocol.ErrorBody
			if err := json.Unmarshal(body, &failure); err != nil {
				return fmt.Errorf("decode linear join: %w (%s)", err, clip(string(body), 300))
			}
			step.CheckEQ("linear join code", protocol.CodeUnsupported, failure.Error.Code)
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("entry status：单条 sync entry",
		"GET /v1/board/sync/entries/slack/status；GET .../missing/status",
		"200 单个 entry（key/platform/enabled/owned/inbox/outbox）；未知 key 404", func(step *Step) error {
			status, body, err := e.api(step, "slack entry status", http.MethodGet,
				"/sync/entries/slack/status", nil)
			if err != nil {
				return err
			}
			step.CheckEQ("status 状态", http.StatusOK, status)
			var view struct {
				Keys []struct {
					Key      string `json:"key"`
					Platform string `json:"platform"`
					Enabled  bool   `json:"enabled"`
					Owned    bool   `json:"owned"`
					Inbox    struct {
						Done int `json:"done"`
					} `json:"inbox"`
				} `json:"keys"`
				Outbox struct {
					Pending int `json:"pending"`
					Leased  int `json:"leased"`
				} `json:"outbox"`
			}
			if err := json.Unmarshal(body, &view); err != nil {
				return fmt.Errorf("decode entry status: %w (%s)", err, clip(string(body), 300))
			}
			step.CheckEQ("entry 数", 1, len(view.Keys))
			if len(view.Keys) == 1 {
				step.CheckEQ("key", "slack", view.Keys[0].Key)
				step.CheckEQ("platform", "slack", view.Keys[0].Platform)
				step.CheckBool("enabled", true, view.Keys[0].Enabled)
				step.CheckBool("owned", true, view.Keys[0].Owned)
			}
			status, body, err = e.api(step, "unknown entry status", http.MethodGet,
				"/sync/entries/missing/status", nil)
			if err != nil {
				return err
			}
			step.CheckEQ("未知 key 状态", http.StatusNotFound, status)
			var failure protocol.ErrorBody
			if err := json.Unmarshal(body, &failure); err != nil {
				return fmt.Errorf("decode unknown status: %w (%s)", err, clip(string(body), 300))
			}
			step.CheckEQ("未知 key code", protocol.CodeNotFound, failure.Error.Code)
			return nil
		}); err != nil {
		return
	}
}

// waitSyncQuiet waits until no inbound or outbound item is pending or leased,
// so a head comparison around a source read isn't racing a worker.
func (e *e2eEnv) waitSyncQuiet() error {
	return waitFor(10*time.Second, func() error {
		status, body, err := e.harness.API(http.MethodGet, "/sync/status", nil)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("sync status %d: %s", status, clip(string(body), 200))
		}
		var view syncStatusResponse
		if err := json.Unmarshal(body, &view); err != nil {
			return err
		}
		if view.Outbox.Pending+view.Outbox.Leased != 0 {
			return fmt.Errorf("outbox busy: %+v", view.Outbox)
		}
		for _, key := range view.Keys {
			if key.Inbox.Pending+key.Inbox.Leased != 0 {
				return fmt.Errorf("inbox busy for %s: %+v", key.Key, key.Inbox)
			}
		}
		return nil
	})
}

// mutationCalls counts GraphQL requests that change Linear, so a source read
// is proven read-only.
func mutationCalls(writes []linearWrite) int {
	count := 0
	for _, write := range writes {
		if strings.Contains(write.Query, "mutation") {
			count++
		}
	}
	return count
}
