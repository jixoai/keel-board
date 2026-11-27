package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jixoai/keel-board/protocol"
	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/chatsdk"
)

// Aliases keep the bridge scenario readable while using the real contract
// types and delivery kinds.
type chatsdkInboundEvent = chatsdk.InboundEvent
type chatsdkInboundBatch = chatsdk.InboundBatch

const chatsdkDeliverMessage = chatsdk.DeliverMessage

// watcherViewResponse is the management DTO the harness reads.
type watcherResponse struct {
	Watcher struct {
		ID      string `json:"id"`
		Channel string `json:"channel"`
		Action  string `json:"action"`
		Enabled bool   `json:"enabled"`
		Cursor  string `json:"cursor"`
	} `json:"watcher"`
}

type watcherDeliveriesResponse struct {
	Deliveries []struct {
		WatcherID string `json:"watcherId"`
		Event     string `json:"event"`
		State     string `json:"state"`
		Attempts  int    `json:"attempts"`
		LastError string `json:"lastError"`
	} `json:"deliveries"`
	Depth struct {
		Pending int `json:"pending"`
		Leased  int `json:"leased"`
		Done    int `json:"done"`
		Dead    int `json:"dead"`
	} `json:"depth"`
}

type scheduleResponse struct {
	Schedule struct {
		ID        string `json:"id"`
		Channel   string `json:"channel"`
		Status    string `json:"status"`
		Sent      int    `json:"sent"`
		NextRunAt string `json:"nextRunAt"`
	} `json:"schedule"`
	Replayed bool `json:"replayed"`
}

// scenarioAutomation proves watchers and schedules run in the harness server,
// mirroring the workers cmd/board starts beside sync.
func (e *e2eEnv) scenarioAutomation() {
	sc := e.report.Scenario("automation")
	var watcherID string

	if err := sc.Step("创建 watcher（post action）",
		"POST /channels automation；POST /watchers filter=labels{kind:alert} action=post",
		"201，enabled=true，cursor 固定在 head", func(step *Step) error {
			status, body, err := e.api(step, "create automation channel", http.MethodPost, "/channels", map[string]any{"path": "automation"})
			if err != nil {
				return err
			}
			step.CheckEQ("频道创建状态", http.StatusCreated, status)
			status, body, err = e.api(step, "create watcher", http.MethodPost, "/watchers", map[string]any{
				"id": "e2e-alert-watcher", "channel": "automation",
				"filter": map[string]any{"types": []string{"message.posted"}, "labels": map[string]string{"kind": "alert"}},
				"action": map[string]any{"name": "post", "params": map[string]string{"text": "watcher fired"}},
			})
			if err != nil {
				return err
			}
			step.CheckEQ("watcher 创建状态", http.StatusCreated, status)
			var view watcherResponse
			if err := json.Unmarshal(body, &view); err != nil {
				return err
			}
			watcherID = view.Watcher.ID
			step.CheckEQ("watcher id", "e2e-alert-watcher", watcherID)
			step.CheckEQ("action", "post", view.Watcher.Action)
			step.CheckBool("enabled", true, view.Watcher.Enabled)
			step.Check("cursor 已固定", view.Watcher.Cursor != "", "非空", view.Watcher.Cursor)
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("watcher 触发：匹配标签的 board 消息产生 bot post",
		"POST /messages labels{kind:alert}；等待 watcher delivery 与 bot 消息",
		"bot 在 automation 频道发出 'watcher fired'，delivery done=1", func(step *Step) error {
			triggered, err := e.postMessageLabels(step, "automation", "alert trigger message", map[string]string{"kind": "alert"})
			if err != nil {
				return err
			}
			step.Note("trigger message id=%s", triggered.ID)
			err = waitFor(15*time.Second, func() error {
				page, findErr := e.threads(step, "automation", "watcher fired")
				if findErr != nil {
					return findErr
				}
				if len(page.Threads) != 1 {
					return fmt.Errorf("watcher posts=%d", len(page.Threads))
				}
				return nil
			})
			if err != nil {
				return err
			}
			page, err := e.threads(step, "automation", "watcher fired")
			if err != nil {
				return err
			}
			step.CheckContains("bot post 文本", "watcher fired", page.Threads[0].Message.Text)
			status, body, err := e.api(step, "watcher deliveries", http.MethodGet, "/watchers/"+watcherID+"/deliveries", nil)
			if err != nil {
				return err
			}
			step.CheckEQ("deliveries 状态", http.StatusOK, status)
			var deliveries watcherDeliveriesResponse
			if err := json.Unmarshal(body, &deliveries); err != nil {
				return err
			}
			step.Check("done ≥ 1", deliveries.Depth.Done >= 1, ">=1", fmt.Sprint(deliveries.Depth.Done))
			step.CheckEQ("dead", 0, deliveries.Depth.Dead)
			if len(deliveries.Deliveries) > 0 {
				step.CheckEQ("delivery state", "done", deliveries.Deliveries[0].State)
			}
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("schedule 一次性触发：到点由创建者写入 board",
		"POST /schedules {channel: automation, text, times:1}；等待 schedule worker",
		"board 出现 'scheduled hello'，schedule done/sent=1，视图不回显 text", func(step *Step) error {
			status, body, err := e.api(step, "create schedule", http.MethodPost, "/schedules", map[string]any{
				"channel": "automation", "text": "scheduled hello", "times": 1,
				"labels": map[string]string{"kind": "reminder"},
			})
			if err != nil {
				return err
			}
			step.CheckEQ("schedule 创建状态", http.StatusCreated, status)
			step.Check("创建响应不回显 text", !strings.Contains(string(body), "scheduled hello"), "不包含", trimBody(string(body)))
			var created scheduleResponse
			if err := json.Unmarshal(body, &created); err != nil {
				return err
			}
			scheduleID := created.Schedule.ID
			step.Check("schedule ID 非空", scheduleID != "", "非空", scheduleID)
			err = waitFor(15*time.Second, func() error {
				page, findErr := e.threads(step, "automation", "scheduled hello")
				if findErr != nil {
					return findErr
				}
				if len(page.Threads) != 1 {
					return fmt.Errorf("scheduled posts=%d", len(page.Threads))
				}
				return nil
			})
			if err != nil {
				return err
			}
			status, body, err = e.api(step, "read schedule", http.MethodGet, "/schedules/"+scheduleID, nil)
			if err != nil {
				return err
			}
			step.CheckEQ("schedule 读取状态", http.StatusOK, status)
			step.Check("读取视图不回显 text", !strings.Contains(string(body), "scheduled hello"), "不包含", trimBody(string(body)))
			var finished scheduleResponse
			if err := json.Unmarshal(body, &finished); err != nil {
				return err
			}
			step.CheckEQ("schedule status", "done", finished.Schedule.Status)
			step.CheckEQ("schedule sent", 1, finished.Schedule.Sent)
			step.Actual = fmt.Sprintf("schedule=%s status=%s sent=%d", scheduleID, finished.Schedule.Status, finished.Schedule.Sent)
			return nil
		}); err != nil {
		return
	}
}

// scenarioChatSDK proves the bridge route and the remote adapter against the
// reference sidecar fixture: signed inbound batches, HMAC/nonce/timestamp
// failures, and message/edit/reaction/delete deliveries.
func (e *e2eEnv) scenarioChatSDK() {
	sc := e.report.Scenario("chatsdk")
	var root protocol.MessageView

	if err := sc.Step("bridge outbound：board reply → sidecar /deliver",
		"attach C_CHAT:T1；POST board reply；sidecar 收到 kind=message",
		"真实 chatsdk client 签名并投递，copy 记录在 board", func(step *Step) error {
			created, err := e.postMessage(step, "chatty-board", "chatty board root")
			if err != nil {
				return err
			}
			root = created
			before := len(e.harness.sidecar.Deliveries())
			view, err := e.attachThread(step, "chatty", root.ID, "C_CHAT:T1")
			if err != nil {
				return err
			}
			step.CheckEQ("attach kind", "thread", view.Kind)
			time.Sleep(200 * time.Millisecond)
			step.CheckEQ("attach 不发消息", before, len(e.harness.sidecar.Deliveries()))
			reply, err := e.replyMessage(step, root.ID, "chatty outbound hello")
			if err != nil {
				return err
			}
			err = waitFor(15*time.Second, func() error {
				for _, delivery := range e.harness.sidecar.Deliveries() {
					if delivery.Kind == chatsdkDeliverMessage && strings.Contains(delivery.Body.Markdown, "chatty outbound hello") {
						return nil
					}
				}
				return fmt.Errorf("sidecar 未收到 message delivery")
			})
			if err != nil {
				return err
			}
			var found bool
			for _, delivery := range e.harness.sidecar.Deliveries() {
				if delivery.Kind == chatsdkDeliverMessage && strings.Contains(delivery.Body.Markdown, "chatty outbound hello") {
					found = true
					step.CheckEQ("delivery key", "chatty", string(delivery.Key))
					step.CheckEQ("delivery channel", "C_CHAT", delivery.Target.Channel)
					step.CheckEQ("delivery thread", "T1", delivery.Target.Thread)
					step.Check("delivery ID 非空", delivery.DeliveryID != "", "非空", delivery.DeliveryID)
				}
			}
			step.CheckBool("message delivery 存在", true, found)
			thread, err := e.getThread(step, root.ID)
			if err != nil {
				return err
			}
			for _, candidate := range thread.Replies {
				if candidate.ID == reply.ID {
					step.CheckEQ("无 undelivered", 0, len(candidate.Undelivered))
				}
			}
			step.Note("sidecar deliveries: %s", mustSample(e.harness.sidecar.Deliveries()))
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("bridge outbound 变更：edit / reaction / delete",
		"board edit、react、delete 已投递 reply", "sidecar 依次收到 kind=edit/reaction/delete", func(step *Step) error {
			thread, err := e.getThread(step, root.ID)
			if err != nil {
				return err
			}
			var reply protocol.MessageView
			for _, candidate := range thread.Replies {
				if strings.Contains(candidate.Text, "chatty outbound hello") {
					reply = candidate
				}
			}
			if reply.ID == "" {
				return fmt.Errorf("找不到已投递 reply")
			}
			base := len(e.harness.sidecar.Deliveries())
			if err := e.editMessage(step, reply.ID, "chatty outbound edited", "local:user:dev"); err != nil {
				return err
			}
			if err := e.reactMessage(step, reply.ID, "thumbs_up", false, "local:user:dev"); err != nil {
				return err
			}
			if err := e.deleteMessage(step, reply.ID, "local:user:dev"); err != nil {
				return err
			}
			err = waitFor(15*time.Second, func() error {
				kinds := map[string]bool{}
				for _, delivery := range e.harness.sidecar.Deliveries()[base:] {
					kinds[string(delivery.Kind)] = true
				}
				if !kinds["edit"] || !kinds["reaction"] || !kinds["delete"] {
					return fmt.Errorf("kinds=%v", kinds)
				}
				return nil
			})
			if err != nil {
				return err
			}
			kinds := map[string]bool{}
			for _, delivery := range e.harness.sidecar.Deliveries()[base:] {
				kinds[string(delivery.Kind)] = true
			}
			step.CheckBool("edit delivery", true, kinds["edit"])
			step.CheckBool("reaction delivery", true, kinds["reaction"])
			step.CheckBool("delete delivery", true, kinds["delete"])
			step.Note("new deliveries: %s", mustSample(e.harness.sidecar.Deliveries()[base:]))
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("bridge inbound：sidecar PushInbound → board root/reply",
		"sidecar 签名 POST /v1/board/sync/bridge/chatty/events",
		"200 stored≥1，board chatty-board 出现 from=chatty 的 root", func(step *Step) error {
			result, err := e.harness.sidecar.PushInbound(e.ctx, e.harness.baseURL, []chatsdkInboundEvent{{
				Dedupe: "chatty-inbound-1",
				Event:  chattyInboundEvent("chatty inbound hello", "chatty-in-1"),
			}})
			if err != nil {
				return err
			}
			step.Trace("sidecar PushInbound", http.MethodPost, "/v1/board/sync/bridge/chatty/events", result.Status, "signed InboundBatch", string(result.Body))
			step.CheckEQ("bridge 状态", http.StatusOK, result.Status)
			var stored map[string]int
			if err := json.Unmarshal(result.Body, &stored); err != nil {
				return err
			}
			step.Check("stored ≥ 1", stored["stored"] >= 1, ">=1", fmt.Sprint(stored["stored"]))
			var found threadResponse
			err = waitFor(15*time.Second, func() error {
				thread, findErr := e.findThread(step, "chatty-board", "chatty inbound hello")
				if findErr != nil {
					return findErr
				}
				found = thread
				return nil
			})
			if err != nil {
				return err
			}
			step.CheckBool("from=chatty", true, found.Message.From != nil && *found.Message.From == "chatty")
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("bridge inbound edit → board 文本更新",
		"sidecar PushInbound kind=edited（同 message）", "board root 文本变为 edited", func(step *Step) error {
			result, err := e.harness.sidecar.PushInbound(e.ctx, e.harness.baseURL, []chatsdkInboundEvent{{
				Dedupe: "chatty-inbound-edit-1",
				Event:  chatsdkEditEvent("chatty inbound edited", "chatty-in-1"),
			}})
			if err != nil {
				return err
			}
			step.Trace("sidecar PushInbound edit", http.MethodPost, "/v1/board/sync/bridge/chatty/events", result.Status, "signed InboundBatch", string(result.Body))
			step.CheckEQ("bridge 状态", http.StatusOK, result.Status)
			err = waitFor(15*time.Second, func() error {
				thread, findErr := e.findThread(step, "chatty-board", "chatty inbound edited")
				if findErr != nil {
					return findErr
				}
				if thread.Message.Text != "chatty inbound edited" {
					return fmt.Errorf("text=%q", thread.Message.Text)
				}
				return nil
			})
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("bridge HMAC / nonce / 时间窗",
		"bad signature、同一 nonce 重放、过期 timestamp", "401 / 409 / 401，且均不入库", func(step *Step) error {
			body := mustJSON(chatsdkInboundBatch{
				Version: "1",
				Events: []chatsdkInboundEvent{{
					Dedupe: "chatty-hmac-1",
					Event:  chattyInboundEvent("chatty hmac should not import", "chatty-hmac"),
				}},
			})
			status, response := e.bridgeRaw(step, "bad signature", "chatty", body, map[string]string{
				"Content-Type":           "application/json",
				"X-Keel-Board-Timestamp": fmt.Sprint(time.Now().Unix()),
				"X-Keel-Board-Nonce":     "e2e-bad-nonce",
				"X-Keel-Board-Signature": "v1=deadbeef",
			})
			step.CheckEQ("bad signature 状态", http.StatusUnauthorized, status)
			step.Check("bad signature 响应无内容", len(response) == 0, "空", trimBody(string(response)))

			now := time.Now().Unix()
			nonce := "e2e-replay-nonce"
			replayBody := mustJSON(chatsdkInboundBatch{
				Version: "1",
				Events: []chatsdkInboundEvent{{
					Dedupe: "chatty-replay-1",
					Event:  chattyInboundEvent("chatty replay event", "chatty-replay"),
				}},
			})
			first, err := e.harness.sidecar.PushRaw(e.ctx, e.harness.baseURL, replayBody, now, nonce)
			if err != nil {
				return err
			}
			step.Trace("bridge replay first", http.MethodPost, "/v1/board/sync/bridge/chatty/events", first.Status, "signed InboundBatch", string(first.Body))
			step.CheckEQ("首次 nonce 状态", http.StatusOK, first.Status)
			second, err := e.harness.sidecar.PushRaw(e.ctx, e.harness.baseURL, replayBody, now, nonce)
			if err != nil {
				return err
			}
			step.Trace("bridge replay second", http.MethodPost, "/v1/board/sync/bridge/chatty/events", second.Status, "same nonce", string(second.Body))
			step.CheckEQ("重放 nonce 状态", http.StatusConflict, second.Status)

			stale, err := e.harness.sidecar.PushRaw(e.ctx, e.harness.baseURL, mustJSON(chatsdkInboundBatch{
				Version: "1",
				Events: []chatsdkInboundEvent{{
					Dedupe: "chatty-stale-1",
					Event:  chattyInboundEvent("chatty stale event", "chatty-stale"),
				}},
			}), now-3600, "e2e-stale-nonce")
			if err != nil {
				return err
			}
			step.Trace("bridge stale timestamp", http.MethodPost, "/v1/board/sync/bridge/chatty/events", stale.Status, "old timestamp", string(stale.Body))
			step.CheckEQ("过期 timestamp 状态", http.StatusUnauthorized, stale.Status)

			time.Sleep(300 * time.Millisecond)
			for _, text := range []string{"chatty hmac should not import", "chatty stale event"} {
				page, err := e.threads(step, "chatty-board", text)
				if err != nil {
					return err
				}
				step.CheckEQ("未入库 "+text, 0, len(page.Threads))
			}
			return nil
		}); err != nil {
		return
	}

	if err := sc.Step("bridge 身份：sidecar 自己的 author 被丢弃",
		"sidecar PushInbound author=chatty:user:sidecar-bot", "200 stored=0，board 无回声", func(step *Step) error {
			result, err := e.harness.sidecar.PushInbound(e.ctx, e.harness.baseURL, []chatsdkInboundEvent{{
				Dedupe: "chatty-self-1",
				Event:  chattySelfEvent("chatty self echo"),
			}})
			if err != nil {
				return err
			}
			step.Trace("sidecar self push", http.MethodPost, "/v1/board/sync/bridge/chatty/events", result.Status, "signed InboundBatch", string(result.Body))
			step.CheckEQ("bridge 状态", http.StatusOK, result.Status)
			var stored map[string]int
			if err := json.Unmarshal(result.Body, &stored); err != nil {
				return err
			}
			step.CheckEQ("stored", 0, stored["stored"])
			time.Sleep(300 * time.Millisecond)
			page, err := e.threads(step, "chatty-board", "chatty self echo")
			if err != nil {
				return err
			}
			step.CheckEQ("board 无匹配线程", 0, len(page.Threads))
			return nil
		}); err != nil {
		return
	}
}

// bridgeRaw posts an unsigned-by-the-fixture body, for bad-signature tests.
func (e *e2eEnv) bridgeRaw(step *Step, label, key string, body []byte, headers map[string]string) (int, []byte) {
	url := e.harness.baseURL + "/v1/board/sync/bridge/" + key + "/events"
	status, response, err := postRaw(url, body, headers)
	if err != nil {
		step.Note("bridge raw error: %v", err)
		return 0, nil
	}
	step.Trace(label, http.MethodPost, "/v1/board/sync/bridge/"+key+"/events", status, string(body), string(response))
	return status, response
}

func mustJSON(value any) []byte {
	encoded, _ := json.Marshal(value)
	return encoded
}

func chattyAuthor() boardsync.Author {
	return boardsync.Author{ID: "chatty:user:u1", Handle: "u1", Name: "Chatty User"}
}

func chattyInboundEvent(text, messageID string) boardsync.Event {
	return boardsync.Event{
		Kind: boardsync.KindMessage, Type: boardsync.TypeMessage,
		Channel: "C_CHAT", Thread: "C_CHAT:" + messageID, Message: messageID,
		IsRoot: true, Text: text, Author: chattyAuthor(),
	}
}

func chatsdkEditEvent(text, messageID string) boardsync.Event {
	return boardsync.Event{
		Kind: boardsync.KindEdited, Type: boardsync.TypeMessageEdited,
		Channel: "C_CHAT", Thread: "C_CHAT:" + messageID, Message: messageID,
		Text: text, Author: chattyAuthor(),
	}
}

func chattySelfEvent(text string) boardsync.Event {
	return boardsync.Event{
		Kind: boardsync.KindMessage, Type: boardsync.TypeMessage,
		Channel: "C_CHAT", Thread: "C_CHAT:chatty-self", Message: "chatty-self",
		IsRoot: true, Text: text,
		Author: boardsync.Author{ID: chattyBotID, Handle: "sidecar-bot", Name: "Sidecar Bot"},
	}
}
