// Package gateway implements the API Gateway: REST routing, JWT auth,
// token-bucket rate limiting (Redis), request IDs, WebSocket hub, and the
// WS bridge that fans out Kafka events to connected clients.
package gateway

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Hub manages WebSocket connections and fans out events.
type Hub struct {
	upgrader websocket.Upgrader
	log      *slog.Logger

	mu       sync.RWMutex
	clients  map[*client]bool
	// per-user subscriptions: user_id → set of event types (empty = all).
	subMu sync.RWMutex
	subs  map[string]map[string]bool

	broadcast chan *wsMessage
	register  chan *client
	unregister chan *client
}

type client struct {
	hub   *Hub
	conn  *websocket.Conn
	user  string
	role  string
	send  chan *wsMessage
}

type wsMessage struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// NewHub creates a WebSocket hub.
func NewHub(log *slog.Logger) *Hub {
	return &Hub{
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			// CheckOrigin: allow same-origin + any (dev). Production should
			// restrict to known origins.
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		log:        log,
		clients:    make(map[*client]bool),
		subs:       make(map[string]map[string]bool),
		broadcast:  make(chan *wsMessage, 1024),
		register:   make(chan *client),
		unregister: make(chan *client),
	}
}

// Run starts the hub loop (register/unregister/broadcast).
func (h *Hub) Run() {
	for {
		select {
		case c := <-h.register:
			h.mu.Lock()
			h.clients[c] = true
			h.mu.Unlock()
			h.log.Info("ws client connected", "user", c.user, "role", c.role)
		case c := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
			}
			h.mu.Unlock()
		case msg := <-h.broadcast:
			h.mu.RLock()
			for c := range h.clients {
				// Per-user subscription filter.
				if !h.subscribed(c.user, msg.Type) {
					continue
				}
				select {
				case c.send <- msg:
				default:
					// Backpressure: slow client → drop (documented in ADR-009).
					h.log.Warn("ws client slow, dropping message", "user", c.user)
				}
			}
			h.mu.RUnlock()
		}
	}
}

// subscribed reports whether user is allowed to receive an event type.
// (Empty subs = receive all; USER gets ride.* + nearby; DRIVER gets
// driver.* + assigned-ride events; ADMIN gets everything.)
func (h *Hub) subscribed(user, eventType string) bool {
	h.subMu.RLock()
	defer h.subMu.RUnlock()
	set, ok := h.subs[user]
	if !ok {
		return true // no filter → all
	}
	return len(set) == 0 || set[eventType]
}

// SetSubs records a user's event subscriptions.
func (h *Hub) SetSubs(user string, types []string) {
	h.subMu.Lock()
	defer h.subMu.Unlock()
	set := make(map[string]bool, len(types))
	for _, t := range types {
		set[t] = true
	}
	h.subs[user] = set
}

// Broadcast pushes an event to all (filtered) clients.
func (h *Hub) Broadcast(eventType string, payload any) {
	b, err := json.Marshal(payload)
	if err != nil {
		return
	}
	select {
	case h.broadcast <- &wsMessage{Type: eventType, Payload: b}:
	default:
		// Hub queue full: drop (backpressure).
	}
}

// Connections returns the active connection count (for metrics).
func (h *Hub) Connections() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// ServeWS upgrades an HTTP connection to WebSocket.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request, user, role string) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &client{hub: h, conn: conn, user: user, role: role, send: make(chan *wsMessage, 256)}
	h.register <- c

	go c.writePump()
	go c.readPump()
}

// readPump handles inbound control messages (ping/pong, close) and liveness.
func (c *client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(4096)
	_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	})
	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
		// We don't process inbound data frames in v1 (clients are
		// read-only consumers of events); the deadline + pong keep the
		// connection alive.
	}
}

// writePump sends outbound messages and emits pings for liveness.
func (c *client) writePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				// Hub closed our channel.
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteJSON(msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}