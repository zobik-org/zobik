package console

import (
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/crypto/chacha20poly1305"
)

// The work area keeps each conversation in the console's directory, encrypted
// with a key of its own that the password seals (implementation §6, The
// operator console):
//
//	<conversation>/conversation   the conversation, in JSON
//	<conversation>/<attachment>   the content of each attachment
//	deleted                       the traces of deleted conversations whose
//	                              outcome has not arrived yet
//
// Each file is a nonce followed by the ciphertext, and its path inside the
// directory is the associated data, so a file moved to another place does not open.

// Conversation is a conversation of the work area, as the API returns it.
type Conversation struct {
	ID        string     `json:"id"`
	Title     string     `json:"title"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	Messages  []*Message `json:"messages"`
}

// The kinds of message.
const (
	KindRequest = "request" // the person's request, which opens a trace
	KindOutcome = "outcome" // the delivery of a trace's outcome (architecture §3.5.4)
	KindQuery   = "query"   // a hitl_contact_user query of a node of the trace (architecture §3.5.2)
)

// The states of an outcome or a query.
const (
	StateAwaiting = "awaiting" // the role retains it until the person answers
	StateAnswered = "answered"
	StateExpired  = "expired" // the role closed it without an answer
)

// Message is a message of a conversation.
type Message struct {
	ID          string       `json:"id"`
	Kind        string       `json:"kind"`
	Time        time.Time    `json:"time"`
	TraceID     string       `json:"trace_id"`
	Text        string       `json:"text,omitempty"`
	Attachments []Attachment `json:"attachments,omitempty"`
	// EntryTaskID is, in a request, its entry subtask, and in an outcome, the
	// attempt that produced it.
	EntryTaskID string `json:"entry_task_id,omitempty"`
	// Outcome is the type of the entry subtask's terminal event, with its
	// failure if it failed.
	Outcome       string `json:"outcome,omitempty"`
	FailureReason string `json:"failure_reason,omitempty"`
	FailureCode   string `json:"failure_code,omitempty"`
	// TaskID is the hitl_contact_user task the person answers: the delivery's
	// current attempt or the query's current round.
	TaskID  string          `json:"task_id,omitempty"`
	Options []string        `json:"options,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"` // the query's data, as it arrived
	State   string          `json:"state,omitempty"`
	// FinalRound marks the delivery's final round, after which an outcome
	// that did not reach the person does not come back (architecture §3.5.4).
	FinalRound bool            `json:"final_round,omitempty"`
	Answer     json.RawMessage `json:"answer,omitempty"`  // the data the console answered with
	Expired    string          `json:"expired,omitempty"` // unanswered | undelivered
}

// Attachment is an attachment of a message; its content lives in a file of its own.
type Attachment struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	MediaType string `json:"media_type"`
	Size      int64  `json:"size"`
}

// store reads and writes the encrypted files.
type store struct {
	dir  string
	aead cipher.AEAD
}

func openStore(dir string, key []byte) (*store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}
	return &store{dir: dir, aead: aead}, nil
}

// write encrypts plain into name through a temporary file, so an interrupted
// write leaves the previous content or none.
func (st *store) write(name string, plain []byte) error {
	nonce := make([]byte, st.aead.NonceSize(), st.aead.NonceSize()+len(plain)+st.aead.Overhead())
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	path := filepath.Join(st.dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, st.aead.Seal(nonce, nonce, plain, []byte(name)), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// read decrypts name; one that does not exist is fs.ErrNotExist.
func (st *store) read(name string) ([]byte, error) {
	b, err := os.ReadFile(filepath.Join(st.dir, name))
	if err != nil {
		return nil, err
	}
	n := st.aead.NonceSize()
	if len(b) < n {
		return nil, fmt.Errorf("console: %s: truncated", name)
	}
	plain, err := st.aead.Open(nil, b[:n], b[n:], []byte(name))
	if err != nil {
		return nil, fmt.Errorf("console: %s: %w", name, err)
	}
	return plain, nil
}

func (st *store) saveConversation(c *Conversation) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return st.write(filepath.Join(c.ID, "conversation"), b)
}

// loadConversations reads every conversation.
func (st *store) loadConversations() (map[string]*Conversation, error) {
	entries, err := os.ReadDir(st.dir)
	if err != nil {
		return nil, err
	}
	out := map[string]*Conversation{}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		b, err := st.read(filepath.Join(e.Name(), "conversation"))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var c Conversation
		if err := json.Unmarshal(b, &c); err != nil {
			return nil, err
		}
		out[c.ID] = &c
	}
	return out, nil
}

func (st *store) attachmentName(conversation, attachment string) string {
	return filepath.Join(conversation, attachment)
}

func (st *store) removeConversation(id string) error {
	return os.RemoveAll(filepath.Join(st.dir, id))
}

const fileDeleted = "deleted"

func (st *store) saveDeleted(traces map[string]bool) error {
	b, err := json.Marshal(traces)
	if err != nil {
		return err
	}
	return st.write(fileDeleted, b)
}

func (st *store) loadDeleted() (map[string]bool, error) {
	out := map[string]bool{}
	b, err := st.read(fileDeleted)
	if errors.Is(err, fs.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	return out, json.Unmarshal(b, &out)
}
