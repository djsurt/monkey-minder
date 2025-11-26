package monkeyminder

import (
	clientapi "github.com/djsurt/monkey-minder/common/proto"
	"github.com/djsurt/monkey-minder/server/internal/tree"
	raftpb "github.com/djsurt/monkey-minder/server/proto/raft"
)

type ClientMessage interface {
	// true if this message must be forwarded to the leader,
	// false if this message may be processed on any node.
	IsLeaderOnly() bool

	// actually do the stuff corresponding to this message.
	// will only be executed on the node who is responsible for processing the event.
	//
	// currentState is the current state of the tree, and SHOULD NOT be modified by this method.
	//  instead, you should return one or more log entry as newEntries.
	// response is the response which will be passed back to the client.
	//  you DO NOT need to set the id field, that will be handled for you.
	//  you also DO NOT need to set any fields not relevant to your particular endpoint.
	//   (for reference, see the zookeeper paper, or look at which fields the corresponding client method accesses on the server response.)
	// newEntries is a list of new log entries to be appended to the log in order to do any tree modifications necessary.
	//  those who are making no changes should set it to nil.
	DoMessage(currentState *tree.Tree) (response *clientapi.ServerResponse, newEntries []*raftpb.LogEntry)

	// returns true if, based on the contents of entry, our watch should fire.
	// for those messages which do not support watches, returning false is sufficient.
	WatchTest(entry *raftpb.LogEntry) bool

	// this is like DoMessage but for processing watches.
	// for those message which do not support watches, a single line with a panic is sufficient.
	DoMessageWatch(currentState *tree.Tree) (response *clientapi.ServerResponse)
}

type messageId uint64
type Version int64

type SimpleMessageCommon struct {
	id messageId
}

type WatchMessageCommon struct {
	watchId messageId
}

type Create struct {
	SimpleMessageCommon
	path string
	data string
}

func (c *Create) IsLeaderOnly() bool {
	return true
}

func (c *Create) DoMessage(currentState *tree.Tree) (*clientapi.ServerResponse, []*raftpb.LogEntry) {
	entry := &raftpb.LogEntry{
		Kind:       raftpb.LogEntryType_CREATE,
		TargetPath: c.path,
		Value:      c.data,
	}

	response := &clientapi.ServerResponse{
		Success: true,
		Data:    &c.data,
	}

	return response, []*raftpb.LogEntry{entry}
}

func (c *Create) WatchTest(entry *raftpb.LogEntry) bool {
	return false
}

func (c *Create) DoMessageWatch(currentState *tree.Tree) *clientapi.ServerResponse {
	panic("Create does not support watches")
}

type Delete struct {
	SimpleMessageCommon
	path    string
	version Version
}

func (d *Delete) IsLeaderOnly() bool {
	return true
}

func (d *Delete) DoMessage(currentState *tree.Tree) (*clientapi.ServerResponse, []*raftpb.LogEntry) {
	_, err := currentState.Get(d.path)
	if err != nil {
		return &clientapi.ServerResponse{Success: false}, nil
	}

	entry := &raftpb.LogEntry{
		Kind:       raftpb.LogEntryType_DELETE,
		TargetPath: d.path,
	}

	return &clientapi.ServerResponse{Success: true}, []*raftpb.LogEntry{entry}
}

func (d *Delete) WatchTest(entry *raftpb.LogEntry) bool {
	return false
}

func (d *Delete) DoMessageWatch(currentState *tree.Tree) *clientapi.ServerResponse {
	panic("Delete does not support watches")
}

type Exists struct {
	SimpleMessageCommon
	WatchMessageCommon
	path string
}

func (e *Exists) IsLeaderOnly() bool {
	return false
}

func (e *Exists) DoMessage(currentState *tree.Tree) (*clientapi.ServerResponse, []*raftpb.LogEntry) {
	_, err := currentState.Get(e.path)
	exists := err == nil

	response := &clientapi.ServerResponse{
		Exists: exists,
	}

	return response, nil
}

func (e *Exists) WatchTest(entry *raftpb.LogEntry) bool {
	if entry.TargetPath != e.path {
		return false
	}
	// This watch fires on CREATE and DELETE events
	return entry.Kind == raftpb.LogEntryType_CREATE || entry.Kind == raftpb.LogEntryType_DELETE
}

func (e *Exists) DoMessageWatch(currentState *tree.Tree) *clientapi.ServerResponse {
	_, err := currentState.Get(e.path)
	return &clientapi.ServerResponse{
		Exists: err == nil,
	}
}

type GetData struct {
	SimpleMessageCommon
	WatchMessageCommon
	path string
}

func (m *GetData) IsLeaderOnly() bool {
	return false
}

// Retrieve the requested value from the currentState. If any error occurs,
// return a response w/ Success = false.
func (m *GetData) DoMessage(currentState *tree.Tree) (
	response *clientapi.ServerResponse,
	newEntries []*raftpb.LogEntry,
) {
	// Never need to apply new entries for a read
	newEntries = nil

	data, err := currentState.Get(m.path)
	if err != nil {
		response.Succeeded = false
		return
	}
	response.Succeeded = true
	response.Data = &data
	return
}

// If the entry modifies the value of m.path, returns true.
func (m *GetData) WatchTest(entry *raftpb.LogEntry) bool {
	isMyTarget := m.path == entry.TargetPath
	isModification := (entry.Kind == raftpb.LogEntryType_UPDATE ||
		entry.Kind == raftpb.LogEntryType_DELETE)

	return isMyTarget && isModification
}

func (m *GetData) DoMessageWatch(currentState *tree.Tree) (
	response *clientapi.ServerResponse,
) {
	panic("Not implemented!")
}

type SetData struct {
	SimpleMessageCommon
	path    string
	data    string
	version Version
}

func (m *SetData) IsLeaderOnly() bool {
	return true
}

// Update the value of m.path to data if version == version.
// Fails if the version number doesn't match or if the path doesn't
// exist.
func (m *SetData) DoMessage(currentState *tree.Tree) (
	response *clientapi.ServerResponse,
	newEntries []*raftpb.LogEntry,
) {
	// First check if the node exists
	version, err := currentState.GetVersion(m.path)
	if err != nil {
		response.Succeeded = false
		return
	}

	// Check version number.
	if m.version != -1 && m.version != Version(version) {
		response.Succeeded = false
		return
	}

	response.Succeeded = true
	newEntries = append(newEntries, &raftpb.LogEntry{
		Kind:       raftpb.LogEntryType_UPDATE,
		TargetPath: m.path,
		Value:      m.data,
	})
	return
}

func (m *SetData) WatchTest(entry *raftpb.LogEntry) bool {
	return false
}

func (m *SetData) DoMessageWatch(currentState *tree.Tree) (
	response *clientapi.ServerResponse,
) {
	panic("SetData does not support watches")
}

type GetChildren struct {
	SimpleMessageCommon
	WatchMessageCommon
	path string
}

// GetChildren can be served locally.
func (m *GetChildren) IsLeaderOnly() bool {
	return false
}

// Get the absolute paths of all children of the given path.
// Fails if the node does not exist.
func (m *GetChildren) DoMessage(currentState *tree.Tree) (
	response *clientapi.ServerResponse,
	newEntries []*raftpb.LogEntry,
) {
	children, err := currentState.GetChildren(m.path)
	if err != nil {
		response.Succeeded = false
		return
	}

	response.Succeeded = true
	response.Children = children
	return
}

func (m *GetChildren) WatchTest(entry *raftpb.LogEntry) bool {
	return false
}

func (m *GetChildren) DoMessageWatch(currentState *tree.Tree) (
	response *clientapi.ServerResponse,
) {
	panic("GetChildren does not support watches.")
}
