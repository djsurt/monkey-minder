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

// TODO: implement ClientMessage
type Create struct {
	SimpleMessageCommon
	path string
	data string
}

// TODO: implement ClientMessage
type Delete struct {
	SimpleMessageCommon
	path    string
	version Version
}

// TODO: implement ClientMessage
type Exists struct {
	SimpleMessageCommon
	WatchMessageCommon
	path string
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
	isModification := (entry.Kind == raftpb.LogEntryType_CREATE ||
		entry.Kind == raftpb.LogEntryType_UPDATE ||
		entry.Kind == raftpb.LogEntryType_DELETE)

	return isMyTarget && isModification
}

func (m *GetData) DoMessageWatch(currentState *tree.Tree) (
	response *clientapi.ServerResponse,
) {
	panic("Not implemented!")
}

// TODO: implement ClientMessage
type SetData struct {
	SimpleMessageCommon
	path    string
	data    string
	version Version
}

// TODO: implement ClientMessage
type GetChildren struct {
	SimpleMessageCommon
	WatchMessageCommon
	path string
}
