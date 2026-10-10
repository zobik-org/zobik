package bus

// The subject families of the Bus (implementation §1.2.1).
var families = []string{"task.>", "prop.>", "lease.>", "notice.>"}

// The JetStream resources outside the families: the Config Store's head and
// history (implementation §1.2.5) and the Task Broker's state (implementation §1.2.7).
const (
	BucketConfig  = "config"
	BucketTasks   = "tasks"
	StreamHistory = "config_history"
	// SubjectHistory prefixes each published version: config.history.<config_version>.
	SubjectHistory = "config.history"
)

// ConsoleScope is the console's: it creates and updates the streams, consumers
// and buckets in the acts of deployment (implementation §6, Installation and zobik init).
var ConsoleScope = Scope{
	Role:      "console",
	Publish:   []string{"$JS.API.>"},
	Subscribe: []string{"_INBOX.>"},
}

// TapScope is zobik tap's: it subscribes to every family and publishes nothing.
// zobik init registers it only in a development binary.
var TapScope = Scope{
	Role:      "tap",
	Subscribe: families,
}

// PublishScope is zobik publish's: it publishes events by hand on the stream
// families, and subscribes only to the inbox of the stream's acknowledgment.
// zobik init registers it only in a development binary.
var PublishScope = Scope{
	Role:      "publish",
	Publish:   []string{"task.>", "prop.>", "notice.>"},
	Subscribe: []string{"_INBOX.>"},
}

// The scopes of the structural roles. {{subject()}} expands to the identity's
// public nkey, so each permission that carries it names only its holder
// (implementation §1.2.1).
//
// Every role pulls from durable consumers named by its identity, which zobik init
// creates, and receives replies on an inbox of its own: InboxPrefix.
const self = "{{subject()}}"

// InboxPrefix is the reply prefix of identity's connection, the only inbox its
// scope lets it subscribe to.
func InboxPrefix(identity string) string { return "_INBOX." + identity }

var (
	consumerPub = []string{
		"$JS.API.CONSUMER.INFO.*." + self,
		"$JS.API.CONSUMER.MSG.NEXT.*." + self,
		"$JS.ACK.*." + self + ".>",
	}
	inboxSub = []string{"_INBOX." + self + ".>"}

	// configRead reads the head and a version of the history (architecture §2.3).
	configRead = []string{
		"$JS.API.STREAM.INFO.KV_" + BucketConfig,
		"$JS.API.DIRECT.GET.KV_" + BucketConfig + ".>",
		"$JS.API.STREAM.INFO." + StreamHistory,
		"$JS.API.DIRECT.GET." + StreamHistory,
		"$JS.API.DIRECT.GET." + StreamHistory + ".>",
	}

	// participant is what a node publishes in the CNP cycle: its proposals and
	// renewals under its own identity, and the terminal events of any destination
	// (implementation §1.2.1).
	participant = []string{
		"prop." + self + ".>",
		"lease." + self + ".>",
		"task.completed.>",
		"task.failed.>",
	}
)

func join(lists ...[]string) []string {
	var out []string
	for _, l := range lists {
		out = append(out, l...)
	}
	return out
}

// ConfigScope is the Config Store's: it claims config_change and writes the
// head and the history (implementation §1.2.5).
var ConfigScope = Scope{
	Role: "config",
	Publish: join(consumerPub, configRead, participant, []string{
		"$KV." + BucketConfig + ".>",
		SubjectHistory + ".>",
	}),
	Subscribe: inboxSub,
}

// TaskBrokerScope is the Task Broker's: it re-announces, awards and rejects,
// receives the renewals on NATS core and keeps its state in its bucket
// (implementation §1.2.1, §1.2.7). It is the only one that publishes
// task.assigned and task.rejected.
var TaskBrokerScope = Scope{
	Role: "task_broker",
	Publish: join(consumerPub, configRead, []string{
		"task.announced.>",
		"task.assigned.*",
		"task.rejected.*",
		"$KV." + BucketTasks + ".>",
		"$JS.API.STREAM.INFO.KV_" + BucketTasks,
		"$JS.API.STREAM.UPDATE.KV_" + BucketTasks,
		"$JS.API.DIRECT.GET.KV_" + BucketTasks + ".>",
		"$JS.API.CONSUMER.CREATE.KV_" + BucketTasks + ".>",
		"$JS.API.CONSUMER.DELETE.KV_" + BucketTasks + ".>",
	}),
	Subscribe: join(inboxSub, []string{"lease.>"}),
}

// ContextScope is the Context Store's: it observes the terminal events to purge
// on trace close (implementation §1.2.9), and publishes no event.
var ContextScope = Scope{
	Role:      "context",
	Publish:   join(consumerPub, configRead),
	Subscribe: inboxSub,
}

// ChannelScope is the channel role's: it opens traces and their entry subtasks,
// and claims and closes the queries of its audience (architecture §3.5).
var ChannelScope = Scope{
	Role:      "channel",
	Publish:   join(consumerPub, configRead, participant, []string{"task.announced.>"}),
	Subscribe: inboxSub,
}

// RoleScopes are the scopes zobik init registers for the structural roles.
var RoleScopes = []Scope{ConfigScope, TaskBrokerScope, ContextScope, ChannelScope}
