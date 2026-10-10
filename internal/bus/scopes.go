package bus

// The subject families of the Bus (implementation §1.2.1).
var families = []string{"task.>", "prop.>", "lease.>", "notice.>"}

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
