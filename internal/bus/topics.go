package bus

// TopicIntake is the entry topic, the only fixed-convention one an emergent
// node claims (architecture §3.7.7).
const TopicIntake = "intake"

// StructuralTopics are the fixed-convention topics with a structural claimant
// (architecture §3.7.7): admission there is by credential, without similarity
// (architecture §3.7.5, step 4).
var StructuralTopics = map[string]bool{
	"config_change":        true,
	"trace_query":          true,
	"node_review":          true,
	"node_query":           true,
	"node_catalog_query":   true,
	"node_catalog_publish": true,
	"node_catalog_import":  true,
	"node_provision":       true,
	"node_create":          true,
	"node_retire":          true,
	"topic_catalog_query":  true,
}
