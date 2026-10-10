package deploy

// names derives the engine's names from the network's (implementation §6, The
// Docker network): the container name carries the network because Docker requires
// it to be unique on the host.
type names struct{ network string }

func (n names) dockerNetwork() string        { return "zobik-" + n.network }
func (n names) container(role string) string { return "zobik-" + n.network + "-" + role }
func (n names) volume(role string) string    { return "zobik-" + n.network + "-" + role }
