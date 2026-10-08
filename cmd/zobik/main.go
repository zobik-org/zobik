// Command zobik is the operator console, the command line and every
// structural role of a Zobik network, each run as zobik run --role=<role>.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "zobik: no commands yet")
	os.Exit(2)
}
