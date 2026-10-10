package rules

import (
	"errors"
	"fmt"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
)

// Var is a variable a condition reads, with its CEL type.
type Var struct {
	Name string
	Type *types.Type
}

// NewEnv returns an environment over the provider's types and vars.
func NewEnv(p *Provider, vars ...Var) (*cel.Env, error) {
	opts := []cel.EnvOption{cel.CustomTypeProvider(p)}
	for _, v := range vars {
		opts = append(opts, cel.Variable(v.Name, v.Type))
	}
	return cel.NewEnv(opts...)
}

// Bool is a compiled condition that evaluates to a bool.
type Bool struct {
	Expr string
	prg  cel.Program
}

// CompileBool compiles expr against env and requires it to be a bool.
func CompileBool(env *cel.Env, expr string) (*Bool, error) {
	ast, iss := env.Compile(expr)
	if iss.Err() != nil {
		return nil, fmt.Errorf("rules: %q: %w", expr, iss.Err())
	}
	if !ast.OutputType().IsExactType(types.BoolType) {
		return nil, fmt.Errorf("rules: %q is %s, not bool", expr, ast.OutputType())
	}
	prg, err := env.Program(ast)
	if err != nil {
		return nil, err
	}
	return &Bool{Expr: expr, prg: prg}, nil
}

// Eval evaluates the condition over vars. An evaluation error comes out as an
// error and not as false (implementation §1.2.12).
func (b *Bool) Eval(vars map[string]any) (bool, error) {
	out, _, err := b.prg.Eval(vars)
	if err != nil {
		return false, fmt.Errorf("rules: %q: %w", b.Expr, err)
	}
	v, ok := out.Value().(bool)
	if !ok {
		return false, errors.New("rules: not a bool")
	}
	return v, nil
}
