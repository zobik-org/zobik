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

// Expr is a compiled expression of a declared type.
type Expr struct {
	Expr string
	prg  cel.Program
}

// Compile compiles expr against env and requires it to be of type want.
func Compile(env *cel.Env, expr string, want *types.Type) (*Expr, error) {
	ast, iss := env.Compile(expr)
	if iss.Err() != nil {
		return nil, fmt.Errorf("rules: %q: %w", expr, iss.Err())
	}
	if !ast.OutputType().IsExactType(want) {
		return nil, fmt.Errorf("rules: %q is %s, not %s", expr, ast.OutputType(), want)
	}
	prg, err := env.Program(ast)
	if err != nil {
		return nil, err
	}
	return &Expr{Expr: expr, prg: prg}, nil
}

// Eval evaluates the expression over vars. An evaluation error comes out as an
// error and not as a value (implementation §1.2.12).
func (e *Expr) Eval(vars map[string]any) (any, error) {
	out, _, err := e.prg.Eval(vars)
	if err != nil {
		return nil, fmt.Errorf("rules: %q: %w", e.Expr, err)
	}
	return out.Value(), nil
}

// Bool is a compiled condition that evaluates to a bool.
type Bool struct{ *Expr }

// CompileBool compiles expr against env and requires it to be a bool.
func CompileBool(env *cel.Env, expr string) (*Bool, error) {
	e, err := Compile(env, expr, types.BoolType)
	if err != nil {
		return nil, err
	}
	return &Bool{e}, nil
}

// Eval evaluates the condition over vars.
func (b *Bool) Eval(vars map[string]any) (bool, error) {
	out, err := b.Expr.Eval(vars)
	if err != nil {
		return false, err
	}
	v, ok := out.(bool)
	if !ok {
		return false, errors.New("rules: not a bool")
	}
	return v, nil
}
