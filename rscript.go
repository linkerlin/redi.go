package redi

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// ScriptReturnType selects the cast applied to a Lua script reply,
// mirroring Redisson's RScript.ReturnType.
type ScriptReturnType int

const (
	ScriptReturnValue ScriptReturnType = iota
	ScriptReturnBoolean
	ScriptReturnInteger
	ScriptReturnStatus
)

// RScript evaluates Lua scripts with cached digests (EVAL/EVALSHA),
// mirroring Redisson's RScript.
type RScript struct {
	c *Client
}

func newRScript(c *Client) *RScript { return &RScript{c: c} }

// Eval runs a script with keys and argument values, casting the reply
// per rt. A Lua nil reply decodes to (nil, nil).
func (s *RScript) Eval(ctx context.Context, script string, rt ScriptReturnType, keys []string, values ...any) (any, error) {
	res, err := redis.NewScript(script).Run(ctx, s.c.rc, keys, values...).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return castScriptReply(res, rt), nil
}

// EvalSha runs a script by SHA digest (the script must already be loaded).
func (s *RScript) EvalSha(ctx context.Context, sha string, rt ScriptReturnType, keys []string, values ...any) (any, error) {
	res, err := s.c.rc.EvalSha(ctx, sha, keys, values...).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return castScriptReply(res, rt), nil
}

// ScriptLoad loads a script and returns its SHA digest.
func (s *RScript) ScriptLoad(ctx context.Context, script string) (string, error) {
	return s.c.rc.ScriptLoad(ctx, script).Result()
}

// ScriptExists reports which digests are cached on the server.
func (s *RScript) ScriptExists(ctx context.Context, shas ...string) ([]bool, error) {
	return s.c.rc.ScriptExists(ctx, shas...).Result()
}

func castScriptReply(res any, rt ScriptReturnType) any {
	switch rt {
	case ScriptReturnBoolean:
		if n, ok := res.(int64); ok {
			return n != 0
		}
		return res
	case ScriptReturnInteger:
		if n, ok := res.(int64); ok {
			return n
		}
		return res
	case ScriptReturnStatus:
		if str, ok := res.(string); ok {
			return str
		}
		return res
	default:
		return res
	}
}
