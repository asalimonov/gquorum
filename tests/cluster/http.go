//go:build cluster

package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/meta"
	"github.com/asalimonov/gquorum/topology"
)

type Response struct {
	Status int
	Header http.Header
	Data   []byte
}

func (r Response) Decode(target any) error { return json.Unmarshal(r.Data, target) }
func (c *Cluster) Request(ctx context.Context, name, method, path string, body any, expected uint64) (Response, error) {
	var response Response
	node := c.Node(name)
	if node == nil || node.Endpoint() == "" {
		return response, fmt.Errorf("node %s has no running HTTP endpoint", name)
	}
	var encoded []byte
	var err error
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil {
			return response, err
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, node.Endpoint()+path, bytes.NewReader(encoded))
	if err != nil {
		return response, err
	}
	request.Header.Set("Content-Type", "application/json")
	if strings.HasPrefix(path, "/internal/test/") {
		request.Header.Set("X-Gquorum-Test-Credential", c.Credential)
	}
	if expected > 0 {
		request.Header.Set("If-Match", strconv.FormatUint(expected, 10))
	}
	result, err := c.http.Do(request)
	if err != nil {
		return response, err
	}
	defer result.Body.Close()
	response.Status, response.Header = result.StatusCode, result.Header.Clone()
	response.Data, err = io.ReadAll(io.LimitReader(result.Body, 16<<20))
	return response, err
}
func requireStatus(response Response, err error, status int) error {
	if err != nil {
		return err
	}
	if response.Status != status {
		return fmt.Errorf("HTTP %d, wanted %d: %s", response.Status, status, response.Data)
	}
	return nil
}
func (c *Cluster) Document(ctx context.Context, node, view string) (topology.Document, error) {
	response, err := c.Request(ctx, node, "GET", "/api/topology/"+view, nil, 0)
	var doc topology.Document
	if err := requireStatus(response, err, 200); err != nil {
		return doc, err
	}
	return doc, response.Decode(&doc)
}
func (c *Cluster) Status(ctx context.Context, node string) (topology.ClusterStatus, error) {
	response, err := c.Request(ctx, node, "GET", "/api/cluster/status", nil, 0)
	var status topology.ClusterStatus
	if err := requireStatus(response, err, 200); err != nil {
		return status, err
	}
	return status, response.Decode(&status)
}
func (c *Cluster) Execution(ctx context.Context, node string, id topology.ExecutionID) (topology.ExecutionStatus, error) {
	response, err := c.Request(ctx, node, "GET", "/api/executions/"+string(id), nil, 0)
	var status topology.ExecutionStatus
	if err := requireStatus(response, err, 200); err != nil {
		return status, err
	}
	return status, response.Decode(&status)
}
func Poll(ctx context.Context, check func() (bool, error)) error {
	ticker := time.NewTicker(60 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		done, err := check()
		if err != nil {
			last = err
		}
		if done {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), last)
		case <-ticker.C:
		}
	}
}
func (c *Cluster) WaitExecution(ctx context.Context, node string, id topology.ExecutionID) error {
	return Poll(ctx, func() (bool, error) {
		status, err := c.Execution(ctx, node, id)
		if err != nil {
			return false, err
		}
		if status.State.Terminal() && status.State != topology.ExecutionSucceeded {
			return true, fmt.Errorf("execution %s terminated: %+v", id, status)
		}
		return status.State == topology.ExecutionSucceeded, nil
	})
}
func (c *Cluster) Mutate(ctx context.Context, node, method, path string, body any) (meta.MutationResult, error) {
	var result meta.MutationResult
	doc, err := c.Document(ctx, node, "latest")
	if err != nil {
		return result, err
	}
	response, err := c.Request(ctx, node, method, path, body, doc.Version)
	if err := requireStatus(response, err, 202); err != nil {
		return result, err
	}
	if err := response.Decode(&result); err != nil {
		return result, err
	}
	if err := c.WaitExecution(ctx, node, result.ExecutionID); err != nil {
		return result, err
	}
	err = c.WaitActive(ctx, result.Document.Version)
	return result, err
}

func (c *Cluster) WaitActive(ctx context.Context, version uint64) error {
	return Poll(ctx, func() (bool, error) {
		for _, n := range c.Nodes() {
			if !n.running.Load() {
				continue
			}
			doc, err := c.Document(ctx, n.Spec.Name, "active")
			if err != nil {
				return false, err
			}
			if doc.Version != version {
				return false, nil
			}
		}
		return true, nil
	})
}

func (c *Cluster) Stage(ctx context.Context, node string, doc topology.Document) (meta.MutationResult, error) {
	var result meta.MutationResult
	response, err := c.Request(ctx, node, "PUT", "/api/topology", doc, doc.Version)
	if err := requireStatus(response, err, 200); err != nil {
		return result, err
	}
	return result, response.Decode(&result)
}

func (c *Cluster) Apply(ctx context.Context, node string, version uint64) (topology.ExecutionID, error) {
	var result struct {
		ExecutionID topology.ExecutionID `json:"execution_id"`
	}
	response, err := c.Request(ctx, node, "POST", fmt.Sprintf("/api/topology/%d/apply", version), nil, 0)
	if err := requireStatus(response, err, 202); err != nil {
		return "", err
	}
	if err := response.Decode(&result); err != nil {
		return "", err
	}
	if result.ExecutionID == "" {
		return "", errors.New("apply acknowledged without an execution identity")
	}
	if err := c.WaitExecution(ctx, node, result.ExecutionID); err != nil {
		return result.ExecutionID, err
	}
	return result.ExecutionID, c.WaitActive(ctx, version)
}

func (c *Cluster) CreateGroup(ctx context.Context, node string, spec meta.GroupSpec) (meta.MutationResult, error) {
	return c.Mutate(ctx, node, "POST", "/api/groups", map[string]any{"group": spec, "mutation": meta.MutationOptions{Apply: true, Actor: "cluster-test"}})
}
func (c *Cluster) Put(ctx context.Context, node, group, key, id, value string) error {
	response, err := c.Request(ctx, node, "PUT", "/api/load/"+group+"/keys/"+key, map[string]string{"request_id": id, "value": value}, 0)
	return requireStatus(response, err, 200)
}

type Value struct {
	Value       string `json:"value"`
	Found       bool   `json:"found"`
	Index       uint64 `json:"applied_index"`
	Node        string `json:"node"`
	Consistency string `json:"consistency"`
}

func (c *Cluster) Get(ctx context.Context, node, group, key string) (Value, error) {
	var value Value
	response, err := c.Request(ctx, node, "GET", "/api/load/"+group+"/keys/"+key, nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		return value, err
	}
	return value, response.Decode(&value)
}
func (c *Cluster) WaitValue(ctx context.Context, node, group, key, expected string) error {
	return Poll(ctx, func() (bool, error) {
		value, err := c.Get(ctx, node, group, key)
		return value.Found && value.Value == expected && value.Node == node && value.Consistency == "local", err
	})
}
