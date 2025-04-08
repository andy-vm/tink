package worker

import (
	"context"

	"github.com/go-logr/logr"
	"github.com/tinkerbell/tink/internal/proto"
)

var (
	_ ContainerManager = (*containerdManager)(nil)
	_ LogCapturer      = (*containerdLogCapturer)(nil)
)

type containerdManager struct {
	logger          logr.Logger
	registryDetails RegistryConnDetails
}

func NewContainerdManager(logger logr.Logger, registryDetails RegistryConnDetails) ContainerManager {
	return &containerdManager{logger: logger, registryDetails: registryDetails}
}

// CreateContainer implements ContainerManager.
func (c *containerdManager) CreateContainer(ctx context.Context, cmd []string, wfID string, action *proto.WorkflowAction, captureLogs bool, privileged bool) (string, error) {
	return "", nil
}

// PullImage implements ContainerManager.
func (c *containerdManager) PullImage(ctx context.Context, image string) error {
	return nil
}

// RemoveContainer implements ContainerManager.
func (c *containerdManager) RemoveContainer(ctx context.Context, id string) error {
	return nil
}

// StartContainer implements ContainerManager.
func (c *containerdManager) StartContainer(ctx context.Context, id string) error {
	return nil
}

// WaitForContainer implements ContainerManager.
func (c *containerdManager) WaitForContainer(ctx context.Context, id string) (proto.State, error) {
	return 0, nil
}

// WaitForFailedContainer implements ContainerManager.
func (c *containerdManager) WaitForFailedContainer(ctx context.Context, id string, failedActionStatus chan proto.State) {

}

type containerdLogCapturer struct{}

func NewContainerdLogCapturer() LogCapturer {
	return &containerdLogCapturer{}
}

// CaptureLogs streams container logs to the capturer's writer.
func (l *containerdLogCapturer) CaptureLogs(ctx context.Context, id string) {}
