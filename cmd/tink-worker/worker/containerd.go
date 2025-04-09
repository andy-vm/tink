package worker

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/oci"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/containers/image/v5/pkg/shortnames"
	"github.com/containers/image/v5/types"
	"github.com/go-logr/logr"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/pkg/errors"
	"github.com/tinkerbell/tink/internal/proto"
)

var (
	_ ContainerManager = (*containerdManager)(nil)
	_ LogCapturer      = (*containerdLogCapturer)(nil)
)

const (
	namespace  = "tinkerbell"
	socketPath = "/run/containerd/containerd.sock"
)

type containerdManager struct {
	logger          logr.Logger
	registryDetails RegistryConnDetails
	namespace       string
	client          *containerd.Client
	socketPath      string
}

func NewContainerdManager(logger logr.Logger, registryDetails RegistryConnDetails) ContainerManager {
	client, err := containerd.New(socketPath, containerd.WithDefaultNamespace(namespace))
	if err != nil {
		panic(fmt.Errorf("error creating containerd client: %w", err))
	}
	return &containerdManager{
		logger:          logger,
		registryDetails: registryDetails,
		namespace:       namespace,
		socketPath:      socketPath,
		client:          client,
	}
}

// CreateContainer implements ContainerManager.
func (c *containerdManager) CreateContainer(ctx context.Context, cmd []string, wfID string, action *proto.WorkflowAction,
	captureLogs bool, privileged bool) (string, error) {
	l := c.logger.WithValues("action", action.GetName(), "workflowID", wfID)
	l.Info("creating container", "command", cmd)

	// set up a containerd namespace
	ctx = namespaces.WithNamespace(ctx, c.namespace)

	imageName := path.Join(c.registryDetails.Registry, action.GetImage())
	image, err := c.pullImage(ctx, imageName)
	if err != nil {
		return "", err
	}

	// Prepare workflow directory and mounts
	wfDir := filepath.Join(defaultDataDir, wfID)
	mounts := []specs.Mount{
		{
			Source:      wfDir,
			Destination: "/workflow",
			Type:        "bind",
			Options:     []string{"rbind"},
		},
	}

	// Add additional volumes from the action
	for _, volume := range action.GetVolumes() {
		mounts = append(mounts, specs.Mount{
			Source:      volume,
			Destination: volume,
			Type:        "bind",
			Options:     []string{"rbind"},
		})
	}

	// Create the container specification
	opts := []oci.SpecOpts{
		oci.WithImageConfig(image),
		oci.WithProcessArgs(cmd...),
		oci.WithEnv(action.GetEnvironment()),
		oci.WithMounts(mounts),
		oci.WithCapabilities([]string{"CAP_SYS_ADMIN"}),
	}

	if privileged {
		opts = append(opts, oci.WithPrivileged)
	}

	if pidConfig := action.GetPid(); pidConfig != "" {
		opts = append(opts, oci.WithLinuxNamespace(specs.LinuxNamespace{
			Type: specs.PIDNamespace,
			Path: pidConfig,
		}))
	}

	name := makeValidContainerName(fmt.Sprintf("%s-%s", wfID, action.GetName()))
	container, err := c.client.NewContainer(
		ctx,
		name,
		containerd.WithNewSpec(opts...),
		containerd.WithImage(image),
		containerd.WithNewSnapshot(name, image),
	)
	if err != nil {
		return "", errors.Wrap(err, "CONTAINERD CREATE")
	}

	return container.ID(), nil
}

// PullImage implements ContainerManager.
func (c *containerdManager) PullImage(ctx context.Context, imageName string) error {
	// set up a containerd namespace
	ctx = namespaces.WithNamespace(ctx, c.namespace)
	_, err := c.pullImage(ctx, imageName)
	return err
}

func (c *containerdManager) pullImage(ctx context.Context, imageName string) (containerd.Image, error) {
	l := c.logger.WithValues("image", imageName)
	l.Info("pulling image")

	r, err := shortnames.Resolve(&types.SystemContext{PodmanOnlyShortNamesIgnoreRegistriesConfAndForceDockerHub: true}, imageName)
	if err != nil {
		l.Info("unable to resolve image fully qualified name", "error", err)
	}
	if r != nil && len(r.PullCandidates) > 0 {
		imageName = r.PullCandidates[0].Value.String()
	}

	image, err := c.client.GetImage(ctx, imageName)
	if err != nil {
		// Create a resolver with authentication details
		resolver := docker.NewResolver(docker.ResolverOptions{
			Hosts: func(host string) ([]docker.RegistryHost, error) {
				return []docker.RegistryHost{
					{
						Host: c.registryDetails.Registry,
						Authorizer: docker.NewDockerAuthorizer(docker.WithAuthCreds(func(host string) (string, string, error) {
							return c.registryDetails.Username, c.registryDetails.Password, nil
						})),
						Capabilities: docker.HostCapabilityPull,
					},
				}, nil
			},
		})
		// if the image is not in namespaced context, then pull it
		image, err = c.client.Pull(ctx, imageName, containerd.WithPullUnpack, containerd.WithResolver(resolver))
		if err != nil {
			return image, fmt.Errorf("error pulling image: %w", err)
		}
	}

	l.Info("image pulled", "image", image.Name())
	return image, nil
}

// RemoveContainer implements ContainerManager.
func (c *containerdManager) RemoveContainer(ctx context.Context, id string) error {
	l := c.logger.WithValues("containerID", id)
	l.Info("removing container")
	// set up a containerd namespace
	ctx = namespaces.WithNamespace(ctx, c.namespace)

	container, err := c.client.LoadContainer(ctx, id)
	if err != nil {
		return errors.Wrap(err, "failed to load container")
	}

	// delete the container
	err = container.Delete(ctx, containerd.WithSnapshotCleanup)
	if err != nil {
		return errors.Wrap(err, "CONTAINERD REMOVE")
	}

	return nil
}

// StartContainer implements ContainerManager.
func (c *containerdManager) StartContainer(ctx context.Context, id string) error {
	l := c.logger.WithValues("containerID", id)
	l.Info("starting container")
	// set up a containerd namespace
	ctx = namespaces.WithNamespace(ctx, c.namespace)

	container, err := c.client.LoadContainer(ctx, id)
	if err != nil {
		return errors.Wrap(err, "CONTAINERD LOAD")
	}

	// Create the task
	task, err := container.NewTask(ctx, cio.NewCreator(cio.WithStdio))
	if err != nil {
		return errors.Wrap(err, "CONTAINERD TASK CREATE")
	}

	// Start the task
	if err := task.Start(ctx); err != nil {
		_, _ = task.Delete(ctx)
		return errors.Wrap(err, "CONTAINERD TASK START")
	}

	return nil
}

// WaitForContainer implements ContainerManager.
func (c *containerdManager) WaitForContainer(ctx context.Context, id string) (proto.State, error) {
	l := c.logger.WithValues("containerID", id)
	l.Info("waiting container")
	// set up a containerd namespace
	ctx = namespaces.WithNamespace(ctx, c.namespace)

	container, err := c.client.LoadContainer(ctx, id)
	if err != nil {
		return proto.State_STATE_FAILED, err
	}

	// get the task associated with the container
	task, err := container.Task(ctx, nil)
	if err != nil {
		return proto.State_STATE_FAILED, err
	}

	var exitStatusC <-chan containerd.ExitStatus
	exitStatusC, err = task.Wait(ctx)
	if err != nil {
		return proto.State_STATE_FAILED, fmt.Errorf("error waiting on task: %w", err)
	}

	select {
	case exitStatus := <-exitStatusC:
		if exitStatus.ExitCode() == 0 {
			return proto.State_STATE_SUCCESS, nil
		}
		return proto.State_STATE_FAILED, nil
	case <-ctx.Done():
		return proto.State_STATE_TIMEOUT, ctx.Err()
	}
}

// WaitForFailedContainer implements ContainerManager.
func (c *containerdManager) WaitForFailedContainer(ctx context.Context, id string, failedActionStatus chan proto.State) {
	l := c.logger.WithValues("containerID", id)
	l.Info("waiting failed container")
	// set up a containerd namespace
	ctx = namespaces.WithNamespace(ctx, c.namespace)

	container, err := c.client.LoadContainer(ctx, id)
	if err != nil {
		failedActionStatus <- proto.State_STATE_FAILED
		return
	}

	// get the task associated with the container
	task, err := container.Task(ctx, nil)
	if err != nil {
		l.Error(err, "error loading task")
		failedActionStatus <- proto.State_STATE_FAILED
		return
	}

	var exitStatusC <-chan containerd.ExitStatus
	exitStatusC, err = task.Wait(ctx)
	if err != nil {
		l.Error(err, "error waiting on task")
		failedActionStatus <- proto.State_STATE_FAILED
		return
	}

	select {
	case exitStatus := <-exitStatusC:
		if exitStatus.ExitCode() == 0 {
			failedActionStatus <- proto.State_STATE_SUCCESS
			return
		}
		failedActionStatus <- proto.State_STATE_FAILED
	case <-ctx.Done():
		l.Error(ctx.Err(), "context done")
		failedActionStatus <- proto.State_STATE_TIMEOUT
	}
}

type containerdLogCapturer struct{}

func NewContainerdLogCapturer() LogCapturer {
	return &containerdLogCapturer{}
}

// CaptureLogs streams container logs to the capturer's writer.
func (l *containerdLogCapturer) CaptureLogs(ctx context.Context, id string) {}

func Init() error {
	content, err := os.ReadFile("/proc/cmdline")
	if err != nil {
		return err
	}
	cmdLines := strings.Split(string(content), " ")
	cfg := parseCmdLine(cmdLines)
	envs := []string{
		fmt.Sprintf("DOCKER_REGISTRY=%s", cfg.registry),
		fmt.Sprintf("REGISTRY_USERNAME=%s", cfg.username),
		fmt.Sprintf("REGISTRY_PASSWORD=%s", cfg.password),
		fmt.Sprintf("TINKERBELL_GRPC_AUTHORITY=%s", cfg.grpcAuthority),
		fmt.Sprintf("TINKERBELL_TLS=%s", cfg.tinkServerTLS),
		fmt.Sprintf("TINKERBELL_INSECURE_TLS=%s", cfg.tinkServerInsecureTLS),
		fmt.Sprintf("WORKER_ID=%s", cfg.workerID),
		fmt.Sprintf("ID=%s", cfg.workerID),
		fmt.Sprintf("HTTP_PROXY=%s", cfg.httpProxy),
		fmt.Sprintf("HTTPS_PROXY=%s", cfg.httpsProxy),
		fmt.Sprintf("NO_PROXY=%s", cfg.noProxy),
	}

	for _, env := range envs {
		kv := splitEnv(env)
		if err := os.Setenv(kv[0], kv[1]); err != nil {
			return fmt.Errorf("failed to set environment variable %s: %w", kv[0], err)
		}
	}
	return nil
}

type tinkWorkerConfig struct {
	// Registry configuration
	registry string
	username string
	password string

	// Tink Server GRPC address:port
	grpcAuthority string

	// Worker ID
	workerID string

	// tinkWorkerImage is the Tink worker image location.
	tinkWorkerImage string

	// tinkServerTLS is whether or not to use TLS for tink-server communication.
	tinkServerTLS string

	// tinkServerInsecureTLS is whether or not to use insecure TLS for tink-server communication; only applies is TLS itself is on
	tinkServerInsecureTLS string

	httpProxy  string
	httpsProxy string
	noProxy    string
}

func parseCmdLine(cmdLines []string) (cfg tinkWorkerConfig) {
	for i := range cmdLines {
		cmdLine := strings.SplitN(cmdLines[i], "=", 2)
		if len(cmdLine) == 0 {
			continue
		}

		switch cmd := cmdLine[0]; cmd {
		case "docker_registry":
			cfg.registry = cmdLine[1]
		case "registry_username":
			cfg.username = cmdLine[1]
		case "registry_password":
			cfg.password = cmdLine[1]
		case "grpc_authority":
			cfg.grpcAuthority = cmdLine[1]
		case "worker_id":
			cfg.workerID = cmdLine[1]
		case "tink_worker_image":
			cfg.tinkWorkerImage = cmdLine[1]
		case "tinkerbell_tls":
			cfg.tinkServerTLS = cmdLine[1]
		case "tinkerbell_insecure_tls":
			cfg.tinkServerInsecureTLS = cmdLine[1]
		case "HTTP_PROXY":
			cfg.httpProxy = cmdLine[1]
		case "HTTPS_PROXY":
			cfg.httpsProxy = cmdLine[1]
		case "NO_PROXY":
			cfg.noProxy = cmdLine[1]
		}
	}
	return cfg
}

func splitEnv(env string) []string {
	kv := strings.SplitN(env, "=", 2)
	if len(kv) == 2 {
		return kv
	}
	return nil
}
