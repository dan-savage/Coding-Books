//go:build integration
// +build integration

package nsq

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

type container struct {
	testcontainers.Container
	IP   string
	Port string
}

func setupNSQ(ctx context.Context) (*container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "nsqio/nsq",
		ExposedPorts: []string{"4150/tcp"},
		Cmd:          []string{"nsqd"},
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	ip, err := c.Host(ctx)
	if err != nil {
		return nil, err
	}

	mappedPort, err := c.MappedPort(ctx, "4150")
	if err != nil {
		return nil, err
	}

	return &container{Container: c, IP: ip, Port: mappedPort.Port()}, nil
}

func TestNSQQueueMux(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	nsqC, err := setupNSQ(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Clean up the container after the test is complete
	defer nsqC.Terminate(ctx)

	mux := newNSQQueueMux(false)
	var wg sync.WaitGroup
	queue, err := mux.Queue(fmt.Sprintf("nsqd://%s:%s/testtopic/testchannel#ephemeral", nsqC.IP, nsqC.Port))
	if err != nil {
		t.Fatal(err)
	}
	expectedMsg := []byte(`{"test":1}`)
	wg.Add(1)
	handler := queue.AddHandler(func(ctx context.Context, msg []byte) error {
		if !reflect.DeepEqual(msg, expectedMsg) {
			t.Errorf("Test failed, expected %s got %s", expectedMsg, msg)
		}
		wg.Done()
		return nil
	})
	handler.Start()
	err = handler.Publish(expectedMsg)
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}
