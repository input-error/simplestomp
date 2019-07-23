package simplestomp

// https://github.com/go-stomp/stomp/blob/master/examples/client_test/main.go
import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/go-stomp/stomp"
)

// Client wraps the stomp connection and is contains the connection parameters.
type Client struct {
	Conn     *stomp.Conn
	Username string
	Password string
	Server   string
	Port     int
}

func (svc *Client) validateConfig() error {
	if svc.Username == "" {
		return errors.New("the Username variable cannot be empty")
	}

	if svc.Password == "" {
		return errors.New("the Password variable cannot be empty")
	}

	if svc.Server == "" {
		return errors.New("the Server variable cannot be empty")
	}

	if svc.Port <= 0 {
		return errors.New("the Port variable cannot be equal to or less than 0")
	}

	return nil

}

func (svc *Client) getConnection() (*stomp.Conn, error) {
	if svc.Conn != nil {
		return svc.Conn, nil
	}

	if err := svc.validateConfig(); err != nil {
		return nil, err
	}

	netConn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", svc.Server, svc.Port), 10*time.Second)
	if err != nil {
		return nil, err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	svc.Conn, err = stomp.Connect(
		netConn,
		stomp.ConnOpt.Login(svc.Username, svc.Password),
		stomp.ConnOpt.Host(svc.Server),
		stomp.ConnOpt.Header("client-id", hostname),
	)
	if err != nil {
		return nil, err
	}

	return svc.Conn, nil
}

// Close will gracefully Disconnect the stomp connection.
func (svc *Client) Close() {
	svc.Conn.Disconnect()
}

// GetMessage will retrieve a single message from a given queue.
func (svc *Client) GetMessage(queue string) (string, error) {
	stompConn, err := svc.getConnection()
	if err != nil {
		return "", err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}

	subQueue, err := stompConn.Subscribe(
		fmt.Sprintf("/queue/%s", queue),
		stomp.AckAuto,
		stomp.SubscribeOpt.Header("durable-subscription-name", hostname),
		stomp.SubscribeOpt.Header("subscription-type", "MULTICAST"))
	defer subQueue.Unsubscribe()

	if err != nil {
		return "", err
	}

	msg := <-subQueue.C
	if msg.Err != nil {
		return "", msg.Err
	}

	return string(msg.Body), nil
}

// ProcessMessages will continually receive messages and call the
// processFunc method passing the message to it each time until
// the context is Closed.
func (svc *Client) ProcessMessages(ctx context.Context, queue string, processFunc func(*stomp.Message) error) error {
	stompConn, err := svc.getConnection()
	if err != nil {
		return err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	subQueue, err := stompConn.Subscribe(
		fmt.Sprintf("/queue/%s", queue),
		stomp.AckAuto,
		stomp.SubscribeOpt.Header("durable-subscription-name", hostname),
		stomp.SubscribeOpt.Header("subscription-type", "MULTICAST"))

	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return subQueue.Unsubscribe()
		case msg, ok := <-subQueue.C:
			if !ok {
				return nil
			}

			if msg.Err != nil {
				return msg.Err
			}

			err := processFunc(msg)

			if err != nil {
				return err
			}
		default:
			// No message recv'd
			time.Sleep(1 * time.Second)
			continue
		}
	}
}

// SendMessage will send a single message to a given queue.
func (svc *Client) SendMessage(body string, queue string, contenttype string) error {
	stompConn, err := svc.getConnection()
	if err != nil {
		return err
	}

	err = stompConn.Send(
		fmt.Sprintf("/queue/%s", queue),
		contenttype,
		[]byte(body),
		stomp.SendOpt.Receipt)

	if err != nil {
		fmt.Printf("Error sending: %s\n", err)
	}

	return nil
}
