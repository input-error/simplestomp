package simplestomp

// https://github.com/go-stomp/stomp/blob/master/examples/client_test/main.go
import (
	"context"
	"fmt"
	"github.com/go-stomp/stomp"
	"net"
	"os"
	"time"
)

type Client struct {
	conn     *stomp.Conn
	Username string
	Password string
	Server   string
	Port     int
}

func (svc *Client) getConnection() (*stomp.Conn, error) {
	if svc.conn != nil {
		return svc.conn, nil
	}

	netConn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", svc.Server, svc.Port), 10*time.Second)
	if err != nil {
		return nil, err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	svc.conn, err = stomp.Connect(netConn, stomp.ConnOpt.Login(svc.Username, svc.Password), stomp.ConnOpt.Host(svc.Server), stomp.ConnOpt.Header("client-id", hostname))
	if err != nil {
		return nil, err
	}

	return svc.conn, nil
}

func (svc *Client) Close() {
	svc.conn.Disconnect()
}

func (svc *Client) GetMessage(queue string) (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}

	subQueue, err := svc.conn.Subscribe(
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

func (svc *Client) ProcessMessages(queue string, ctx context.Context, processFunc func(*stomp.Message) error) error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	subQueue, err := svc.conn.Subscribe(
		fmt.Sprintf("/queue/%s", queue),
		stomp.AckAuto,
		stomp.SubscribeOpt.Header("durable-subscription-name", hostname),
		stomp.SubscribeOpt.Header("subscription-type", "MULTICAST"))
	defer subQueue.Unsubscribe()

	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
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
		}
	}

	return nil
}

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
