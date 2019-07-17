package simplestomp

// https://github.com/go-stomp/stomp/blob/master/examples/client_test/main.go
import (
	"fmt"
	"github.com/go-stomp/stomp"
	"net"
	"os"
	"time"
)

type simplestomp struct {
	conn     *stomp.Conn
	Username string
	Password string
	Server   string
	Port     int
}

func (svc *simplestomp) getConnection() (*stomp.Conn, error) {
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

func (svc *simplestomp) Close() {
	svc.conn.Disconnect()
}

func (svc *simplestomp) GetMessage(queue string) (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}

	subQueue, err := svc.conn.Subscribe(
		fmt.Sprintf("/queue/%s", queue),
		stomp.AckAuto,
		stomp.SubscribeOpt.Header("durable-subscription-name", hostname),
		stomp.SubscribeOpt.Header("subscription-type", "MULTICAST"))

	if err != nil {
		return "", err
	}

	msg := <-subQueue.C
	if msg.Err != nil {
		return "", msg.Err
	}

	err = subQueue.Unsubscribe()
	if err != nil {
		return "", err
	}

	return string(msg.Body), nil
}

func (svc *simplestomp) ProcessMessages(queue string, processFunc func(*stomp.Message) error) error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	subQueue, err := svc.conn.Subscribe(
		fmt.Sprintf("/queue/%s", queue),
		stomp.AckAuto,
		stomp.SubscribeOpt.Header("durable-subscription-name", hostname),
		stomp.SubscribeOpt.Header("subscription-type", "MULTICAST"))

	if err != nil {
		return err
	}

	for {
		msg, ok := <-subQueue.C
		if !ok {
			// This never is called. Should I use a signal? When signal is SET then stop this loop? Is that even possible since it looks to be blocking on the above?
			//https://github.com/go-stomp/stomp/blob/master/example_test.go
			//Not much help here ...
			break
		}

		err := processFunc(msg)

		if err != nil {
			return err
		}
	}

	return nil
}

func (svc *simplestomp) SendMessage(body string, queue string, contenttype string) error {
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

func processMessage(ch chan *stomp.Message) {
	fmt.Printf("Recv'd message: %s\n", string((<-ch).Body))
}
