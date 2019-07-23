package simplestomp

import (
	"context"
	"testing"
	"time"

	"github.com/go-stomp/stomp"
)

func getConnection() *Client {
	svc := Client{
		Username: "artemis",
		Password: "artemis",
		Server:   "localhost",
		Port:     61616,
	}

	return &svc
}

func TestValidateConf_Valid(t *testing.T) {
	svc := Client{
		Username: "artemis",
		Password: "artemis",
		Server:   "localhost",
		Port:     61616,
	}

	if svc.validateConfig() != nil {
		t.Fatal("A valid configuration should not return an error")
	}
}

func TestValidateConf_Username(t *testing.T) {
	svc := Client{
		//Username: "artemis",
		Password: "artemis",
		Server:   "localhost",
		Port:     61616,
	}

	if err := svc.validateConfig(); err == nil {
		t.Fatal("an empty username should return an error")
	}
}

func TestValidateConf_Password(t *testing.T) {
	svc := Client{
		Username: "artemis",
		//Password: "artemis",
		Server: "localhost",
		Port:   61616,
	}

	if err := svc.validateConfig(); err == nil {
		t.Fatal("an empty password should return an error")
	}
}

func TestValidateConf_Server(t *testing.T) {
	svc := Client{
		Username: "artemis",
		Password: "artemis",
		//Server: "localhost",
		Port: 61616,
	}

	if err := svc.validateConfig(); err == nil {
		t.Fatal("an empty server should return an error")
	}
}

func TestValidateConf_Port(t *testing.T) {
	svc := Client{
		Username: "artemis",
		Password: "artemis",
		Server:   "localhost",
		//Port: 61616,
	}

	if err := svc.validateConfig(); err == nil {
		t.Fatal("an empty or unset port should return an error")
	}
}

func TestSendMessage(t *testing.T) {
	svc := getConnection()
	defer svc.Close()

	err := svc.SendMessage("This is a test!", "testqueue", "text/plain")
	if err != nil {
		t.Errorf("Crap we failed to send!. Error: %s\n", err)
		t.FailNow()
	}
	t.Logf("Sent first message\n")
}

func TestGetMessage(t *testing.T) {
	svc := getConnection()
	defer svc.Close()

	err := svc.SendMessage("This is a test!", "testqueue", "text/plain")
	if err != nil {
		t.Errorf("Crap we failed to send!. Error: %s\n", err)
		t.FailNow()
	}
	t.Logf("Sent first message\n")

	msg, err := svc.GetMessage("testqueue")
	if err != nil {
		t.Errorf("Failed to get messages!. Error: %s\n", err)
		t.FailNow()
	}

	t.Logf("Message: %s\n", msg)
}

func TestProcessMessage(t *testing.T) {
	svc := getConnection()

	err := svc.SendMessage("This is a test 2!", "testqueue", "text/plain")
	if err != nil {
		t.Errorf("Crap we failed to send!. Error: %s\n", err)
	}

	ctx := context.Background()
	go svc.ProcessMessages(
		ctx,
		"testqueue",
		func(message *stomp.Message) error {
			t.Logf("ProcMessages: Msg: %s\n", string(message.Body))

			return message.Err
		})

	time.Sleep(3 * time.Second)
	ctx.Done()
	svc.Close()
}
