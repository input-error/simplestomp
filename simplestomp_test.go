package simplestomp

import (
	"github.com/go-stomp/stomp"
	"testing"
)

// Starts up a rkt? or docker? container prior to running tests
func setupDockerContainer() {
}

func TestSendMessage(t *testing.T) {
}

func TestGetMessage(t *testing.T) {
}

func TestProcessMessage(t *testing.T) {
}

func TestAll(t *testing.T) {
	svc := &simplestomp{
		Username: "artemis",
		Password: "artemis",
		Server:   "localhost",
		Port:     61616,
	}
	defer svc.Close()

	err := svc.SendMessage("This is a test!", "testqueue", "text/plain")
	if err != nil {
		t.Errorf("Crap we failed to send!. Error: %s\n", err)
		t.Fail()
	}
	t.Logf("Sent first message")

	msg, err := svc.GetMessage("testqueue")
	if err != nil {
		t.Errorf("Failed to get messages!. Error: %s\n", err)
		t.Fail()
	}

	t.Logf("Message: %s\n", msg)

	err = svc.SendMessage("This is a test 2!", "testqueue", "text/plain")
	if err != nil {
		t.Errorf("Crap we failed to send!. Error: %s\n", err)
		t.Fail()
	}

	err = svc.ProcessMessages(
		"testqueue",
		func(message *stomp.Message) error {
			t.Logf("ProcMessages: Msg: %s\n", string(message.Body))

			return message.Err
		})
}
