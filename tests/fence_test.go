package tests

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/tidwall/gjson"
)

func subTestFence(t *testing.T, mc *mockServer) {
	// Standard
	runStep(t, mc, "basic", fence_basic_test)
	runStep(t, mc, "channel message order", fence_channel_message_order_test)
	runStep(t, mc, "detect inside,outside", fence_detect_inside_test)

	// Roaming
	runStep(t, mc, "roaming live", fence_roaming_live_test)
	runStep(t, mc, "roaming channel", fence_roaming_channel_test)
	runStep(t, mc, "roaming webhook", fence_roaming_webhook_test)

	// channel meta
	runStep(t, mc, "channel meta", fence_channel_meta_test)
}

type fenceReader struct {
	conn net.Conn
	rd   *bufio.Reader
}

func (fr *fenceReader) receive() (string, error) {
	if err := fr.conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		return "", err
	}
	line, err := fr.rd.ReadBytes('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 4 || line[0] != '$' || line[len(line)-2] != '\r' || line[len(line)-1] != '\n' {
		return "", errors.New("invalid message")
	}
	n, err := strconv.ParseUint(string(line[1:len(line)-2]), 10, 64)
	if err != nil {
		return "", err
	}
	buf := make([]byte, int(n)+2)
	_, err = io.ReadFull(fr.rd, buf)
	if err != nil {
		return "", err
	}
	if buf[len(buf)-2] != '\r' || buf[len(buf)-1] != '\n' {
		return "", errors.New("invalid message")
	}
	js := buf[:len(buf)-2]
	var m interface{}
	if err := json.Unmarshal(js, &m); err != nil {
		return "", err
	}
	return string(js), nil
}

func (fr *fenceReader) receiveExpect(valex ...string) error {
	s, err := fr.receive()
	if err != nil {
		return err
	}
	for i := 0; i < len(valex); i += 2 {
		if gjson.Get(s, valex[i]).String() != valex[i+1] {
			return fmt.Errorf("expected '%s'='%s', got '%s'", valex[i], valex[i+1], gjson.Get(s, valex[i]).String())
		}
	}
	return nil
}

func fence_basic_test(mc *mockServer) error {
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = fmt.Fprintf(conn, "NEARBY mykey FENCE POINT 33 -115 5000\r\n")
	if err != nil {
		return err
	}

	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	res := string(buf[:n])
	if res != "+OK\r\n" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}
	rd := &fenceReader{conn, bufio.NewReader(conn)}

	// send a point
	c, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer c.Close()

	res, err = redis.String(c.Do("SET", "mykey", "myid1", "POINT", 33, -115))
	if err != nil {
		return err
	}
	if res != "OK" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}

	// receive the message
	if err := rd.receiveExpect("command", "set",
		"detect", "enter",
		"key", "mykey",
		"id", "myid1",
		"object.type", "Point",
		"object.coordinates", "[-115,33]"); err != nil {
		return err
	}

	if err := rd.receiveExpect("command", "set",
		"detect", "inside",
		"key", "mykey",
		"id", "myid1",
		"object.type", "Point",
		"object.coordinates", "[-115,33]"); err != nil {
		return err
	}

	res, err = redis.String(c.Do("SET", "mykey", "myid1", "POINT", 34, -115))
	if err != nil {
		return err
	}
	if res != "OK" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}

	// receive the message
	if err := rd.receiveExpect("command", "set",
		"detect", "exit",
		"key", "mykey",
		"id", "myid1",
		"object.type", "Point",
		"object.coordinates", "[-115,34]"); err != nil {
		return err
	}

	if err := rd.receiveExpect("command", "set",
		"detect", "outside",
		"key", "mykey",
		"id", "myid1",
		"object.type", "Point",
		"object.coordinates", "[-115,34]"); err != nil {
		return err
	}
	return nil
}

func fence_channel_message_order_test(mc *mockServer) error {
	// Create a channel to store the goroutines error
	finalErr := make(chan error)

	// Concurrently subscribe for notifications
	go func() {
		// Create the subscription connection to Tile38 to subscribe for updates
		sc, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
		if err != nil {
			log.Println(err)
			return
		}
		defer sc.Close()

		// Subscribe the subscription client to the * pattern
		psc := redis.PubSubConn{Conn: sc}
		if err := psc.PSubscribe("*"); err != nil {
			log.Println(err)
			return
		}

		var msgs []string

		// While not a permanent error on the connection.
	loop:
		for sc.Err() == nil {
			switch v := psc.Receive().(type) {
			case redis.Message:
				msgs = append(msgs, string(v.Data))
				if len(msgs) == 8 {
					break loop
				}
			case error:
				fmt.Print(err.Error())
			}
		}

		// Verify all messages
		correctOrder := []string{"exit:A", "exit:B", "outside:A", "outside:B", "enter:C", "enter:D", "inside:C", "inside:D"}
		for i := range msgs {
			if gjson.Get(msgs[i], "detect").String()+":"+
				gjson.Get(msgs[i], "hook").String() != correctOrder[i] {
				finalErr <- errors.New("INVALID MESSAGE ORDER")
			}
		}
		finalErr <- nil
	}()

	// Create the base connection for setting up points and geofences
	bc, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer bc.Close()

	// Fire all setup commands on the base client
	for _, cmd := range []string{
		"SET points point POINT 33.412529053733444 -111.93368911743164",
		fmt.Sprintf(`SETCHAN A WITHIN points FENCE OBJECT {"type":"Polygon","coordinates":[[[-111.95205688476562,33.400491820565236],[-111.92630767822266,33.400491820565236],[-111.92630767822266,33.422272258866045],[-111.95205688476562,33.422272258866045],[-111.95205688476562,33.400491820565236]]]}`),
		fmt.Sprintf(`SETCHAN B WITHIN points FENCE OBJECT {"type":"Polygon","coordinates":[[[-111.93952560424803,33.403501285221594],[-111.92630767822266,33.403501285221594],[-111.92630767822266,33.41997983836345],[-111.93952560424803,33.41997983836345],[-111.93952560424803,33.403501285221594]]]}`),
		fmt.Sprintf(`SETCHAN C WITHIN points FENCE OBJECT {"type":"Polygon","coordinates":[[[-111.9255781173706,33.40342963251261],[-111.91201686859131,33.40342963251261],[-111.91201686859131,33.41994401881284],[-111.9255781173706,33.41994401881284],[-111.9255781173706,33.40342963251261]]]}`),
		fmt.Sprintf(`SETCHAN D WITHIN points FENCE OBJECT {"type":"Polygon","coordinates":[[[-111.92562103271484,33.40063513076968],[-111.90021514892578,33.40063513076968],[-111.90021514892578,33.42212898435788],[-111.92562103271484,33.42212898435788],[-111.92562103271484,33.40063513076968]]]}`),
		"SET points point POINT 33.412529053733444 -111.91909790039062",
	} {
		if _, err := do(bc, cmd); err != nil {
			return err
		}
	}
	return <-finalErr
}

func fence_detect_inside_test(mc *mockServer) error {
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = fmt.Fprintf(conn, "WITHIN users FENCE DETECT inside,outside POINTS BOUNDS 33.618824 -84.457973 33.654359 -84.399859\r\n")
	if err != nil {
		return err
	}

	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	res := string(buf[:n])
	if res != "+OK\r\n" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}
	rd := &fenceReader{conn, bufio.NewReader(conn)}

	// send a point
	c, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer c.Close()

	res, err = redis.String(c.Do("SET", "users", "200", "POINT", "33.642301", "-84.43118"))
	if err != nil {
		return err
	}
	if res != "OK" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}

	if err := rd.receiveExpect("command", "set",
		"detect", "inside",
		"key", "users",
		"id", "200",
		"point", `{"lat":33.642301,"lon":-84.43118}`); err != nil {
		return err
	}

	res, err = redis.String(c.Do("SET", "users", "200", "POINT", "34.642301", "-84.43118"))
	if err != nil {
		return err
	}
	if res != "OK" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}

	// receive the message
	if err := rd.receiveExpect("command", "set",
		"detect", "outside",
		"key", "users",
		"id", "200",
		"point", `{"lat":34.642301,"lon":-84.43118}`); err != nil {
		return err
	}
	return nil
}

// do performs the passed command on the passed redis client
func do(c redis.Conn, cmd string) (interface{}, error) {
	// Split out all parameters
	params := strings.Split(cmd, " ")

	// Produce a slice of interfaces for use in the arguments
	var args []interface{}
	for _, p := range params[1:] {
		args = append(args, p)
	}

	// Perform the request and return the response
	return c.Do(params[0], args...)
}

func fence_channel_meta_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SETCHAN", "carbon", "NEARBY", "x", "MATCH", "carbon*", "FENCE", "NODWELL", "points", "ROAM", "x", "*", "200000"}, {"1"},
		{"OUTPUT", "json"}, {`{"ok":true}`},
		// check for valid json on the chans command
		{"CHANS", "*"}, {
			func(v interface{}) (resp, expect interface{}) {
				// v is the value as strings or slices of strings
				// test will pass as long as `resp` and `expect` are the same.
				if !json.Valid([]byte(v.(string))) {
					return v, "Valid JSON"
				}
				return true, true
			},
		},
	})
}
