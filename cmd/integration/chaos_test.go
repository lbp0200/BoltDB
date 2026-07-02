package integration

import (
	"bufio"
	"context"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
)

func TestChaos_PubSub(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	numClients := 50
	numOps := 30
	channels := []string{"chaos:a", "chaos:b", "chaos:c", "chaos:d", "chaos:e"}

	var wg sync.WaitGroup

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("client %d panicked: %v", id, r)
				}
			}()

			conn, err := net.Dial("tcp", sharedListener.Addr().String())
			if err != nil {
				return
			}
			defer conn.Close()
			reader := bufio.NewReader(conn)

			subscribed := make(map[string]bool)

			for j := 0; j < numOps; j++ {
				op := rand.Intn(8)
				switch op {
				case 0:
					ch := channels[rand.Intn(len(channels))]
					sendPubSubCmd(t, conn, "SUBSCRIBE", ch)
					readPubSubResp(t, reader)
					subscribed[ch] = true
				case 1:
					ch := channels[rand.Intn(len(channels))]
					sendPubSubCmd(t, conn, "UNSUBSCRIBE", ch)
					readPubSubResp(t, reader)
					delete(subscribed, ch)
				case 2:
					ch := channels[rand.Intn(len(channels))]
					sharedClient.Publish(ctx, ch, "chaos_msg")
				case 3:
					sendPubSubCmd(t, conn, "PING")
					_, err := reader.ReadString('\n')
					if err != nil {
						return
					}
				case 4:
					if len(subscribed) > 0 {
						for ch := range subscribed {
							sendPubSubCmd(t, conn, "UNSUBSCRIBE", ch)
							readPubSubResp(t, reader)
							break
						}
					}
					delete(subscribed, "")
				case 5:
					conn.Close()
					return
				case 6:
					sendPubSubCmd(t, conn, "QUIT")
					_, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					return
				case 7:
					sendPubSubCmd(t, conn, "GET", "somekey")
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					if line[0] != '-' {
						t.Logf("chaos: GET in pubsub mode returned unexpected: %s", line)
					}
				}
			}
		}(i)
	}

	wg.Wait()

	sharedClient.Ping(ctx)

	setupTest(t)
	t.Logf("PubSub chaos completed: %d clients, %d ops each", numClients, numOps)
}

func TestChaos_Transaction(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	numClients := 30
	numOps := 20

	var wg sync.WaitGroup

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("tx client %d panicked: %v", id, r)
				}
			}()

			conn, err := net.Dial("tcp", sharedListener.Addr().String())
			if err != nil {
				return
			}
			defer conn.Close()
			reader := bufio.NewReader(conn)

			inTx := false

			for j := 0; j < numOps; j++ {
				op := rand.Intn(7)
				switch op {
				case 0:
					sendRESP(conn, "MULTI")
					resp, err := proto.ReadRESP(reader)
					if err != nil {
						return
					}
					if string(resp.Args[0]) == "OK" {
						inTx = true
					}
				case 1:
					if inTx {
						sendRESP(conn, "SET", "chaos:txkey", "val")
						proto.ReadRESP(reader)
						sendRESP(conn, "EXEC")
						rawLine, err := reader.ReadString('\n')
						if err != nil {
							return
						}
						if rawLine[0] == '*' {
							inTx = false
						}
					}
				case 2:
					if inTx {
						sendRESP(conn, "DISCARD")
						resp, err := proto.ReadRESP(reader)
						if err != nil {
							return
						}
						if string(resp.Args[0]) == "OK" {
							inTx = false
						}
					}
				case 3:
					sendRESP(conn, "WATCH", "chaos:watch")
					proto.ReadRESP(reader)
				case 4:
					sendRESP(conn, "UNWATCH")
					proto.ReadRESP(reader)
				case 5:
					sharedClient.Set(ctx, "chaos:watch", "modified", 0)
				case 6:
					conn.Close()
					return
				}
			}

			if inTx {
				sendRESP(conn, "DISCARD")
				proto.ReadRESP(reader)
			}
		}(i)
	}

	wg.Wait()

	sharedClient.Ping(ctx)

	setupTest(t)
	t.Logf("Transaction chaos completed: %d clients, %d ops each", numClients, numOps)
}

func TestChaos_Blocking(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	numBlockers := 20
	numPushers := 10
	numOps := 15

	var wg sync.WaitGroup

	blockersStarted := make(chan struct{}, numBlockers)

	for i := 0; i < numBlockers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("blocker %d panicked: %v", id, r)
				}
			}()

			conn, err := net.Dial("tcp", sharedListener.Addr().String())
			if err != nil {
				return
			}
			defer conn.Close()

			blockersStarted <- struct{}{}

			for j := 0; j < numOps; j++ {
				timeout := rand.Intn(3) + 1 // 1-3s, never infinite
				cmd := []string{"BLPOP", "BRPOP"}[rand.Intn(2)]
				proto.WriteRESP(conn, &proto.Array{
					Args: [][]byte{
						[]byte(cmd),
						[]byte("chaos:blist"),
						[]byte(strconv.Itoa(timeout)),
					},
				})

				reader := bufio.NewReader(conn)
				resp, err := proto.ReadRESP(reader)
				if err != nil {
					return
				}
				if resp != nil && len(resp.Args) == 2 {
					t.Logf("blocker %d got data from %s", id, cmd)
				}
			}
		}(i)
	}

	time.Sleep(200 * time.Millisecond)

	for i := 0; i < numPushers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("pusher %d panicked: %v", id, r)
				}
			}()

			for j := 0; j < numOps*2; j++ {
				sharedClient.LPush(ctx, "chaos:blist", "val")
				time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	sharedClient.Ping(ctx)

	setupTest(t)
	t.Logf("Blocking chaos completed: %d blockers, %d pushers, %d ops each", numBlockers, numPushers, numOps)
}

func TestChaos_BlockingWithClientKill(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	numBlockers := 10

	var wg sync.WaitGroup

	var addrsMu sync.Mutex
	blockerAddrs := make([]string, 0, numBlockers)
	blockerConns := make([]net.Conn, 0, numBlockers)

	for i := 0; i < numBlockers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("blocker %d panicked: %v", id, r)
				}
			}()

			conn, err := net.Dial("tcp", sharedListener.Addr().String())
			if err != nil {
				return
			}

			addrsMu.Lock()
			blockerAddrs = append(blockerAddrs, conn.RemoteAddr().String())
			blockerConns = append(blockerConns, conn)
			addrsMu.Unlock()

			proto.WriteRESP(conn, &proto.Array{
				Args: [][]byte{[]byte("BLPOP"), []byte("chaos:kill"), []byte("0")},
			})

			reader := bufio.NewReader(conn)
			proto.ReadRESP(reader)
		}(i)
	}

	time.Sleep(200 * time.Millisecond)

	addrsMu.Lock()
	for _, addr := range blockerAddrs {
		sharedClient.Do(ctx, "CLIENT", "KILL", "ADDR", addr)
	}
	addrsMu.Unlock()

	time.Sleep(500 * time.Millisecond)
	for _, conn := range blockerConns {
		conn.Close()
	}

	sharedClient.Ping(ctx)

	setupTest(t)
	t.Logf("Blocking+CLIENT KILL chaos completed: %d blockers killed", numBlockers)
}

func TestChaos_MixedAll(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	numClients := 30
	numOps := 25

	var wg sync.WaitGroup

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("mixed client %d panicked: %v", id, r)
				}
			}()

			conn, err := net.Dial("tcp", sharedListener.Addr().String())
			if err != nil {
				return
			}
			defer conn.Close()
			reader := bufio.NewReader(conn)

			for j := 0; j < numOps; j++ {
				op := rand.Intn(10)
				switch op {
				case 0:
					sendPubSubCmd(t, conn, "SUBSCRIBE", "chaos:mix")
					readPubSubResp(t, reader)
				case 1:
					sendPubSubCmd(t, conn, "UNSUBSCRIBE", "chaos:mix")
					readPubSubResp(t, reader)
				case 2:
					sharedClient.Publish(ctx, "chaos:mix", "data")
				case 3:
					sendRESP(conn, "MULTI")
					proto.ReadRESP(reader)
				case 4:
					sendRESP(conn, "EXEC")
					reader.ReadString('\n')
				case 5:
					sendRESP(conn, "DISCARD")
					proto.ReadRESP(reader)
				case 6:
					sendRESP(conn, "WATCH", "chaos:mix")
					proto.ReadRESP(reader)
				case 7:
					sendRESP(conn, "SET", "chaos:mix", "value")
					proto.ReadRESP(reader)
				case 8:
					sendRESP(conn, "GET", "chaos:mix")
					proto.ReadRESP(reader)
				case 9:
					sendRESP(conn, "PING")
					reader.ReadString('\n')
				}
			}
		}(i)
	}

	wg.Wait()

	sharedClient.Ping(ctx)

	setupTest(t)
	t.Logf("Mixed chaos completed: %d clients, %d ops each", numClients, numOps)
}

func sendRESP(conn net.Conn, cmd string, args ...string) {
	cmdArgs := make([][]byte, 1+len(args))
	cmdArgs[0] = []byte(cmd)
	for i, arg := range args {
		cmdArgs[i+1] = []byte(arg)
	}
	proto.WriteRESP(conn, &proto.Array{Args: cmdArgs})
}
