package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/tidwall/redcon"
)

var port int
var pgurl string

func main() {
	flag.IntVar(&port, "port", 6380, "tcp port")
	flag.Parse()
	var pgport = 30000 + (port % 30000)
	var buf [32]byte
	rand.Read(buf[:])
	var pgpass = string(hex.EncodeToString(buf[:]))
	pgurl = fmt.Sprintf("postgres://postgres:%s@127.0.0.1:%d", pgpass, pgport)

	var pgname = "pgred"

	exec.Command("docker", "stop", pgname).Run()
	exec.Command("docker", "rm", pgname).Run()
	defer func() {
		exec.Command("docker", "stop", pgname).Run()
		exec.Command("docker", "rm", pgname).Run()
	}()

	tag := "postgres:17-alpine"
	fmt.Printf("Starting up %s (port %d)...\n", tag, pgport)

	cmd := exec.Command("docker", "run",
		"--name", pgname,
		"--rm",
		"-e", "POSTGRES_PASSWORD="+pgpass,
		"-p", fmt.Sprintf("%d:5432", pgport),
		tag)
	cmd.Stdout = os.Stdout
	errrd, err := cmd.StderrPipe()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	go func() {
		listening := false
		rd := bufio.NewReader(errrd)
		for {
			bline, err := rd.ReadBytes('\n')
			if err != nil {
				break
			}
			line := string(bline)
			fmt.Printf("%s", line)
			if strings.Contains(line, "listening") &&
				strings.Contains(line, "port 5432") {
				listening = true
			}
			if strings.Contains(line, "database system is ready to accept "+
				"connections") && listening {
				go func() {
					initDatabase()
					startRedconServer()
				}()
			}
		}
	}()
	if err := cmd.Run(); err != nil {
		os.Exit(1)
	}
}

func initDatabase() {
	pconn, err := pgx.Connect(context.Background(), pgurl)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	defer pconn.Close(context.Background())
	_, err = pconn.Exec(context.Background(),
		"create table fields (key text primary key, value text)")
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func getkey(pconn *pgx.Conn, key string) (*string, error) {
	var value string
	err := pconn.QueryRow(context.Background(),
		"select value from fields where key = $1", key).Scan(&value)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return nil, nil
		}
		return nil, err
	}
	return &value, nil
}

func setkey(pconn *pgx.Conn, key, value string) error {
	_, err := pconn.Exec(context.Background(),
		"insert into fields (key, value) values ($1, $2) "+
			"on conflict (key) do "+
			"update set value = $2",
		key, value)
	return err
}

func delkey(pconn *pgx.Conn, key string) (bool, error) {
	value, err := getkey(pconn, key)
	if err != nil || value == nil {
		return false, err
	}
	_, err = pconn.Exec(context.Background(),
		"delete from fields where key = $1", key)
	return err == nil, err
}

func startRedconServer() {
	fmt.Printf("Starting redcon server (port %d)\n", port)
	fmt.Fprintf(os.Stderr, "%s\n",
		redcon.ListenAndServe(fmt.Sprintf(":%d", port),
			func(conn redcon.Conn, cmd redcon.Command) {
				pconn := conn.Context().(*pgx.Conn)
				switch strings.ToLower(string(cmd.Args[0])) {
				case "ping":
					conn.WriteString("PONG")
				case "echo":
					if len(cmd.Args) < 2 {
						conn.WriteError("Wrong number of arguments")
						return
					}
					conn.WriteBulkString(string(cmd.Args[1]))
				case "flushdb", "flushall":
					_, err := pconn.Exec(context.Background(),
						"delete from fields")
					if err != nil {
						conn.WriteError(err.Error())
					} else {
						conn.WriteString("OK")
					}
				case "begin":
					tags, err := pconn.Exec(context.Background(),
						"BEGIN ISOLATION LEVEL SERIALIZABLE")
					if err != nil {
						conn.WriteError(err.Error())
					} else {
						conn.WriteString(fmt.Sprint(tags))
					}
				case "del":
					if len(cmd.Args) < 2 {
						conn.WriteError("Wrong number of arguments")
						return
					}
					var count int
					for i := 1; i < len(cmd.Args); i++ {
						deleted, err := delkey(pconn, string(cmd.Args[i]))
						if err != nil {
							conn.WriteError(err.Error())
							return
						}
						if deleted {
							count++
						}
					}
					conn.WriteInt(count)
				case "commit", "rollback", "end", "abort":
					tags, err := pconn.Exec(context.Background(),
						string(cmd.Args[0]))
					if err != nil {
						conn.WriteError(err.Error())
					} else {
						conn.WriteString(fmt.Sprint(tags))
					}
				case "set":
					if len(cmd.Args) < 3 {
						conn.WriteError("Wrong number of arguments")
						return
					}
					err := setkey(pconn, string(cmd.Args[1]),
						string(cmd.Args[2]))
					if err != nil {
						conn.WriteError(err.Error())
					} else {
						conn.WriteString("OK")
					}
				case "get":
					if len(cmd.Args) < 2 {
						conn.WriteError("Wrong number of arguments")
						return
					}
					value, err := getkey(pconn, string(cmd.Args[1]))
					if err != nil {
						conn.WriteError(err.Error())
					} else if value == nil {
						conn.WriteNull()
					} else {
						conn.WriteBulkString(*value)
					}
				default:
					conn.WriteError("Unknown command")
				}
			},
			func(conn redcon.Conn) bool {
				pconn, err := pgx.Connect(context.Background(), pgurl)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%v\n", err)
					return false
				}
				conn.SetContext(pconn)
				fmt.Printf("Client connected %s\n", conn.RemoteAddr())
				return true
			},
			func(conn redcon.Conn, err error) {
				conn.Context().(*pgx.Conn).Close(context.Background())
			},
		),
	)
	os.Exit(1)
}
