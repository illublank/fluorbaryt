package main

import (
	"github.com/illublank/fluorbaryt/client/mysql"
)

func main() {
	// cfg := env.LoadAllWithoutPrefix("FB_")

	config := &mysql.Config{
		Host:     "106.75.181.203",
		Port:     30306,
		User:     "root",
		Pass:     "654321",
		ServerId: 1111,
		LogFile:  "mysql-bin.000001",
		Position: 4,
	}

	srv := mysql.Server{
		Cfg: config,
	}

	srv.Run()
}
