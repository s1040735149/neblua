package main

import (
	"log"
	"nebluaGraph/nebulaGraph"
)

var NebulaInstance *nebulaGraph.NebulaObj

func main() {
	NebulaInstance, err := nebulaGraph.NewGraphInstance(&nebulaGraph.NebulaConf{
		Address:  "10.111.136.13",
		Port:     9669,
		UserName: "user",
		Password: "password",
	})
	if err != nil {
		log.Fatal(err)
	}
}
