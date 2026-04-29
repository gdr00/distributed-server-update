package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/gdr00/distributed-server-update/internal/controller"
	"github.com/gdr00/distributed-server-update/internal/types"
)

func main() {

	initCmd := flag.NewFlagSet("init", flag.ExitOnError)
	initConfig := initCmd.String("config", "", "path to config file")
	initEmpty := initCmd.Bool("empty", false, "init empty replica")

	startConfig := flag.String("config", "", "path to config file")

	if len(os.Args) > 1 && os.Args[1] == "init" {
		initCmd.Parse(os.Args[2:])
		cfg, err := types.LoadConfig(*initConfig)
		if err != nil {
			log.Fatal(err)
		}
		if *initEmpty {
			// just create node_id and empty crdt_state.json
			if err := controller.InitEmptyNode(cfg.CRDTWorkdir); err != nil {
				log.Fatal(err)
			}
		} else {
			if err := controller.InitNode(cfg); err != nil {
				log.Fatal(err)
			}
		}
		return
	}

	flag.Parse()
	if *startConfig == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s --config=<path>\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}
	cfg, err := types.LoadConfig(*startConfig)
	if err != nil {
		log.Fatal(err)
	}

	ctrl := controller.New(cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := ctrl.Run(ctx); err != nil {
		log.Fatal(err)
	}

}
