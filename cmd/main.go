package cmd

import (
	"context"
	"flag"
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

	flag.Parse()
	cfg, err := types.LoadConfig(*initConfig)
	if err != nil {
		log.Fatal(err)
	}

	if len(os.Args) > 1 && os.Args[1] == "init" {
		initCmd.Parse(os.Args[2:])
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

	ctrl := controller.New(cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := ctrl.Run(ctx); err != nil {
		log.Fatal(err)
	}

}
