package cmd

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/gdr00/distributed-server-update/internal/controller"
	"github.com/gdr00/distributed-server-update/internal/types"
)

func main() {

	initCmd := flag.NewFlagSet("init", flag.ExitOnError)
	initSettings := initCmd.String("settings", "", "path to settings file")
	initWorkDir := initCmd.String("crtdWorkdir", defaultWorkDir(), "path to crdt work directory")

	startWorkDir := flag.String("crdtWorkDir", defaultWorkDir(), "path to crdt work directory")

	if len(os.Args) > 1 && os.Args[1] == "init" {
		initCmd.Parse(os.Args[2:])
		if err := controller.InitNode(*initSettings, *initWorkDir); err != nil {
			log.Fatal(err)
		}
		return
	}

	flag.Parse()
	cfg, err := types.LoadConfig(*startWorkDir)
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

func defaultWorkDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("failed to get home dir: %v", err)
	}
	return filepath.Join(home, ".crtd-conf")
}
