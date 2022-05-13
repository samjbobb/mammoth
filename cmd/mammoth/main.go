package main

import (
	"os"

	"github.com/samjbobb/mammoth/config"
	"github.com/samjbobb/mammoth/supervisor"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "mammoth",
		Usage: "sync postgres with a target",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Value:   "config.yml",
				Aliases: []string{"c"},
				Usage:   "path to config file",
			},
		},
		Action: func(ctx *cli.Context) error {
			s := supervisor.NewSupervisor()
			return s.Run(ctx.Context, ctx.String("config"))
		},
		Commands: []*cli.Command{
			{
				Name: "initconfig",
				Action: func(ctx *cli.Context) error {
					return config.WriteExampleConfig(ctx.String("config"))
				},
			},
			{
				Name: "streamtofile",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "out",
						Value:   "out.jsonl",
						Aliases: []string{"o"},
						Usage:   "file to write",
					},
				},
				Action: func(ctx *cli.Context) error {
					return supervisor.StreamToFile(ctx.Context, ctx.String("config"), ctx.String("out"))
				},
			},
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		logrus.Fatal(err)
	}
}
