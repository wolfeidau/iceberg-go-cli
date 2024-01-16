package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/alecthomas/kong"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	version = "dev"

	cli struct {
		Debug    bool `help:"Enable debug mode."`
		Version  kong.VersionFlag
		Database string `help:"Database name."`
		Table    string `help:"Table name."`
	}
)

func main() {
	kong.Parse(&cli,
		kong.Vars{"version": version}, // bind a var for version
	)

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).With().Stack().Logger()

	awscfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	gluesvc := glue.NewFromConfig(awscfg)

	tblRes, err := gluesvc.GetTable(
		context.Background(),
		&glue.GetTableInput{DatabaseName: aws.String(cli.Database), Name: aws.String(cli.Table)},
	)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to get table")
	}

	data, err := json.Marshal(tblRes.Table.Parameters)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to marshal table parameters")
	}

	fmt.Println(string(data))

	iofs, err := io.LoadFS(map[string]string{}, tblRes.Table.Parameters["metadata_location"])
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load file system")
	}

	log.Info().Str("metadata_location", tblRes.Table.Parameters["metadata_location"]).Msg("loaded file system")

	table, err := table.NewFromLocation([]string{cli.Table}, tblRes.Table.Parameters["metadata_location"], iofs)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create table")
	}

	fmt.Println(table.MetadataLocation())

	fmt.Println(table.Schema())

	fmt.Println(table.Spec())

	fmt.Println(table.SortOrder())

	manifests, err := table.CurrentSnapshot().Manifests(iofs)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create table")
	}

	for _, m := range manifests {
		data, err := json.Marshal(m)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to create table")
		}

		fmt.Println(string(data))
	}
}
