package main

import (
	"os"

	"github.com/fmarmol/permos"
	bolt "go.etcd.io/bbolt"
)

const (
	DefaultNameBBolt = "local"
)

const (
	ConfigPath    = "$HOME/.config"
	BboltciPath   = ConfigPath + "/bbolt-ci"
	DefaultDBPath = BboltciPath + "/local.bbolt"
)

func InstallBBolt() error {
	_, err := os.Stat(os.ExpandEnv(ConfigPath))
	if err != nil {
		return err
	}
	bboltPath := os.ExpandEnv(BboltciPath)
	_, err = os.Stat(bboltPath)
	if os.IsNotExist(err) {
		err2 := os.Mkdir(bboltPath, 0700)
		if err2 != nil {
			return err2
		}
	}
	dbPath := os.ExpandEnv(DefaultDBPath)
	perm := permos.Perm{UserRead: true, UserWrite: true, UserExec: true}
	db, err := bolt.Open(dbPath, perm.FileMode(), nil)
	if err != nil {
		return err
	}
	defer db.Close()
	return nil
}
