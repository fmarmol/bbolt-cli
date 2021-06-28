package main

import (
	"fmt"
	"os"

	"github.com/fmarmol/permos"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	bolt "go.etcd.io/bbolt"
)

func newBucketCmd() *cobra.Command {
	bucketCmd := &cobra.Command{
		Use:   "bucket",
		Short: "bucket subcommand",
	}
	return bucketCmd
}

func newListBucketsCmd() *cobra.Command {
	listBucketCmd := &cobra.Command{
		Use:   "list",
		Short: "list buckets",
		RunE: func(cmd *cobra.Command, args []string) error {

			perm := permos.Perm{UserRead: true}
			dbPath := os.ExpandEnv(DefaultDBPath)
			db, err := bolt.Open(dbPath, perm.FileMode(), nil)
			if err != nil {
				return err
			}
			defer db.Close()

			return db.Update(func(tx *bolt.Tx) error {
				return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
					fmt.Println(string(name))
					return nil
				})
			})
		},
	}
	return listBucketCmd
}

func newListKeysBucketCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "list-keys",
		Short: "list key value pairs.",
		RunE: func(cmd *cobra.Command, args []string) error {

			perm := permos.Perm{UserRead: true}
			dbPath := os.ExpandEnv(DefaultDBPath)
			db, err := bolt.Open(dbPath, perm.FileMode(), nil)
			if err != nil {
				return err
			}
			defer db.Close()

			bucket, err := cmd.Flags().GetString("bucket")
			if err != nil {
				return err
			}

			return db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(bucket))
				if b == nil {
					b, err = tx.CreateBucket([]byte(bucket))
					if err != nil {
						return err
					}
				}
				return b.ForEach(func(k, v []byte) error {
					fmt.Println(string(k))
					return nil
				})
			})
		},
	}
	cmd.Flags().StringP("bucket", "b", "default", "bucket")
	return cmd

}

func newDeleteCmd() *cobra.Command {
	var deleteCmd = &cobra.Command{
		Use:   "delete",
		Short: "delete key value pair.",
		RunE: func(cmd *cobra.Command, args []string) error {

			perm := permos.Perm{UserRead: true}
			dbPath := os.ExpandEnv(DefaultDBPath)
			db, err := bolt.Open(dbPath, perm.FileMode(), nil)
			if err != nil {
				return err
			}
			defer db.Close()

			bucket, err := cmd.Flags().GetString("bucket")
			if err != nil {
				return err
			}
			key, err := cmd.Flags().GetString("key")
			if err != nil {
				return err
			}

			return db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(bucket))
				if b == nil {
					b, err = tx.CreateBucket([]byte(bucket))
					if err != nil {
						return err
					}
				}
				return b.Delete([]byte(key))
			})
		},
	}
	deleteCmd.Flags().StringP("key", "k", "", "key")
	deleteCmd.Flags().StringP("bucket", "b", "default", "bucket")
	return deleteCmd

}

func newGetCmd() *cobra.Command {
	var getCmd = &cobra.Command{
		Use:   "get",
		Short: "get key value pair.",
		RunE: func(cmd *cobra.Command, args []string) error {

			perm := permos.Perm{UserRead: true}
			dbPath := os.ExpandEnv(DefaultDBPath)
			db, err := bolt.Open(dbPath, perm.FileMode(), nil)
			if err != nil {
				return err
			}
			defer db.Close()

			bucket, err := cmd.Flags().GetString("bucket")
			if err != nil {
				return err
			}
			key, err := cmd.Flags().GetString("key")
			if err != nil {
				return err
			}

			return db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(bucket))
				if b == nil {
					b, err = tx.CreateBucket([]byte(bucket))
					if err != nil {
						return err
					}
				}
				value := b.Get([]byte(key))
				if len(value) == 0 {
					fmt.Fprintln(os.Stderr, "empty value")
				} else {
					fmt.Println(string(value))
				}
				return nil
			})
		},
	}
	getCmd.Flags().StringP("key", "k", "", "key")
	getCmd.Flags().StringP("bucket", "b", "default", "bucket")
	return getCmd

}

func newPutCmd() *cobra.Command {
	var putCmd = &cobra.Command{
		Use:   "put",
		Short: "put key value pair.",
		RunE: func(cmd *cobra.Command, args []string) error {

			perm := permos.Perm{UserRead: true}
			dbPath := os.ExpandEnv(DefaultDBPath)
			db, err := bolt.Open(dbPath, perm.FileMode(), nil)
			if err != nil {
				return err
			}
			defer db.Close()

			bucket, err := cmd.Flags().GetString("bucket")
			if err != nil {
				return err
			}
			key, err := cmd.Flags().GetString("key")
			if err != nil {
				return err
			}
			value, err := cmd.Flags().GetString("value")
			if err != nil {
				return err
			}

			return db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(bucket))
				if b == nil {
					b, err = tx.CreateBucket([]byte(bucket))
					if err != nil {
						return err
					}
				}
				return b.Put([]byte(key), []byte(value))
			})
		},
	}
	putCmd.Flags().StringP("key", "k", "", "key")
	putCmd.Flags().StringP("value", "v", "", "value")
	putCmd.Flags().StringP("bucket", "b", "default", "bucket")
	return putCmd

}

func newInstallCmd() *cobra.Command {
	var installCmd = &cobra.Command{
		Use:   "install",
		Short: "install bbolt",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := InstallBBolt(); err != nil {
				return err
			}
			return nil
		},
	}
	return installCmd

}

func main() {
	root := new(cobra.Command)
	root.AddCommand(newInstallCmd())
	root.AddCommand(newPutCmd())
	root.AddCommand(newGetCmd())
	root.AddCommand(newDeleteCmd())
	root.AddCommand(newListBucketsCmd())

	bucketCmd := newBucketCmd()
	bucketCmd.AddCommand(newListKeysBucketCmd())
	root.AddCommand(bucketCmd)
	if err := root.Execute(); err != nil {
		logrus.WithError(err).Error("error")
		os.Exit(1)
	}
}
