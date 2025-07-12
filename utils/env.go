package utils

import (
	"os"
	"strings"
)

func LoadEnvFile() error {
	f, err := os.ReadFile("./.env")
	if err != nil {
		return err
	}

	ls := strings.Split(string(f), "\n")
	for _, l := range ls {
		ps := strings.SplitN(l, "=", 2)

		err := os.Setenv(ps[0], ps[1])
		if err != nil {
			return err
		}
	}

	return nil
}
