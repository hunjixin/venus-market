package utils

import (
	"bytes"
	"os/exec"
	"strings"

	"github.com/mitchellh/go-homedir"
	"golang.org/x/xerrors"
)

func Move(from, to string) error {
	from, err := homedir.Expand(from)
	if err != nil {
		return xerrors.Errorf("Move: expanding from: %w", err)
	}

	to, err = homedir.Expand(to)
	if err != nil {
		return xerrors.Errorf("Move: expanding to: %w", err)
	}

	log.Debugw("Move file", "from", from, "to", to)
	var errOut bytes.Buffer
	cmd := exec.Command("/usr/bin/env", "mv", from, to) // nolint
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return xerrors.Errorf("exec mv (stderr: %s): %w", strings.TrimSpace(errOut.String()), err)
	}

	return nil
}
