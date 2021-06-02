package mounter

import (
	"fmt"
	"github.com/kubernetes-sigs/nfs-ganesha-server-and-external-provisioner/pkg/s3"
	"os"
	"path"
)

// Implements Mounter
type rcloneMounter struct {
	meta            *s3.FSMeta
	url             string
	region          string
	accessKeyID     string
	secretAccessKey string
	additionalArgs  []string
}

const (
	rcloneCmd = "rclone"
)

func newRcloneMounter(meta *s3.FSMeta, cfg *s3.Config, additionalArgs []string) (Mounter, error) {
	return &rcloneMounter{
		meta:            meta,
		url:             cfg.Endpoint,
		region:          cfg.Region,
		accessKeyID:     cfg.AccessKeyID,
		secretAccessKey: cfg.SecretAccessKey,
		additionalArgs:  additionalArgs,
	}, nil
}

func (rclone *rcloneMounter) Mount(target string, fork bool) error {
	args := []string{
		"mount",
		fmt.Sprintf(":s3:%s", path.Join(rclone.meta.BucketName, rclone.meta.Prefix, rclone.meta.FSPath)),
		fmt.Sprintf("%s", target),
		"--daemon",
		"--s3-provider=AWS",
		"--s3-env-auth=true",
		fmt.Sprintf("--s3-region=%s", rclone.region),
		fmt.Sprintf("--s3-endpoint=%s", rclone.url),
		"--allow-other",
	}
	args = append(args, rclone.additionalArgs...)
	os.Setenv("AWS_ACCESS_KEY_ID", rclone.accessKeyID)
	os.Setenv("AWS_SECRET_ACCESS_KEY", rclone.secretAccessKey)
	if fork {
		return fuseMountFork(target, rcloneCmd, args)
	}
	return fuseMount(target, rcloneCmd, args)
}
