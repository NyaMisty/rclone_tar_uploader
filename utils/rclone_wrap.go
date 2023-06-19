package utils

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	_ "github.com/rclone/rclone/backend/all"
	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/rc/rcflags"
	"github.com/rclone/rclone/fs/rc/rcserver"
	"github.com/rclone/rclone/fs/sync"
)

func Rclone_init(addr string) {
	var err error

	globalConfig := fs.GetConfig(nil)
	globalConfig.StreamingUploadCutoff = fs.SizeSuffix(0)
	globalConfig.LogLevel.Set("DEBUG")

	configfile.Install()
	accounting.Start(context.Background())

	rcflags.Opt.HTTP.ListenAddr = []string{addr}
	rcflags.Opt.Auth.BasicUser = "rclone"
	rcflags.Opt.Auth.BasicPass = "rclone"
	rcflags.Opt.WebUI = true
	rcflags.Opt.WebGUIFetchURL = "https://api.github.com/repos/rclone/rclone-webui-react/releases/latest"
	rcflags.Opt.WebGUINoOpenBrowser = true
	rcflags.Opt.AccessControlAllowOrigin = "*"
	rcflags.Opt.EnableMetrics = true
	rcflags.Opt.Enabled = true
	_, err = rcserver.Start(context.Background(), &rcflags.Opt)
	if err != nil {
		panic(err)
	}
}

func MkdirAll(path string) error {
	fdst := cmd.NewFsDir([]string{path})
	err := operations.Mkdir(context.Background(), fdst, "")
	return err
}

func MoveFiles(src string, dst string) error {
	fsrc, srcFileName, fdst := cmd.NewFsSrcFileDst([]string{src, dst})
	if srcFileName == "" {
		return sync.MoveDir(context.Background(), fdst, fsrc, false, false)
	}
	return operations.MoveFile(context.Background(), fdst, fsrc, srcFileName, srcFileName)
}

func DownloadFile(ctx context.Context, dst string) (fs.Object, io.ReadCloser, error) {
	newCtx, fi := filter.AddConfig(ctx)
	f, filename := cmd.NewFsFile(dst)
	if filename == "" {
		return nil, nil, fmt.Errorf("should upload to a file instead of folder")
	}
	if err := fi.AddFile(filename); err != nil {
		return nil, nil, fmt.Errorf("cannot limit to single file: %w", err)
	}

	var o fs.Object
	err := operations.ListFn(newCtx, f, func(_o fs.Object) {
		o = _o
	})
	if err != nil {
		return nil, nil, fmt.Errorf("cannot list file: %w", err)
	}
	rc, err := operations.NewReOpen(newCtx, o, 5)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file: %w", err)
	}
	return o, rc, nil
}

func UploadFile(dst string, buffer []byte) (fs.Object, error) {
	fdst, dstFileName := cmd.NewFsDstFile([]string{dst})
	obj, err := operations.RcatSize(context.Background(), fdst, dstFileName, io.NopCloser(bytes.NewReader(buffer)), int64(len(buffer)), time.Now(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to rclone upload: %w", err)
	}
	return obj, nil
}

func UploadFileReader(dst string, reader io.Reader, size int64) (fs.Object, error) {
	fdst, dstFileName := cmd.NewFsDstFile([]string{dst})
	obj, err := operations.RcatSize(context.Background(), fdst, dstFileName, io.NopCloser(reader), size, time.Now(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to rclone upload: %w", err)
	}
	return obj, nil
}

type RcloneWriter struct {
	*io.PipeWriter
	closeChan chan int
	RetObj    fs.Object
	RetError  error
}

func (w *RcloneWriter) Close() error {
	if err := w.PipeWriter.Close(); err != nil {
		return err
	}
	<-w.closeChan
	return w.RetError
}

func (w *RcloneWriter) StartRclone(dst string, pipereader io.ReadCloser) {
	fdst, dstFileName := cmd.NewFsDstFile([]string{dst})
	obj, err := operations.Rcat(context.Background(), fdst, dstFileName, pipereader, time.Now(), nil)
	w.RetObj = obj
	w.RetError = err
	w.closeChan <- 1
}

var _ = io.WriteCloser(&RcloneWriter{})

func UploadFileWriter(dst string) io.WriteCloser {
	pipereader, pipewriter := io.Pipe()
	w := &RcloneWriter{
		PipeWriter: pipewriter,
		closeChan:  make(chan int),
	}
	go w.StartRclone(dst, pipereader)
	return w
}
