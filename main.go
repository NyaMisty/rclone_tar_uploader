package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/rclone/rclone/fs"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"rclone_tar_uploader/utils"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	writerCount        = 10
	workerCount        = 64
	maxFileSize  int64 = 10 << 30 // 10GB
	outputPrefix       = ""
)

var downloadedBytes int64

type countingWriter struct{}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n := len(p)
	atomic.AddInt64(&downloadedBytes, int64(n))
	return n, nil
}

type fileData struct {
	name     string
	url      string
	metadata map[string]string
	data     *bytes.Buffer
}

var curTarSplit int64 = 0

func getNextTarFileName() string {
	allocated := atomic.AddInt64(&curTarSplit, 1)
	filename := fmt.Sprintf("%s%d.tar", outputPrefix, allocated)
	return filename
}

var urls = make(chan string)
var urls_count int64
var failedUrls sync.Map

func downloader(wg *sync.WaitGroup, files chan *fileData) {
	defer wg.Done()
	for url := range urls {
		var succ bool
		for i := 0; i < 5; i++ {
			succ = func() bool {
				resp, err := http.Get(url)
				if err != nil {
					log.Infof("Failed to download: %s\n", url)
					return false
				}
				defer resp.Body.Close()

				buf := &bytes.Buffer{}
				_, err = io.Copy(io.MultiWriter(&countingWriter{}, buf), resp.Body)
				if err != nil {
					log.Infof("Failed to read response body: %s\n", url)
					return false
				}

				reqheaders, err := json.Marshal(resp.Request.Header)
				if err != nil {
					panic("cannot marshal header")
				}

				respheaders, err := json.Marshal(resp.Header)
				if err != nil {
					panic("cannot marshal header")
				}

				files <- &fileData{
					name: filepath.Base(url),
					data: buf,
					url:  url,
					metadata: map[string]string{
						"req_headers":  string(reqheaders),
						"resp_headers": string(respheaders),
					},
				}
				return true
			}()
			if succ {
				break
			}
			time.Sleep(2 * time.Second)
		}
		atomic.AddInt64(&urls_count, -1)
		if !succ {
			log.Warnf("failed to download url %v", url)
			failedUrls.Store(url, 1)
		}
	}
}
func putURL(urls chan string, url string) {
	atomic.AddInt64(&urls_count, 1)
	urls <- url
}

var openedFile int64

func writer(wg *sync.WaitGroup, files chan *fileData) {
	defer wg.Done()
	defer close(files)

	var err error

	var tarFileName string
	var tarFile io.WriteCloser
	var tw *tar.Writer
	var fileSize int64

	var writtenURLs []string

	closeFile := func() error {
		defer atomic.AddInt64(&openedFile, -1)
		log.Infof("closing file %v", tarFileName)
		err = tw.Close()
		if err != nil {
			log.Infof("failed to close tarwriter: %v", err)
			panic("shouldn't fail")
		}
		err = tarFile.Close()
		if err != nil {
			log.Warnf("failed to close rclone file %v, retrying... err: %v", tarFileName, err)
			// errored, re-uploading
			go func() {
				for _, url := range writtenURLs {
					putURL(urls, url)
				}
			}()
			writtenURLs = []string{}
			return err
		}
		return nil
	}

	switchFile := func(closeOnly bool) {
		log.Infof("switching output file from: %v, closeOnly: %v", tarFileName, closeOnly)
		useSameName := false
		writtenURLs = []string{}
		if tarFile != nil {
			if err = closeFile(); err != nil {
				useSameName = true
				closeOnly = false
			}
			tarFile = nil
		}

		if closeOnly {
			return
		}
		if !useSameName {
			tarFileName = getNextTarFileName()
		}
		//var err error
		atomic.AddInt64(&openedFile, 1)
		tarFile = utils.UploadFileWriter(tarFileName)
		tw = tar.NewWriter(tarFile)
		fileSize = 0
	}

	for {
		var file *fileData
		select {
		case file = <-files:
			break
		case <-time.After(time.Second * 1):
			if val := atomic.LoadInt64(&urls_count); val == 0 {
				if tarFile != nil {
					switchFile(true)
				}
			}
			break
		}
		if file == nil {
			continue
		}

		if tarFile == nil || fileSize+int64(file.data.Len()) > maxFileSize {
			switchFile(false)
		}

		hdr := &tar.Header{
			Name:       file.name,
			Mode:       0600,
			Size:       int64(file.data.Len()),
			PAXRecords: map[string]string{},
		}
		for k, v := range file.metadata {
			hdr.PAXRecords["MISTYWARC."+k] = v
		}
		if err := tw.WriteHeader(hdr); err != nil {
			fmt.Printf("Failed to write tar header: %s\n", err)
			panic(111)
		}
		if _, err := tw.Write(file.data.Bytes()); err != nil {
			fmt.Printf("Failed to write file to tar: %s\n", err)
			panic(222)
		}

		fileSize += int64(file.data.Len())
	}
}

func startDownload(fileName string) {
	files := make(chan *fileData, 10)

	// Timer for bandwidth calculation
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		var prevBytes int64
		for range ticker.C {
			currentBytes := atomic.LoadInt64(&downloadedBytes)
			fmt.Printf("Downloaded: %.2f MB/s\n", float64(currentBytes-prevBytes)/1024/1024)
			prevBytes = currentBytes
		}
	}()

	// Spawn workers
	var downloaderWg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		downloaderWg.Add(1)
		go downloader(&downloaderWg, files)
	}

	// Tar file writer
	var writerWg sync.WaitGroup
	for i := 0; i < writerCount; i++ {
		writerWg.Add(1)
		go writer(&writerWg, files)
	}

	// Read urls from file and send to channel
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Printf("Failed to open file: %s\n", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		l := scanner.Text()
		var url string
		url = l
		if strings.HasPrefix(l, "\"") {
			err := json.Unmarshal([]byte(l), &url)
			if err != nil {
				fmt.Printf("Failed to unmarshal url: %s\n", scanner.Text())
				continue
			}
		}
		putURL(urls, url)
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Failed to scan file: %s\n", err)
		return
	}

	//close(urls)
	//downloaderWg.Wait()
	ensureTime := 0
	for {
		if t := atomic.LoadInt64(&openedFile); t == 0 {
			log.Infof("no opened files, waiting for final exit!")
			ensureTime += 1
		} else {
			fmt.Printf("remaining url: %v, workers: %v\n", urls_count, t)
			ensureTime = 0
		}
		time.Sleep(2 * time.Second)
		if ensureTime > 5 {
			log.Infof("FINAL EXIT!")
			break
		}
	}

	f, err := os.Create(fileName + "_fails")
	if err != nil {
		panic(err)
	}
	failedUrls.Range(func(key, value any) bool {
		if _, err := f.WriteString(key.(string)); err != nil {
			panic(err)
		}
		if _, err := f.WriteString("\n"); err != nil {
			panic(err)
		}
		return true
	})
	f.Close()
}

func main() {
	var err error
	fileName := os.Args[1]
	workerCount, err = strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	writerCount, err = strconv.Atoi(os.Args[3])
	if err != nil {
		panic(err)
	}
	var fileSize fs.SizeSuffix
	err = fileSize.Set(os.Args[4])
	if err != nil {
		panic(err)
	}
	maxFileSize = int64(fileSize)

	outputPrefix = os.Args[5]

	log.Warnf("started downloading %v!", fileName)
	http.DefaultClient.Transport = http.DefaultTransport
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = workerCount + 10
	http.DefaultTransport.(*http.Transport).MaxIdleConns = workerCount + 10

	utils.InitLog(fmt.Sprintf("tarupload-%v", fileName))
	utils.Rclone_init("0.0.0.0:0")
	startDownload(fileName)
}
