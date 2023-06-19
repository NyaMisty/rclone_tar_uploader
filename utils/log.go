package utils

import (
	"fmt"
	"github.com/rifflock/lfshook"
	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/writer"
	"io/ioutil"
	"os"
)

func InitLog(logName string) {
	stdFormatter := &log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02.15:04:05.000000",
		ForceColors:     true,
		DisableColors:   false,
	}
	fileFormatter := &log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02.15:04:05.000000",
		ForceColors:     false,
		DisableColors:   true,
	}
	log.SetFormatter(stdFormatter)
	log.SetLevel(log.DebugLevel)

	logPath, _ := os.Getwd() // 后期可以配置
	logFile := fmt.Sprintf("%s/worker-%s.log", logPath, logName)
	errlogFile := fmt.Sprintf("%s/worker-%s_err.log", logPath, logName)
	lfHook := lfshook.NewHook(lfshook.PathMap{
		log.PanicLevel: logFile,
		log.FatalLevel: logFile,
		log.ErrorLevel: logFile,
		log.WarnLevel:  logFile,
		log.InfoLevel:  logFile,
		log.DebugLevel: logFile,
	}, fileFormatter)
	lferrHook := lfshook.NewHook(lfshook.PathMap{
		log.PanicLevel: errlogFile,
		log.FatalLevel: errlogFile,
		log.ErrorLevel: errlogFile,
		log.WarnLevel:  errlogFile,
	}, fileFormatter)

	log.SetOutput(ioutil.Discard)
	log.AddHook(&writer.Hook{
		Writer: os.Stderr,
		LogLevels: []log.Level{
			log.PanicLevel,
			log.FatalLevel,
			log.ErrorLevel,
			log.WarnLevel,
			log.InfoLevel,
		},
	})
	log.AddHook(lferrHook)
	log.AddHook(lfHook)
}
