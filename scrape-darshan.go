package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
)

// const TARGET_MOUNT_POINT string = "/scratch2"
const TARGET_MOUNT_POINT string = "/var/opt/cray/dws/mounts/batch"
const NUM_THREADS int = 4

// return both the file name and its match count
type MountCount struct {
	filename string
	count    int
	err      error
}

func scan_darshanlog(filename string) (count int, err error) {
	var out bytes.Buffer
	if strings.HasSuffix(filename, ".txt.gz") {
		cmd := exec.Command("gunzip", "-c", filename)
	} else {
		cmd := exec.Command("darshan-parser", filename)
	}
	// use blocking Run and load store all of stdout in a bytes.Buffer
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		count = -1
		return
	}
	scanner := bufio.NewScanner(strings.NewReader(out.String()))
	for scanner.Scan() {
		/*
			// alternatively, use non-blocking Start and block on processing output from the StdoutPipe
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				count = -1
				return
			}
			cmd.Start()
			reader := bufio.NewReader(stdout)
			for {
				line, _, err := reader.ReadLine()
				if err != nil {
					break
				}
				tokens := strings.Split(line, ":")
		*/
		// "# mount entry: 8157206977945068522   /global/cscratch1   lustre"
		tokens := strings.Split(scanner.Text(), ":")
		if len(tokens) >= 1 && tokens[0] == "# mount entry" {
			tokens = strings.Fields(tokens[1])
			// DWFS mounts appear as /var/opt/cray/dws/mounts/batch
			if strings.HasPrefix(tokens[1], TARGET_MOUNT_POINT) {
				count++
			}
		}
	}
	return
}

func worker(id int, filenames <-chan string, results chan<- MountCount) {
	var result MountCount

	for filename := range filenames {
		log.Printf("Task %2d processing %s\n", id, filename)
		result.filename = filename
		result.count, result.err = scan_darshanlog(filename)
		results <- result // this blocks
	}
}

func main() {
	file_list := os.Args[1:]
	n_files := len(file_list)
	fmt.Printf("Got %d files to process\n", n_files)

	filenames := make(chan string, n_files)
	results := make(chan MountCount, n_files)

	// initialize workers
	for i := 0; i < NUM_THREADS; i++ {
		go worker(i, filenames, results)
	}

	// load up the job queue
	for _, file := range file_list {
		filenames <- file
	}
	close(filenames)

	// retrieve results from job queue
	// `for result := range results` results in a hang for some reason
	for i := 0; i < n_files; i++ {
		result := <-results
		log.Printf("Got result from %s\n", result.filename)
		if result.err != nil {
			log.Println(result.err)
		} else if result.count > 0 {
			log.Printf("%s contains %d relevant mounts\n",
				result.filename,
				result.count)
		}
	}
}
