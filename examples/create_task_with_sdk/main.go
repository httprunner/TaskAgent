package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
)

func main() {
	tableURL := flag.String("table", os.Getenv("TASK_BITABLE_URL"), "Feishu task table URL")
	aID := flag.String("aid", "", "aid that will be nested inside Params JSON")
	eID := flag.String("eid", "", "eid that will be nested inside Params JSON")
	timeout := flag.Duration("timeout", 30*time.Second, "Request timeout")
	flag.Parse()

	if strings.TrimSpace(*tableURL) == "" {
		log.Fatal("missing task table url (pass -table or export TASK_BITABLE_URL)")
	}
	if strings.TrimSpace(*aID) == "" {
		log.Fatal("missing -aid value")
	}
	if strings.TrimSpace(*eID) == "" {
		log.Fatal("missing -eid value")
	}

	client, err := taskagent.NewFeishuClientFromEnv()
	if err != nil {
		log.Fatalf("init Feishu client: %v", err)
	}

	itemID := strings.TrimSpace(*eID)
	payload, err := json.Marshal(map[string]string{
		"type": "auto_additional_crawl",
		"aid":  strings.TrimSpace(*aID),
		"eid":  itemID,
	})
	if err != nil {
		log.Fatalf("encode Params JSON: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	record := taskagent.TaskRecordInput{
		App:     "com.smile.gifmaker",
		Scene:   taskagent.SceneVideoScreenCapture,
		ItemID:  itemID,
		Params:  string(payload),
		Status:  "pending",
		Webhook: "pending",
	}

	id, err := client.CreateTaskRecord(ctx, strings.TrimSpace(*tableURL), record, nil)
	if err != nil {
		log.Fatalf("create task record: %v", err)
	}

	fmt.Printf("Created task record %s\n", id)
}
