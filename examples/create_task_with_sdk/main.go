package main

import (
	"context"
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
	bookID := flag.String("bid", "", "book id")
	userID := flag.String("uid", "", "user id")
	eID := flag.String("eid", "", "eid")
	timeout := flag.Duration("timeout", 30*time.Second, "Request timeout")
	flag.Parse()

	if strings.TrimSpace(*tableURL) == "" {
		log.Fatal("missing task table url (pass -table or export TASK_BITABLE_URL)")
	}
	if strings.TrimSpace(*bookID) == "" {
		log.Fatal("missing -bid value")
	}
	if strings.TrimSpace(*userID) == "" {
		log.Fatal("missing -uid value")
	}
	if strings.TrimSpace(*eID) == "" {
		log.Fatal("missing -eid value")
	}

	client, err := taskagent.NewFeishuClientFromEnv()
	if err != nil {
		log.Fatalf("init Feishu client: %v", err)
	}

	aID := fmt.Sprintf("快手_%s_%s", strings.TrimSpace(*bookID), strings.TrimSpace(*userID))
	itemID := strings.TrimSpace(*eID)

	record := taskagent.TaskRecordInput{
		App:     "com.smile.gifmaker",
		Scene:   taskagent.SceneVideoScreenCapture,
		BookID:  *bookID,
		UserID:  *userID,
		ItemID:  itemID,
		GroupID: aID,
		Status:  "pending",
		Extra:   "auto_additional_crawl",
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	id, err := client.CreateTaskRecord(ctx, strings.TrimSpace(*tableURL), record, nil)
	if err != nil {
		log.Fatalf("create task record: %v", err)
	}

	fmt.Printf("Created task record %s\n", id)
}
