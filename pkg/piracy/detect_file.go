package piracy

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// DetectFromFiles performs piracy detection from local files (JSONL + CSV)
// This function uses the common detection logic (DetectCommon)
func DetectFromFiles(dataFilePath, dramaFilePath string, filterRate float64, outputCSV string) error {
	// Validate required parameters
	if dataFilePath == "" {
		return fmt.Errorf("data file path is required")
	}
	if dramaFilePath == "" {
		return fmt.Errorf("drama file path is required")
	}

	// Read video records from JSONL file
	contentRecords, err := readVideoRecords(dataFilePath)
	if err != nil {
		return fmt.Errorf("failed to read data file: %w", err)
	}
	log.Printf("Successfully read %d video records\n", len(contentRecords))

	// Read drama list from CSV file
	dramaRecords, err := readDramaRecords(dramaFilePath)
	if err != nil {
		return fmt.Errorf("failed to read drama file: %w", err)
	}
	log.Printf("Successfully read %d dramas\n", len(dramaRecords))

	// Run piracy detection using common logic
	threshold := filterRate / 100 // Convert percentage to ratio
	report := DetectCommon(contentRecords, dramaRecords, threshold)
	log.Printf("Detection completed, %d suspicious clusters found\n", len(report.Matches))

	// Output results
	baseDir := filepath.Dir(dataFilePath)
	baseName := strings.TrimSuffix(filepath.Base(dataFilePath), filepath.Ext(dataFilePath))

	// Build console output
	var output strings.Builder
	output.WriteString("=== Piracy Detection Results ===\n")

	if len(report.Matches) > 0 {
		// Group matches by Params (drama name)
		matchesByParams := make(map[string][]Match)
		for _, match := range report.Matches {
			matchesByParams[match.Params] = append(matchesByParams[match.Params], match)
		}

		// Convert to array for sorting
		type dramaResult struct {
			params           string
			matches          []Match
			originalDuration float64
		}
		var dramaResults []dramaResult
		for params, matches := range matchesByParams {
			if len(matches) == 0 {
				continue
			}
			dramaResults = append(dramaResults, dramaResult{
				params:           params,
				matches:          matches,
				originalDuration: matches[0].TotalDuration,
			})
		}

		// Sort by number of suspicious authors descending
		sort.Slice(dramaResults, func(i, j int) bool {
			return len(dramaResults[i].matches) > len(dramaResults[j].matches)
		})

		// Display results
		for i, dr := range dramaResults {
			output.WriteString(fmt.Sprintf("\n%d. Drama: %s (Original duration: %.0f seconds)\n", i+1, dr.params, dr.originalDuration))
			output.WriteString("   Suspected piracy authors:\n")

			displayedCount := 0
			for _, match := range dr.matches {
				ratioPct := match.Ratio * 100
				if ratioPct >= filterRate {
					output.WriteString(fmt.Sprintf("   - Author: %s, Videos: %d, Total duration: %.0f seconds (%.1f%%)\n",
						match.UserName, match.RecordCount, match.SumDuration, ratioPct))
					displayedCount++
				}
			}

			filteredCount := len(dr.matches) - displayedCount
			if filteredCount > 0 {
				output.WriteString(fmt.Sprintf("   ... %d more authors below %.1f%% (see summary CSV)\n",
					filteredCount, filterRate))
			}
		}

		// Print to console
		log.Println("\n" + output.String())
	} else {
		log.Println("\nNo suspected piracy dramas found")
	}

	// Generate summary CSV
	var summaryFile string
	if outputCSV != "" {
		summaryFile = outputCSV
	} else {
		summaryFile = filepath.Join(baseDir, baseName+"_summary.csv")
	}

	if err := generateSummaryCSV(report.Matches, summaryFile); err != nil {
		log.Printf("Failed to save summary CSV: %v", err)
	} else {
		log.Printf("Summary CSV saved to: %s\n", summaryFile)
	}

	// Print statistics
	log.Printf("\n=== Statistics ===\n")
	log.Printf("Total video records: %d\n", report.ResultRows)
	log.Printf("Dramas in catalog: %d\n", report.TaskRows)
	log.Printf("Suspicious clusters: %d\n", len(report.Matches))

	return nil
}

// readVideoRecords reads video records from JSONL file and converts to ContentRecord format
func readVideoRecords(filePath string) ([]ContentRecord, error) {
	type videoRecordJSON struct {
		Query    string `json:"query"`
		UserID   string `json:"user_id"`
		UserName string `json:"user_name"`
		Duration int    `json:"duration"` // milliseconds
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	var records []ContentRecord
	decoder := json.NewDecoder(file)

	const offlineApp = "offline"
	for {
		var record videoRecordJSON
		if err := decoder.Decode(&record); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to decode JSON line: %w", err)
		}
		query := strings.TrimSpace(record.Query)
		records = append(records, ContentRecord{
			Params:      query,
			BookID:      query,
			App:         offlineApp,
			UserID:      record.UserID,
			UserName:    record.UserName,
			DurationSec: float64(record.Duration) / 1000, // convert to seconds
		})
	}

	return records, nil
}

// readDramaRecords reads drama list from CSV file and converts to DramaRecord format
func readDramaRecords(filePath string) ([]DramaRecord, error) {
	const (
		HeaderDramaNameList    = "短剧名称"
		HeaderDramaDurationSec = "短剧时长（秒）"
	)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = '\t' // Use tab delimiter

	// Read header row
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	// Find column indices
	var nameIndex, durationIndex int
	nameFound, durationFound := false, false
	for i, header := range headers {
		cleanHeader := strings.TrimSpace(header)
		switch cleanHeader {
		case HeaderDramaNameList:
			nameIndex = i
			nameFound = true
		case HeaderDramaDurationSec:
			durationIndex = i
			durationFound = true
		}
	}

	if !nameFound {
		return nil, fmt.Errorf("CSV file missing '%s' column", HeaderDramaNameList)
	}
	if !durationFound {
		return nil, fmt.Errorf("CSV file missing '%s' column", HeaderDramaDurationSec)
	}

	var dramas []DramaRecord

	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read CSV line: %w", err)
		}

		if len(record) <= nameIndex || len(record) <= durationIndex {
			continue // Skip incomplete rows
		}

		name := strings.TrimSpace(record[nameIndex])
		if name == "" {
			continue // Skip empty names
		}

		durationStr := strings.TrimSpace(record[durationIndex])
		if durationStr == "" {
			continue // Skip empty durations
		}

		// Convert duration to float64 (seconds)
		duration, err := strconv.ParseFloat(durationStr, 64)
		if err != nil {
			continue // Skip invalid durations
		}

		dramas = append(dramas, DramaRecord{
			BookID:   name,
			Name:     name,
			Duration: duration,
		})
	}

	return dramas, nil
}

// CSV Header constants for piracy detection results
const (
	// HeaderDramaName is the CSV header for drama name (短剧名称)
	HeaderDramaName = "短剧名称"
	// HeaderOriginalDuration is the CSV header for original drama duration (原剧时长(秒))
	HeaderOriginalDuration = "原剧时长(秒)"
	// HeaderPirateAuthor is the CSV header for pirate author name (盗版作者)
	HeaderPirateAuthor = "盗版作者"
	// HeaderUserID is the CSV header for user ID (用户ID)
	HeaderUserID = "用户ID"
	// HeaderVideoCount is the CSV header for video count (视频数)
	HeaderVideoCount = "视频数"
	// HeaderTotalDuration is the CSV header for total video duration (视频总时长(秒))
	HeaderTotalDuration = "视频总时长(秒)"
	// HeaderDurationRatio is the CSV header for duration ratio (视频时长占比)
	HeaderDurationRatio = "视频时长占比"
)

// generateSummaryCSV generates summary CSV for detected piracy
func generateSummaryCSV(matches []Match, outputFile string) error {

	// Create output directory if needed
	if err := os.MkdirAll(filepath.Dir(outputFile), 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Open output file
	outFile, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Setup CSV writer with tab delimiter
	writer := csv.NewWriter(outFile)
	writer.Comma = '\t'
	defer writer.Flush()

	// Write headers
	headers := []string{HeaderDramaName, HeaderOriginalDuration, HeaderPirateAuthor, HeaderUserID, HeaderVideoCount, HeaderTotalDuration, HeaderDurationRatio}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write headers: %w", err)
	}

	// Write data rows
	for _, match := range matches {
		record := []string{
			cleanString(match.Params),                // Drama name
			fmt.Sprintf("%.0f", match.TotalDuration), // Original duration
			cleanString(match.UserName),              // Pirate author
			cleanString(match.UserID),                // User ID
			strconv.Itoa(match.RecordCount),          // Video count
			fmt.Sprintf("%.0f", match.SumDuration),   // Total video duration
			fmt.Sprintf("%.1f%%", match.Ratio*100),   // Duration ratio
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	return nil
}

// cleanString removes special characters from string for CSV compatibility
func cleanString(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	return s
}

// ParseSummaryCSV reads a piracy detection result CSV file and returns a map of userID to UserDramaInfo.
// This function helps convert piracy detection results into a format suitable for search agents.
// It reads the CSV file with adaptive delimiter detection and extracts:
// - UserID
// - UserName (PirateAuthor)
// - List of Dramas for each user
func ParseSummaryCSV(csvPath string) (map[string]UserDramaInfo, error) {
	file, err := os.Open(csvPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	allRecords, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(allRecords) == 0 {
		return nil, fmt.Errorf("CSV file is empty")
	}

	// Parse header row
	header := allRecords[0]
	dramaNameIndex := -1
	userIDIndex := -1
	userNameIndex := -1

	for i, column := range header {
		column = strings.TrimSpace(column)
		switch column {
		case HeaderDramaName:
			dramaNameIndex = i
		case HeaderUserID:
			userIDIndex = i
		case HeaderPirateAuthor:
			userNameIndex = i
		}
	}

	if dramaNameIndex == -1 {
		return nil, fmt.Errorf("column '%s' not found in CSV file", HeaderDramaName)
	}
	if userIDIndex == -1 {
		return nil, fmt.Errorf("column '%s' not found in CSV file", HeaderUserID)
	}
	if userNameIndex == -1 {
		return nil, fmt.Errorf("column '%s' not found in CSV file", HeaderPirateAuthor)
	}

	// Build user map
	result := make(map[string]UserDramaInfo)

	for i := 1; i < len(allRecords); i++ {
		record := allRecords[i]
		if len(record) <= max(dramaNameIndex, userIDIndex, userNameIndex) {
			continue // Skip incomplete rows
		}

		dramaName := strings.TrimSpace(record[dramaNameIndex])
		userID := strings.TrimSpace(record[userIDIndex])
		userName := strings.TrimSpace(record[userNameIndex])

		// Filter out empty or (null) values
		if dramaName == "" || dramaName == "(null)" || dramaName == HeaderDramaName ||
			userID == "" || userID == "(null)" ||
			userName == "" || userName == "(null)" {
			continue
		}

		// Update existing user or create new entry
		if userInfo, exists := result[userID]; exists {
			// Check if drama already exists
			found := false
			for _, existingDrama := range userInfo.Dramas {
				if existingDrama == dramaName {
					found = true
					break
				}
			}
			if !found {
				userInfo.Dramas = append(userInfo.Dramas, dramaName)
				result[userID] = userInfo
			}
		} else {
			result[userID] = UserDramaInfo{
				UserID:   userID,
				UserName: userName,
				Dramas:   []string{dramaName},
			}
		}
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no valid user information found in CSV file")
	}

	return result, nil
}

// max returns the maximum of integers
func max(nums ...int) int {
	if len(nums) == 0 {
		return 0
	}
	m := nums[0]
	for _, n := range nums {
		if n > m {
			m = n
		}
	}
	return m
}
