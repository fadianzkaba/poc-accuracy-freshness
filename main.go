package main

import (
	//"bytes"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

const region = "australia-southeast1"

type dbOutput struct {
	ocv            string
	customerNumber string
	utcTime        string
}

func main() {

	job, err := getJobID()

	gsutilOut, err := getFile(job)

	if err != nil {
		fmt.Errorf("Error returned:%w", err)
		return
	}

	output, err := getDataFromSpanner(gsutilOut)
	if err != nil {
		fmt.Errorf("Error returned:%w", err)
		return
	}

	strings.TrimSpace(output)

	if err != nil {
		fmt.Errorf("Error returned:%w", err)
		return
	}

	if output != "" && !strings.Contains(output, "NO ROWS") {
		fmt.Printf("Valid\n\nData from the file\n%s\n\nData from Spanner\n%s\n", gsutilOut, output)


	} else {
		fmt.Println("Not valid")
	}
}

func getJobID() (string, error) {
	// Get the Job Name
	jobCmd := exec.Command("bash", "-c",
		`gcloud dataflow jobs list --filter="name~score" --region=`+region+` | grep -v JOB_ID | awk '{print $1}' | head -1`)
	jobOut, err := jobCmd.Output()
	if err != nil {

		return "", fmt.Errorf("Error getting job:%w", err)
	}
	job := strings.TrimSpace(string(jobOut))
	return job, nil
}

func getFile(job string) ([]byte, error) {

	// Get the File Name from the Dataflow Job
	fileCmd := exec.Command("bash", "-c",
		`gcloud dataflow jobs describe `+job+` --full --region=`+region+` | yq '.environment.sdkPipelineOptions.["beam:option:go_options:v1"].options.src-file-path'`)
	fileOut, err := fileCmd.Output()
	if err != nil {
		fmt.Println("Error getting file:", err)
		return nil, fmt.Errorf("Error getting file from Dataflow: %w", err)
	}
	file := strings.TrimSpace(string(fileOut))

	// Get the OCV and Customer ID
	gsutilCmd := exec.Command("gsutil", "cat", file)
	gsutilOut, err := gsutilCmd.Output()
	if err != nil {
		return nil, fmt.Errorf("error reading file from GCS: %w", err)
	}

	return gsutilOut, nil
}

func getDataFromSpanner(gsutilOut []byte) (string, error) {

	// Split the first line by "|"
	fields := strings.Split(strings.TrimSpace(string(gsutilOut)), "|")

	if len(fields) < 6 {
		return "", fmt.Errorf("File content doesn't contain expected number of fields")
	}
	db := dbOutput{
		customerNumber: fields[0],
		ocv:            fields[1],
		utcTime:        convertToUTC(fields[5]),
	}

	// Validate the data exists in Spanner
	spannerSQL := fmt.Sprintf(`select customer_number,ocv_id,recommended_customer_score,recommended_customer_score_update_dt, recommended_customer_score_fit_for_use_ind, record_timestamp,created_time from customer_transaction_score where ocv_id="%s" AND customer_number=%s AND record_timestamp="%s"`, db.ocv, db.customerNumber, db.utcTime)
	spannerCmd := exec.Command("gcloud", "spanner", "databases", "execute-sql", "credit",
		"--instance=credit", "--sql="+spannerSQL)
	spannerOut, err := spannerCmd.Output()
	if err != nil {
		return "", fmt.Errorf("Error executing Spanner query:%w", err)
	}

	return string(spannerOut), nil
}
func convertToUTC(ltime string) string {

	layout := "2006-01-02 15:04:05.000"
	loc, err := time.LoadLocation("Australia/Sydney")
	if err != nil {
		panic(err)
	}

	localTime, err := time.ParseInLocation(layout, ltime, loc)
	if err != nil {
		panic(err)
	}

	utcTime := localTime.UTC()

	return utcTime.Format("2006-01-02T15:04:05.000000Z")

}
