package main

import (
	"bytes"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
)

var (
	sess = session.New()
	bucket = "download-demo"
	region = "us-west-2"
	errorLogger = log.New(os.Stderr, "ERROR ", log.Llongfile)
)

func init() {
	sess = session.New()
}

func show(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	s3Key := ""
	if val, ok := req.PathParameters["proxy"]; !ok {
		return clientError(404)
	} else {
		s3Key = val
	}

	isInfo := false
	info := ""
	if _, ok := req.QueryStringParameters["avinfo"]; ok {
		isInfo = true
	}

	fileOut := "/tmp/" + s3Key

	err := downloadS3ToFile(sess, bucket, s3Key, fileOut)
	if err != nil {
		log.Printf("downloadS3ToFile err is : %s \n", err.Error())
		return serverError(err)
	}

	if isInfo {
		info, err = getMetaInfo(fileOut)
		if err != nil {
			log.Printf("getMetaInfo err is : %s \n", err.Error())
			return serverError(err)
		}
		return infoResp(info)
	} else {
		return get302Resp(bucket, s3Key)
	}
}

// Add a helper for handling errors. This logs any error to os.Stderr
// and returns a 500 Internal Server Error response that the AWS API
// Gateway understands.
func serverError(err error) (events.APIGatewayProxyResponse, error) {
	errorLogger.Println(err.Error())
	return events.APIGatewayProxyResponse{
		StatusCode: http.StatusInternalServerError,
		Body:       http.StatusText(http.StatusInternalServerError),
	}, nil
}

// Similarly add a helper for send responses relating to client errors.
func clientError(status int) (events.APIGatewayProxyResponse, error) {
	return events.APIGatewayProxyResponse{
		StatusCode: status,
		Body:       http.StatusText(status),
	}, nil
}

func infoResp(info string) (events.APIGatewayProxyResponse, error) {
	return events.APIGatewayProxyResponse{
		StatusCode: http.StatusOK,
		Body: info,
	}, nil
}

func get302Resp(bucket, key string) (events.APIGatewayProxyResponse, error) {
	location := "https://" + bucket + ".s3." + region + ".amazonaws.com/" + key
	return events.APIGatewayProxyResponse{
		StatusCode: http.StatusFound,
		Headers: map[string]string{"Location": location},
	}, nil
}

func main() {
	lambda.Start(show)
}

func downloadS3ToFile(sess *session.Session, s3Bucket, s3Key, filePath string) error {

	fileDir := filepath.Dir(filePath)
	err := os.MkdirAll(fileDir, os.ModePerm)
	if err != nil {
		return err
	}

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	downloader := s3manager.NewDownloader(sess)
	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(s3Bucket),
			Key:    aws.String(s3Key),
		})

	if err != nil {
		return err
	}

	log.Println("Downloaded", file.Name(), numBytes, "bytes")

	return nil
}

func getMetaInfo(fileFrom string) (string, error) {
	cmdArguments := []string{"-i", fileFrom, "-v", "quiet", "-print_format", "json", "-show_format", "-show_streams"}
	cmd := exec.Command("ffprobe", cmdArguments...)

	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Println(err.Error())
		return "", err
	}
	log.Printf("command output: \n %v", out.String())
	return out.String(), nil
}