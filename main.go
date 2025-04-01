package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Event struct {
	Bucket  string   `json:"bucket"`
	Prefixs []string `json:"prefixs"`
	FileUploadPath string   `json:"file_upload_path"`
}
type BucketBasics struct {
	S3Client *s3.Client
}

var bucketBasics BucketBasics

func init() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(fmt.Sprintf("unable to load SDK config, %v", err))
	}
	bucketBasics = BucketBasics{
		S3Client: s3.NewFromConfig(cfg),
	}
	fmt.Println("✅ AWS S3 client initialized successfully!")
}

func (basics BucketBasics) Handler(ctx context.Context, event Event) (string, error) {
	//iterate over the prefixes and list objects for each prefix
	// var combinedContent string
	var sb strings.Builder
	for _, prefix := range event.Prefixs {
		// result will contain the list of objects for the given prefix
		result, err := basics.S3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: &event.Bucket,
			Prefix: &prefix,
		})
		if err != nil {
			return "", fmt.Errorf("failed to list objects in bucket %s with prefix %s: %w", event.Bucket, prefix, err)
		}
		// iterate over the objects and appends the file content
		for _, object := range result.Contents {
			getObjectOutput, err := basics.S3Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: &event.Bucket,
				Key:    object.Key,
			})
			if err != nil {
				return "", fmt.Errorf("failed to get object %s from bucket %s: %w", *object.Key, event.Bucket, err)
			}

			func() {
				defer getObjectOutput.Body.Close()
				// Read the content of the object and append it to the combinedContent
				if _, err := io.Copy(&sb, getObjectOutput.Body); err != nil {
					fmt.Printf(" Failed to read object %s: %v\n", *object.Key, err)
					return
				}
			}()

			sb.WriteString("\n") // Add a newline between file contents
		}
	}
	// write the combined content to a temporary file
    tempFileName := "/tmp/combined.tf"
    file, err := os.Create(tempFileName)
    if err != nil {
        return "", fmt.Errorf("failed to create temporary file: %w", err)
    }
    defer file.Close()

    _, err = file.WriteString(sb.String())
    if err != nil {
        return "", fmt.Errorf("failed to write to temporary file: %w", err)
    }

	 // uploading the file to the specified file_upload_path
	 file, err = os.Open(tempFileName)
	 if err != nil {
		 return "", fmt.Errorf("failed to open temporary file for upload: %w", err)
	 }
	 defer file.Close()
 
	 _, err = basics.S3Client.PutObject(ctx, &s3.PutObjectInput{
		 Bucket: &event.Bucket,
		 Key:    &event.FileUploadPath,
		 Body:   file,
		 ContentType: aws.String("text/plain"), // Optional: Set content type
	 })
	 if err != nil {
		 return "", fmt.Errorf("failed to upload file to %s/%s: %w", event.Bucket, event.FileUploadPath, err)
	 }

	return sb.String(), nil
}

func main() {
	lambda.Start(bucketBasics.Handler)
}
