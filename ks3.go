package utils

import (
	"github.com/ks3sdklib/aws-sdk-go/aws"
	"github.com/ks3sdklib/aws-sdk-go/aws/credentials"
	"github.com/ks3sdklib/aws-sdk-go/service/s3"
	"os"
	"bytes"
	log "github.com/thinkphoebe/golog"
)

type Ks3Config struct {
	Region   string
	Endpoint string
	Ak       string
	Sk       string
	Bucket   string
}

type Ks3Client struct {
	client *s3.S3
	bucket string
}

func (this *Ks3Client) Init(config *Ks3Config) {
	log.Infof("[Ks3Client.Init] %#v", config)
	c := credentials.NewStaticCredentials(config.Ak, config.Sk, "")
	this.client = s3.New(&aws.Config{
		Region:           config.Region,
		Credentials:      c,
		Endpoint:         config.Endpoint,
		DisableSSL:       true,
		LogLevel:         0, // 0->关闭，1->开启
		S3ForcePathStyle: false,
		LogHTTPBody:      true,
		Logger:           os.Stderr,
	})
	this.bucket = config.Bucket
}

func (this *Ks3Client) Upload(objectKey string, content []byte, isPrivate bool) error {
	acl := "public-read"
	if isPrivate {
		acl = "private"
	}

	params := &s3.PutObjectInput{
		Bucket:      aws.String(this.bucket),
		Key:         aws.String(objectKey),
		ACL:         aws.String(acl), //权限，支持private(私有)，public-read(公开读)
		Body:        bytes.NewReader(content),
		ContentType: aws.String("application/ocet-stream"),
		Metadata: map[string]*string{
		},
	}
	resp, err := this.client.PutObject(params)
	if err != nil {
		log.Errorf("[Ks3Client.Upload] client.PutObject got error [%v], objectKey:%s, resp:%#v",
			err, objectKey, resp)
		return err
	}

	log.Debugf("[Ks3Client.Upload] OK, objectKey:%s", objectKey)
	return nil
}
