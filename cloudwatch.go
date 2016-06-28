package main

import (
	"github.com/BurntSushi/toml"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"sort"
	"time"
)

type CloudWatch struct {
	Client *cloudwatch.CloudWatch
	config CloudWatchConfig
	log    Logger
}

type CloudWatchConfig struct {
	Region             string                    `toml:"region"`
	AWSAccessKeyId     string                    `toml:"aws_access_key_id"`
	AWSSecretAccessKey string                    `toml:"aws_secret_access_key"`
	Token              string                    `toml:"token"`
	Profile            string                    `toml:"profile"`
	Credentials        string                    `toml:"credentials"`
	Timezone           string                    `toml:"timezone"`
	Metrics            []CloudWatchMetricsConfig `toml:"metrics"`
}

type CloudWatchMetricsConfig struct {
	Dimensions []Dimension `toml:"dimensions"`
	Namespace  string      `toml:"namespace"`
	Period     int64       `toml:"period"`
	Statistics string      `toml:"statistics"`
	MetricsConfig
}

type Dimension struct {
	Name  string `toml:"name"`
	Value string `toml:"value"`
}

type MetricStatisticsInput struct {
	Dimensions []*cloudwatch.Dimension
	StartTime  time.Time
	EndTime    time.Time
	MetricName string
	Namespace  string
	Period     int64
	Statistics *string
	Unit       string
}

type Datapoint struct {
	Value     float64
	Timestamp time.Time
}

type Datapoints []Datapoint

func (d Datapoints) Len() int           { return len(d) }
func (d Datapoints) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
func (d Datapoints) Less(i, j int) bool { return d[i].Timestamp.Unix() < d[j].Timestamp.Unix() }

func NewCloudWatch(cwConfig CloudWatchConfig, filename string, log Logger) (Input, error) {
	var err error
	var cw *CloudWatch
	var str string
	str, err = LoadFile(filename)

	if err != nil {
		return cw, err
	}

	var c CloudWatchConfig
	_, err = toml.Decode(str, &c)

	if err != nil {
		return cw, err
	}

	if cwConfig.AWSAccessKeyId != "" {
		c.AWSAccessKeyId = cwConfig.AWSAccessKeyId
	}

	if cwConfig.AWSSecretAccessKey != "" {
		c.AWSSecretAccessKey = cwConfig.AWSSecretAccessKey
	}

	if cwConfig.Token != "" {
		c.Token = cwConfig.Token
	}

	if cwConfig.Profile != "" {
		c.Profile = cwConfig.Profile
	}

	if cwConfig.Credentials != "" {
		c.Credentials = cwConfig.Credentials
	}

	if cwConfig.Timezone != "" {
		c.Timezone = cwConfig.Timezone
	}

	sess := session.New()

	var awsConfig *aws.Config
	if c.AWSAccessKeyId != "" && c.AWSSecretAccessKey != "" {
		awsConfig = NewStaticCredentials(c.AWSAccessKeyId, c.AWSSecretAccessKey, c.Token)
	} else {
		awsConfig = NewSharedCredentials(c.Credentials, c.Profile)
	}

	return &CloudWatch{
		Client: cloudwatch.New(sess, awsConfig.WithRegion(c.Region)),
		config: c,
		log:    log,
	}, err
}

func NewSharedCredentials(file, profile string) *aws.Config {
	creds := credentials.NewSharedCredentials(file, profile)

	return &aws.Config{
		Credentials: creds,
	}
}

func NewStaticCredentials(key, secret, token string) *aws.Config {
	creds := credentials.NewStaticCredentials(key, secret, token)

	return &aws.Config{
		Credentials: creds,
	}
}

func NewSession() *session.Session {
	return session.New()
}

func createStatistics(stat string) *string {
	return aws.String(stat)
}

func createDimensions(dimensions []Dimension) []*cloudwatch.Dimension {
	l := len(dimensions)
	d := make([]*cloudwatch.Dimension, 0, l)

	for _, dim := range dimensions {
		d = append(d, &cloudwatch.Dimension{
			Name:  aws.String(dim.Name),
			Value: aws.String(dim.Value),
		})
	}

	return d
}

func (c *CloudWatch) createGetMetricStatisticsInput(msi MetricStatisticsInput) *cloudwatch.GetMetricStatisticsInput {
	input := &cloudwatch.GetMetricStatisticsInput{}

	if msi.Dimensions != nil {
		input.Dimensions = msi.Dimensions
	}

	if msi.Statistics != nil {
		input.Statistics = []*string{msi.Statistics}
	}

	var period int64 = 60

	if msi.Period >= period {
		input.Period = aws.Int64(msi.Period)
		period = msi.Period
	} else {
		input.Period = aws.Int64(period)
	}

	now, err := FixedTimezone(time.Now(), c.config.Timezone)
	if err != nil {
		c.log.Debug(err)
	}

	if !msi.StartTime.IsZero() {
		input.StartTime = aws.Time(msi.StartTime)
	} else {

		input.StartTime = aws.Time(now.Add(time.Duration(-1*(int(period*3))) * time.Second))
	}

	if !msi.EndTime.IsZero() {
		input.EndTime = aws.Time(msi.EndTime)
	} else {
		input.EndTime = aws.Time(now)
	}

	if msi.MetricName != "" {
		input.MetricName = aws.String(msi.MetricName)
	}

	if msi.Namespace != "" {
		input.Namespace = aws.String(msi.Namespace)
	}

	if msi.Unit != "" {
		input.Unit = aws.String(msi.Unit)
	}

	return input
}

func (c *CloudWatch) getMetricStatistics(msi *cloudwatch.GetMetricStatisticsInput) (*cloudwatch.GetMetricStatisticsOutput, error) {
	return c.Client.GetMetricStatistics(msi)
}

func (c *CloudWatch) fetchDatapoint(output *cloudwatch.GetMetricStatisticsOutput, stat string) Datapoint {
	var d Datapoint

	d = c.latestDatapoint(output.Datapoints, stat)

	return d
}

func (c *CloudWatch) latestDatapoint(cwDatapoints []*cloudwatch.Datapoint, stat string) Datapoint {
	if len(cwDatapoints) == 0 {
		now, err := FixedTimezone(time.Now(), c.config.Timezone)
		if err != nil {
			c.log.Debug(err)
		}
		return Datapoint{
			Value:     0,
			Timestamp: now,
		}
	}

	datapoints := make(Datapoints, 0, len(cwDatapoints))
	for _, o := range cwDatapoints {
		var value float64

		switch stat {
		case "Average":
			value = *o.Average
		case "Maximum":
			value = *o.Maximum
		case "Minimum":
			value = *o.Minimum
		case "SampleCount":
			value = *o.SampleCount
		case "Sum":
			value = *o.Sum
		}

		now, err := FixedTimezone(*o.Timestamp, c.config.Timezone)
		if err != nil {
			c.log.Debug(err)
		}
		datapoints = append(datapoints, Datapoint{
			Value:     value,
			Timestamp: now,
		})
	}

	sort.Sort(sort.Reverse(datapoints))

	return datapoints[0]
}

func (c *CloudWatch) FetchMetrics() ([]Metric, error) {
	var err error
	var metrics []Metric

	metrics = make([]Metric, 0, len(c.config.Metrics))

	for _, m := range c.config.Metrics {
		names := m.SplitName()
		for _, n := range names {
			msi := c.createGetMetricStatisticsInput(MetricStatisticsInput{
				Dimensions: createDimensions(m.Dimensions),
				MetricName: n,
				Namespace:  m.Namespace,
				Statistics: createStatistics(m.Statistics),
				Period:     m.Period,
			})

			resp, err := c.getMetricStatistics(msi)

			if err != nil {
				return metrics, err
			}

			datapoint := c.fetchDatapoint(resp, m.Statistics)

			if (Datapoint{}) == datapoint {
				continue
			}

			name := m.CreateName(n)
			value := m.CalcValue(datapoint.Value)

			metrics = append(metrics, Metric{
				Name:  name,
				Time:  datapoint.Timestamp,
				Value: value,
			})
		}
	}

	return metrics, err
}

func (c *CloudWatch) Teardown() {

}
