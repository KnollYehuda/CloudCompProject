import requests
import twitter
import json
import boto3
from Configuration import Configuration


class TwitterListener:
    def __init__(self):
        self.api = twitter.Api(consumer_key=Configuration.TWITTER_CONSUMER_KEY,
                               consumer_secret=Configuration.TWITTER_CONSUMER_SECRET,
                               access_token_key=Configuration.TWITTER_ACCESS_TOKEN_KEY,
                               access_token_secret=Configuration.TWITTER_ACCESS_TOKEN_SECRET)
        self.cloudwatch = boto3.client('cloudwatch', region_name=Configuration.AWS_REGION)
        self.results = []
        self.track = ''

    def get_tweets_by_track(self, search_word):
        self.results = self.api.GetSearch(search_word)
        self.track = search_word

    def send_tweets_to_sqs(self):
        sqs = boto3.client('sqs', region_name=Configuration.AWS_REGION)
        queue_url = Configuration.QUEUE_URL

        tweets = [i.AsDict() for i in self.results]
        tweets_only_urls = filter(lambda x: len(x['urls']) > 0, tweets)

        for tweet in tweets_only_urls:
            if 'urls' in tweet:
                tweet_expanded_url = tweet['urls'][0]['expanded_url']
                response = sqs.send_message(
                    QueueUrl=queue_url,
                    DelaySeconds=10,
                    MessageAttributes={
                        'url': {
                            'DataType': 'String',
                            'StringValue': tweet_expanded_url
                        },
                        'track': {
                            'DataType': 'String',
                            'StringValue': self.track
                        }
                    },
                    MessageBody=(
                        'New Tweet'
                    )
                )

                print(response['MessageId'])

        instance_id = requests.get('http://169.254.169.254/latest/meta-data/instance-id').content

        self.cloudwatch.put_metric_data(
            MetricData=[
                {
                    'MetricName': 'Track_Monitoring',
                    'Dimensions': [
                        {
                            'Name': 'InstanceId',
                            'Value': instance_id
                        },
                        {
                            'Name': 'APP_VERSION',
                            'Value': '1.0'
                        },
                    ],
                    'Unit': 'Count',
                    'Value': len(tweets_only_urls)
                },
            ],
            Namespace='EC2_TwitterListener'
        )


if __name__ == '__main__':
    twitter_handler = TwitterListener()
    track = raw_input("Enter your search word:\n")
    try:
        while True:
            twitter_handler.get_tweets_by_track(track)
            twitter_handler.send_tweets_to_sqs()
    except KeyboardInterrupt as kiex:
        print('User exited')

    except Exception as tex:
        print(tex)
