from DataStorage import DataStorage
import boto3
import imgkit
import hashlib
from datetime import datetime
import os
from bs4 import BeautifulSoup
import requests
from Configuration import Configuration


class Tweet:
    def __init__(self):
        self.link = ''
        self.track = ''
        self.timestamp = ''
        self.content = ''
        self.title = ''
        self.description = ''
        self.screenshot_url = ''

    def get_tweet_as_tuple(self):
        result_tuple = (self.link, self.track, self.timestamp, self.content, self.title, self.description, self.screenshot_url)
        # print(result_tuple)
        return result_tuple


class LinkListener:
    def __init__(self):
        self.S3_BUCKET = Configuration.S3_BUCKET
        self.db_handler = DataStorage()
        self.sqs = boto3.client('sqs', region_name=Configuration.AWS_REGION)
        self.s3 = boto3.client('s3', region_name=Configuration.AWS_REGION)
        self.cloudwatch = boto3.client('cloudwatch', region_name=Configuration.AWS_REGION)
        self.messages = []
        self.entries = []
        self.tweets = []
        self.new_tweet = Tweet()

    def get_messages_from_queue(self):
        try:
            response = self.sqs.receive_message(
                QueueUrl=Configuration.QUEUE_URL,
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=10
            )
            print('Received messages')
            self.messages.extend(response['Messages'])

            entries_to_delete = [
                {'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']}
                for msg in response['Messages']
            ]

            response_deletion = self.sqs.delete_message_batch(QueueUrl=Configuration.QUEUE_URL, Entries=entries_to_delete)

            if len(response_deletion['Successful']) != len(entries_to_delete):
                print("Failed to delete messages")

        except KeyError:
            print('No More Messages In Queue')

        if len(self.messages) > 0:
            self.entries = [{'url': msg['MessageAttributes']['url']['StringValue'],
                             'track': msg['MessageAttributes']['track']['StringValue']} for msg in self.messages]

        del self.messages[:]

    def parsing_html(self, url):
        try:
            time_before_search = datetime.now()
            req = requests.get(url, headers={'User-Agent': 'CloudComputingProject'})
            soup = BeautifulSoup(req.text, "html.parser")

            # Get title
            self.new_tweet.title = soup.find("title").get_text().encode("utf-8").replace('\'', '').replace('\n', ' ')

            # Get meta description
            for tag in soup.find_all("meta"):
                if tag.get("property") == "og:description":
                    self.new_tweet.description = tag.get("content").encode("utf-8").replace('\'', '').replace('\n', ' ')
                    break

            # Get content
            self.new_tweet.content = soup.find('body').get_text().encode("utf-8").replace('\'', '').replace('\n', ' ')

            time_after_search = datetime.now()

            actual_time = (time_after_search - time_before_search).seconds

            instance_id = requests.get(Configuration.INSTANCE_ID_URL).content

            self.cloudwatch.put_metric_data(
                MetricData=[
                    {
                        'MetricName': 'ScrapingTime',
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
                        'Unit': 'None',
                        'Value': actual_time
                    },
                ],
                Namespace='EC2_LinkListener'
            )

            print('Done Parsing')

        except Exception as ex:
            print('Caught any error in :: Parsing process... Skipping')

    def screenshot_to_s3_bucket(self, url):
        time_before_search = datetime.now()

        # take ps from url to local file
        local_ps_filename = '{}.jpg'.format(self.generate_tweet_id(url))

        try:
            options = {
                'encoding': "utf-8",
                'width': '2000',
                'height': '2000',
                'load-error-handling': 'skip',
                'quiet': ''
            }
            imgkit.config(wkhtmltoimage='/usr/bin/wkhtmltoimage')
            imgkit.from_url(url, local_ps_filename, options=options)

        except Exception:
            pass

        try:
            self.s3.upload_file(local_ps_filename, self.S3_BUCKET, local_ps_filename)
            self.new_tweet.screenshot_url = 's3://{}/{}'.format(self.S3_BUCKET, local_ps_filename)
        except Exception as ex:
            print('Error Upload to S3 - {}'.format(ex))

        time_after_search = datetime.now()
        actual_time = (time_after_search - time_before_search).seconds
        print(actual_time)

        instance_id = requests.get(Configuration.INSTANCE_ID_URL).content

        self.cloudwatch.put_metric_data(
                MetricData=[
                    {
                        'MetricName': 'ScreenshotTime',
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
                        'Unit': 'None',
                        'Value': actual_time
                    },
                ],
                Namespace='EC2_LinkListener'
            )

        # delete local file
        if os.path.exists(local_ps_filename):
            os.remove(local_ps_filename)

        print('Done S3ing')

    def upload_tweet_to_db(self, tweet_tuple):
        try:
            # check if >1000 tweets in db
            print('entered upload_tweet_to_db')
            count = self.db_handler.get_tweets_info_count()
            if int(count) >= 1000:
                print('count is {}'.format(count))
                self.db_handler.delete_tweets_bulk(1)

            # upload new tweet
            self.db_handler.insert_tweet(tweet_tuple)

        except Exception:
            print('Caught any error in :: Uploading to DB process... Skipping')

    def arrange_tuple_to_db(self, tweet_to_upload):
        try:
            self.new_tweet.link = tweet_to_upload['url']
            # print('Link: {}'.format(self.new_tweet.link))

            self.new_tweet.track = tweet_to_upload['track']
            # print('Track: {}'.format(self.new_tweet.track))

            self.parsing_html(tweet_to_upload['url'])
            # print('Title: {}'.format(self.new_tweet.title))
            # print('Description: {}'.format(self.new_tweet.description))
            # print('Content: {}'.format(self.new_tweet.content))

            self.screenshot_to_s3_bucket(tweet_to_upload['url'])
            # print('Screenshot URL: {}'.format(self.new_tweet.screenshot_url))

            self.new_tweet.timestamp = datetime.now().strftime("%Y-%m-%mT%H:%M:%SZ")
            # print('Timestamp: {}'.format(self.new_tweet.timestamp))

            return self.new_tweet.get_tweet_as_tuple()

        except Exception as ex:
            print(ex)
            return

    @staticmethod
    def generate_tweet_id(entry_url):
        try:
            now_str = str(datetime.now()).encode('utf-8')
            tweet_url = entry_url.encode('utf-8')
            joined = str(now_str + tweet_url).encode('utf-8')
            hashed_tweet_id = hashlib.sha1(joined)
            hex_dig = hashed_tweet_id.hexdigest()
            return hex_dig
        except Exception:
            print('Caught any error in :: Generating new TweetID process... Skipping')


if __name__ == '__main__':
    llis = LinkListener()

    while True:
        llis.get_messages_from_queue()
        print('========================== ENTRIES: ====================')
        print(len(llis.entries))
        print('========================== ENTRIES: ====================')
        for tweet in llis.entries:
            print('**********************')
            print('Working on: {}'.format(tweet))
            tweet = llis.arrange_tuple_to_db(tweet)
            llis.upload_tweet_to_db(tweet)
            print('**********************')
        del llis.entries[:]
