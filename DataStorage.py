from datetime import date

import psycopg2
from psycopg2.extras import RealDictCursor
import json
from Configuration import Configuration


class ExtractedLink:
    def __init__(self, url, content, title, description, screenshot_url):
        self.url = url
        self.content = content
        self.title = title
        self.description = description
        self.screenshot_url = screenshot_url


class DataStorage:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(user=Configuration.DB_USER,
                                               password=Configuration.DB_PASSWORD,
                                               host=Configuration.DB_HOST,
                                               port=Configuration.DB_PORT,
                                               database=Configuration.DB_DATABASE)
            cursor = self.connection.cursor()
            # Print PostgreSQL Connection properties
            # print(self.connection.get_dsn_parameters(), "\n")
            # Print PostgreSQL version
            # cursor.execute("SELECT version();")
            # record = cursor.fetchone()
            # print("You are connected to - ", record, "\n")
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

    def create_table(self):
        try:
            cur = self.connection.cursor()

            print(cur)
            # cur.execute("select * from information_schema.tables where table_name = 'tweets_info'")
            # tweets_table_name = cur.fetchone()[0]
            # print(tweets_table_name + ':: EXISTS. Code will not create a new table.')
            # cur.execute("CREATE TABLE tweets_info (PRIMARYID serial PRIMARY KEY, LINK varchar, TWEETID bigint, TITLE varchar, CONTENT varchar, TIMESTAMP date, SCREENSHOT varchar, TRACK varchar);")
            # if not tweets_table_name:
            self.connection.commit()
            cur.close()
            print('TABLE CREATED')

        except Exception as ex:
            print(ex)
            cur.close()

    def insert_tweet(self, tweet_obj):
        try:
            print('entered insert tweet')
            cur = self.connection.cursor()
            # print('Inserting a new tweet...')
            # print(tweet_obj)

            query = 'INSERT INTO public.tweets_info (link, track, timestamp, content, title, description, screenshot_url) VALUES {}'.format(tweet_obj)

            insert_query = query
            cur.execute(insert_query)
            self.connection.commit()
            # print('Added new tweet to DB')
            cur.close()
        except Exception as ex:
            # print('Caught Exception: {}'.format(ex))
            print('')
    def search_db(self, track=None):
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)

        if len(track) > 0:
            cursor.execute('SELECT * FROM public.tweets_info WHERE track=\'{}\''.format(track))
        else:
            cursor.execute('SELECT * FROM public.tweets_info')

        results = cursor.fetchall()

        for result in results:
            if not isinstance(result['timestamp'], str):
                result['timestamp'] = result['timestamp'].strftime("%Y-%m-%dT%H:%M:%SZ")

        cursor.close()
        return results

    def get_tweets_info_count(self):
        cur = self.connection.cursor()
        query = 'select count(*) count from public.tweets_info'
        cur.execute(query)
        results = cur.fetchone()
        self.connection.commit()
        cur.close()
        try:
            return results[0]

        except Exception:
            print('Any problem with count method')
            return 0

    def delete_tweets_bulk(self, count):
        cur = self.connection.cursor()
        query = 'DELETE FROM public.tweets_info WHERE ctid IN (SELECT ctid FROM public.tweets_info ORDER BY timestamp LIMIT {})'.format(count)
        cur.execute(query)
        self.connection.commit()
        cur.close()




# ds = DataStorage()
# # ds.create_table()
# info = ExtractedLink('www.google.com', 'new content', 'new title', 'new description', 'www.screenshot.com')
# ds.insert_tweet(info, 780, 'new track')
