import praw
import json
from praw.models import Redditor
from praw.models import Subreddit
from datetime import datetime
from kafka import KafkaProducer
import time


Data_Path = "/mnt/hgfs/OneDrive/Documents/2015.12-2017.06 NUS/Big Data Engineering/data"


class RedditClient:
    CLIENT_ID = "Ba1ko5k74vJeow"
    CLIENT_SECRET = "8162uBJkcz2R6LNg4DFh0DSO-FI"
    kafka_server = "7.11.230.242:9092"

    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_server)

    def get_reddit_client(self):
        self.reddit = praw.Reddit(client_id=self.CLIENT_ID, client_secret=self.CLIENT_SECRET,
                                  user_agent = "big_data_demo", password="test123456", username="lzc_test")
        return self.reddit

    def retrieve_subreddit(self, topic):
        reddit = self.get_reddit_client()
        subreddit = reddit.subreddit(topic)
        current = (int)(datetime.utcnow().timestamp()).__str__()
        count = 0;
        while True:
            print('current: ' + current)
            print('count: ' + count.__str__())
            timestamp = 'timestamp:2764800..' + current
            for submission in subreddit.search(query=timestamp, sort='new', syntax='cloudsearch'):
                count += 1
                current = (int)(submission.created_utc).__str__()
                print("sending submission...")
                self.producer.send('submissions', ComplexEncoder(indent=4).encode(submission.__dict__).encode())
                print("submission sent.")
                time.sleep(2)
                for comment in submission.comments.list():
                    print("sending comment...")
                    self.producer.send('comments', ComplexEncoder().encode(comment.__dict__).encode())
                    print("comment sent.")
                    time.sleep(2)

class ComplexEncoder(json.JSONEncoder):

    def default(self, obj):
        try:
            if isinstance(obj, Redditor):
                return obj.name
            elif isinstance(obj, Subreddit):
                return obj.display_name
            return json.JSONEncoder.default(self, obj)
        except TypeError:
            pass

if __name__ == "__main__":
    reddit_factory = RedditClient()
    reddit = reddit_factory.get_reddit_client()
    print(reddit.user.me())
    reddit_factory.retrieve_subreddit("learnpython")
    # last = 0
    # for submission in reddit.subreddit("/samsung").hot(limit=1000):
    #     last = submission.fullname
    #     # os.mkdir(Data_Path + '/' + submission.id, 0o777);
    #     # f = open(Data_Path + '/' + submission.id + '/submission-' + submission.id, 'w')
    #     # f.write(ComplexEncoder(indent=4).encode(submission.__dict__))
    #     # submission.comments.replace_more(limit=0)
    #     # for comment in submission.comments.list():
    #     #     f = open(Data_Path + '/' + submission.id + '/comment-' + submission.id + '-' + comment.id, 'w')
    #     #     f.write(ComplexEncoder(indent=4).encode(comment.__dict__))
    print("ok")
