import datetime
import json
import praw
import boto3


from conf import conf_reddit, conf_aws

if __name__ == "__main__":

    ## Create Reddit client
    reddit = praw.Reddit(**conf_reddit)

    ## Selecte subreddit
    subreddit = reddit.subreddit('worldnews')

    ## Hack to only send a submission one time
    processed_submissions = {}

    ## Get Kinesis client
    kinesis = boto3.client("kinesis", 
        region_name = conf_aws["aws_region"], 
        endpoint_url=conf_aws["kinesis_endpoint"])

    ## Initialize stream producer
    for comment in subreddit.stream.comments():
            try:
                submission_id = comment.link_id.split("t3_")[1]
                try:
                    processed_submissions[submission_id] += 1
                except:
                    submission = comment.submission
                    submission_pkt = {
                        "title": submission.title,
                        "id": submission.id,
                        "date": datetime.datetime.fromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                        "type":"submission"
                    }

                    ## Put Record on Kinesis
                    kinesis.put_record(StreamName=conf_aws["stream_name"],
                                       Data=json.dumps(submission_pkt),
                                       PartitionKey = "partitionKey")
                    
                    processed_submissions[submission_pkt['id']] = 1
                comment_pkt = {
                    "author": comment.author.name,
                    "date": datetime.datetime.fromtimestamp(comment.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                    "id": comment.id,
                    "text": comment.body,
                    "submission_id": comment.link_id.split("t3_")[1],
                    "type":"comment"
                }

                ## Put Record on Kinesis
                kinesis.put_record(StreamName=conf_aws["stream_name"],
                                   Data=json.dumps(comment_pkt),
                                   PartitionKey = "partitionKey")
    
            except Exception as e:
                print(e)
                pass