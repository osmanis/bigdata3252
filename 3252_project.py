# AWS and Twitter keys have been removed

!pip install boto3

!pip install tweepy

import os
import json

import boto3
import tweepy

consumer_key = 'KEY'
consumer_secret = 'KEY'

access_token = 'KEY'
access_token_secret = 'KEY'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

kinesis_client = boto3.client('kinesis', region_name='us-east-1', aws_access_key_id='KEY', aws_secret_access_key='KEY')

class KinesisStreamProducer(tweepy.StreamListener):

	def __init__(self, kinesis_client):
		self.kinesis_client = kinesis_client

	def on_data(self, data):
		tweet = json.loads(data)
		self.kinesis_client.put_record(StreamName='ingest-3252', Data=tweet["text"], PartitionKey="key")
		#print("Publishing record to the stream: ", tweet)
		return True

	def on_error(self, status):
		print("Error: " + str(status))

def main():
	mylistener = KinesisStreamProducer(kinesis_client)
	myStream = tweepy.Stream(auth = auth, listener = mylistener)
	myStream.filter(track=['#trump'])

if __name__ == "__main__":
	main()