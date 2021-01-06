# Script to download missed media from TWARC crawls.
# Code adapted from fetch_media.py, crawl_feeds.py, & bhl_twarc.py
# Works without using twitter creds.

import argparse
import logging
from datetime import datetime
import csv
import os
from os.path import join
import requests
import time
import urllib.request
import urllib.parse


private_tweets = []
dead_tweets = []
stale_tweets = []
live_tweets = []

private_profiles = []
dead_profiles = []
stale_profiles = []
live_profiles = []


def fetch_media_for_feed(feed_dict):
    start = datetime.now()

    # Loop through folders
    for feeds in feed_dict:
        short_name = feeds
        feeds = join(feed_dir, feeds)
        media_dir = join(feeds, 'media')
        image_dir = join(media_dir, 'tweet_images')
        profile_images_dir = join(media_dir, 'profile_images')

    # Log creation
        logs_dir = join(media_dir, 'media_logs')
        log_file = join(logs_dir, 'twarc.log')

    # Loop through each folder and make directories
        for directory in [feed_dir, media_dir, logs_dir]:
            if not os.path.exists(image_dir):
                os.makedirs(image_dir)
                print("Creating tweet image directory for " + image_dir)
            if not os.path.exists(profile_images_dir):
                os.makedirs(profile_images_dir)
                print("Creating profile image directory for " + profile_images_dir)
            if not os.path.exists(logs_dir):
                os.makedirs(logs_dir)
                print("Creating logs directory for " + logs_dir)

    # LOGGING formatting
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        logger = logging.getLogger(short_name)
        handler = logging.FileHandler(log_file)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    # Setting up CSV dictionaries
        media_urls_csv = join(media_dir, 'tweet_images.csv')
        profile_image_csv = join(media_dir, 'profile_images.csv')

        media_urls = {}
        profile_image_urls = {}

        logger.info("Starting media downloads for %s", short_name)

    # Reading tweet_images.csv
        with open(media_urls_csv, 'r', newline="") as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                url = row[0]
                filename = row[1]
                media_urls[url] = filename

    # Reading profile_images.csv
        with open(profile_image_csv, 'r', newline="") as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                url = row[0]
                profile_dir = row[1]
                filename = row[2]
                profile_image_urls[url] = {'profile_dir': profile_dir, 'filename': filename}

    # Download Tweet Media
        with requests.Session() as s:
            for url in media_urls:
                try:
                   status_code = urllib.request.urlopen(url)
                except urllib.error.HTTPError as exception:
                    if exception.getcode() !=200:
                        if exception.getcode() ==403:
                            print(exception, url)
                            logger.info("Private tweet media: {0}".format(url))
                            private_tweets.append(url)
                        elif exception.getcode() ==404:
                            print(exception, url)
                            logger.info("Dead tweet media: {0}".format(url))
                            dead_tweets.append(url)
                        else:
                            logger.info("Tweet Media {0}: {1}".format(execption,url))
                            dead_tweets.append(url)

            # Check if the image has already been downloaded
                if media_urls[url] in os.listdir(image_dir):
                    logger.info("{0} Tweet media has already been fetched: {1}".format(media_urls[url], url))
                    stale_tweets.append(url)
                    # continue statement?

            # If the image hasn't already been downloaded
                else:
                    logger.info("{0} Fetching tweet media: {1}".format(short_name, url))
                    media = s.get(url)
                    media_file = join(image_dir, media_urls[url])
                    with open(media_file, 'wb') as media_out:
                        media_out.write(media.content)
                        print("Fetching Tweet Media: {0}".format(media_urls[url]))
                    time.sleep(1)
                    live_tweets.append(url)

    # Downloading Profile Images
        with requests.Session() as s:
            for url in profile_image_urls:
                profile_dir_name = profile_image_urls[url]['profile_dir']
                filename = profile_image_urls[url]['filename']
                profile_dir = join(profile_images_dir, profile_dir_name)

            # Check if URL is live
                try:
                    response = urllib.request.urlopen(url)
                except urllib.error.HTTPError as exception:
                    if exception.getcode() !=200:
                        if exception.getcode() ==403:
                            print(exception, url)
                            logger.info("Private profile media: {0}".format(url))
                            private_profiles.append(url)
                        elif exception.getcode() ==404:
                            print(exception, url)
                            logger.info("Dead profile media: {0}".format(url))
                            dead_profiles.append(url)
                        else:
                            logger.info("Profile media {0}: {1}".format(exception,url))
                            dead_profiles.append(url)

            # Check if ID folder exists
                if not os.path.exists(profile_dir):
                    os.makedirs(profile_dir)

            # Check if image exists in ID folder
                if filename not in os.listdir(profile_dir):
                    logger.info("{0} Fetching profile media: {1}".format(short_name, url))
                    profile_image = s.get(url)
                    profile_image_file = join(profile_dir, filename)
                    with open(profile_image_file, 'wb') as profile_image_out:
                        profile_image_out.write(profile_image.content)
                    time.sleep(1)
                    print("Fetching Profile Media: {0}".format(filename))
                    live_profiles.append(filename)
                else:
                    stale_profiles.append(filename)
    end = datetime.now()
    print("\nTotal Download Time:", end-start)

    return

fetch_media_for_feed(feed_dict)

print("\nPrivate Tweet Images:", len(private_tweets))
print("Dead Tweet Images:", len(dead_tweets))
print("Previously Downloadeded Tweet Images:", len(stale_tweets))
print("Newly Downloaded Tweet Images:", len(live_tweets))

print("\nPrivate Profile Images:", len(private_profiles))
print("Dead Profile Images:", len(dead_profiles))
print("Previously Downloadeded Profile Images:", len(stale_profiles))
print("Newly Downloaded Profile Images:", len(live_profiles))
now = datetime.now()
print("\nTwitter image downloads finished at:", now.strftime("%Y-%m-%d %I:%M:%S"))
