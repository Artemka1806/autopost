import time
import requests
import feedparser
from bs4 import BeautifulSoup
from PIL import Image, ImageFilter
import io
from io import BytesIO
import os
import base64
import re
import aiokafka
from aiokafka import AIOKafkaProducer
import asyncio
import json


config = {}

def load_data():
	try:
		with open("../config.json") as config_file:
			config = json.load(config_file)
			return config
	except FileNotFoundError:
		raise FileNotFoundError("Файл не знайдено. Будь ласка, переконайтеся, що файл 'config.json' існує.")
	except json.JSONDecodeError:
		raise json.JSONDecodeError("Помилка декодування JSON. Будь ласка, переконайтеся, що файл 'config.json' містить коректний JSON.")


config = load_data()


async def send_to_broker(theme, url, header, text, image, date):
	producer = AIOKafkaProducer(bootstrap_servers=config["KAFKA_HOST"])
	await producer.start()
	try:
		await producer.send(theme,  bytes(str({"url": url, "header": header, "text": text, "image": image, "date": date}), "utf-8"))
	finally:
		await producer.stop()


print("AUTOPOST")

while True:
	try:
		feed = feedparser.parse("https://lyceum.ztu.edu.ua/feed/")

		soup = BeautifulSoup(requests.get("https://lyceum.ztu.edu.ua/").text, 'html.parser')

		filteredNews = {}

		allNews = soup.findAll('a', class_='thumbnail-link')

		for data in allNews:
			filteredNews[data["href"]] = data.find('img')["src"]

		entries = feed.entries

		with open("last_posted.txt", "r") as last_posted:
			last_posted_data = last_posted.read()

		data = entries[0].content[0]["value"].split('<div class="pvc_clear"></div>')[2]
		#print(data)
		f = BeautifulSoup(data, "html.parser")
		try:
			external = f.find("div")
			external.extract()
		except AttributeError:
			pass

		#print("\n-------------------------------------------------------\n")
		# p_tags = f.find_all('p')
		# clean_text = ''
		# for p in p_tags:
		# 	clean_text+=p.get_text()+"\n"

		ready_text = ""
		for item in f.find_all(["p", "li"]):
			
			if item.name != "strong" and item.name != "br":
				ready_text += str(item).replace("<strong>", "").replace("</strong>", "").replace("<br />", "\n").replace("<br/>", "\n").replace("<p>", "").replace("</p>", "\n").replace("<ul>", "").replace("</ul>", "\n").replace("<li>", " - ").replace("</li>", "\n")
				ready_text = re.sub(r"<[^>]+>", "", ready_text, flags=re.S)

		if entries[0].link != last_posted_data:
			if entries[0].link in filteredNews.keys():

				print("Entry Title:", entries[0].title)
				print("Entry Date:", entries[0].published)
				print("Entry Text: ", ready_text)
				print("Entry Link:", entries[0].link)
				print("Entry Image:", filteredNews[entries[0].link])
				print("\n")

				
				asyncio.run(send_to_broker("e", entries[0].link, entries[0].title, ready_text, filteredNews[entries[0].link], entries[0].published))
				with open("last_posted.txt", "w") as last_posted:
					last_posted.write(entries[0].link)


	except Exception as e:
		print("ERROR: ", e)

	time.sleep(60)
