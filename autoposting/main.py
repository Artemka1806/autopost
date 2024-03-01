from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncio

import json
import requests

from PIL import Image, ImageFilter
import io
from io import BytesIO
import os
import base64

import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

FB_POST_URL = "https://graph.facebook.com/v19.0/229959926866540/photos"
IG_UPLOAD_URL = "https://graph.facebook.com/v19.0/17841463072998515/media"
IG_POST_URL = "https://graph.facebook.com/v19.0/17841463072998515/media_publish"


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


mongo_client = MongoClient(config["MONGODB_URI"])
db = mongo_client["db"]
posts = db["posts"]
mass_groups = db["mass_groups"]



class Post:
	"""Пост для публікації в соц. мережі (Instagram та/або Facebook)"""
	def __init__(self, url, header, text, image, date):
		self.url = url
		self.header = header
		self.text = text
		self.image = image
		self.date = date


	def __str__(self):
		return self.text


	def upload_image_to_server(self):
		"""Загрузка картинки поста на сервер IMGBB. Повертає URL загруженого зображення"""
		b64_image = base64.b64encode(self.image).decode('utf-8')
		request_data = {
			"key": config["IMGBB_TOKEN"],
			"image":  b64_image,
			"name": "img.png",
			"expiration": 120
		}
		r = requests.post("https://api.imgbb.com/1/upload", data=request_data)
		if r.status_code == 200:
			return r.json()["data"]["url"]
		else:
			raise Exception(r.json()["error"]["message"])


	def square_image(self):
		"""Робить зображення квадратним. Повертає байти"""
		response = requests.get(self.image)
		foreground = Image.open(BytesIO(response.content))
		foreground = foreground.crop((foreground.width / 2, 0, foreground.width, foreground.height))
		foreground = foreground.resize((1000, 1000))

		foreground_blurred = foreground
		for _ in range(6):
			foreground_blurred = foreground_blurred.filter(ImageFilter.BLUR)
		foreground_blurred.putalpha(128)

		response = requests.get(self.image)
		img = Image.open(BytesIO(response.content))
		background = Image.new("RGB", (800, 800), "black")
		background.paste(foreground_blurred, (0, 0), foreground_blurred)

		if img.height<500:
			base_width = 800
			wpercent = base_width / float(img.size[0])
			hsize = int((float(img.size[1]) * float(wpercent)))
			img = img.resize((base_width, hsize), Image.Resampling.LANCZOS)
			x = 0
			y = round((800 - img.height)/2)
			background.paste(img, (x, y))
		else:
			base_height = 800
			hpercent = base_height / float(img.size[1])
			wsize = int((float(img.size[0]) * float(hpercent)))
			img = img.resize((wsize, base_height), Image.Resampling.LANCZOS)
			x = round((800 - img.width) / 2)
			y = 0
			background.paste(img, (x, y))

		ready_bytes = io.BytesIO()
		background.save(ready_bytes, format="JPEG")

		return ready_bytes.getvalue()


	def publish_to_instagram(self):
		"""Публікація в Instagram. Повертає URI поста"""

		sq = Post(self.url, self.header, self.text, self.square_image(), self.date)
		image_url = sq.upload_image_to_server()




		payload = {
			"image_url":  image_url, 
			"access_token": config["FACEBOOK_TOKEN"]
		}

		if self.text != "None":
			payload["caption"]= f"{self.header}\n\n{self.text}"

		r = requests.post(IG_UPLOAD_URL, data=payload)
		post_id = r.json()["id"]
		r_publish = requests.post(IG_POST_URL, params={"creation_id": post_id, "access_token": config["FACEBOOK_TOKEN"]})

		payload = {
			"fields": "permalink", 
			"access_token": config["FACEBOOK_TOKEN"]
		}

		r = requests.get(f"https://graph.facebook.com/v19.0/{r_publish.json()['id']}", params=payload)
		return r.json()["permalink"]


	def publish_to_facebook(self):
		"""Публікація в Facebook. Повертає URI поста"""
		#image_url = self.upload_image_to_server()

		payload = {
			"url": self.image, 
			"access_token": config["FACEBOOK_TOKEN"]
		}

		if self.text != "None":
			payload["message"]= f"{self.header}\n\n{self.url}"
		
		r = requests.post(FB_POST_URL, params=payload)
		payload = {
			"fields": "permalink_url",
			"access_token": config["FACEBOOK_TOKEN"]
		}
		r = requests.get(f"https://graph.facebook.com/v19.0/{r.json()['post_id']}", params=payload)
		return r.json()["permalink_url"]
			

async def send_to_broker(theme, message):
	producer = AIOKafkaProducer(bootstrap_servers=config["KAFKA_HOST"])
	await producer.start()
	try:
		await producer.send(theme, message)
	finally:
		await producer.stop()


async def consume():
	consumer = AIOKafkaConsumer(
		'ig', 'fb', "e",
		bootstrap_servers=config["KAFKA_HOST"],
		group_id="main")
	await consumer.start()

	try:
		async for msg in consumer:
			data = eval(msg.value)
			p = Post(data["url"], data["header"], data["text"], data["image"], data["date"])
			print(p)
			if msg.topic == "ig":
				url = p.publish_to_instagram()
				#await send_to_broker("tg_ig", p.image, p.text, url)

			elif msg.topic == "fb":
				url = p.publish_to_facebook()
				#await send_to_broker("tg_fb", p.image, p.text, url)

			elif msg.topic == "e":
				fb_url = p.publish_to_facebook()
				ig_url = p.publish_to_instagram()


				mongo_client.db.posts.find_one_and_update({"_id":  p.url}, {"$set": 
					{"_id": p.url, "url": p.url, "header": p.header, "text": p.text, "image": p.image, "date": p.date, "facebook_url": fb_url, "instagram_url": ig_url}}, upsert=True
				)

				#await send_to_broker("tg", bytes(p.url))

				groups = mongo_client.db.mass_groups.find({})

				for i in groups:
					r = requests.get(f"https://api.telegram.org/bot{config['TELEGRAM_BOT_TOKEN']}/sendPhoto", params={"chat_id": i["_id"], "photo": p.image, "caption": f'<b>{p.header}</b>\n\n<a href="{p.url}">🌐 Новина на сайті ліцею</a>\n\n<a href="{ig_url}">📸 Пост на Instagram</a>\n\n<a href="{fb_url}">🔵 Публікація на Facebook</a>', "parse_mode": "HTML"})

				print(r.text)
	finally:
		await consumer.stop()


if __name__ == "__main__":
	while True:
		asyncio.run(consume())