from aiokafka import AIOKafkaConsumer
import asyncio

import json
import requests

from PIL import Image, ImageFilter
import io
from io import BytesIO
import os
import base64

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


class Post:
	"""Пост для публікації в соц. мережі (Instagram та/або Facebook)"""
	def __init__(self, image, text):
		self.image = image
		self.text = text
	

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
		"""Робить зображення квадратним та заміняє ним атрибут image. Нічого не повертає"""
		foreground = Image.open(BytesIO(self.image))
		foreground = foreground.crop((foreground.width / 2, 0, foreground.width, foreground.height))
		foreground = foreground.resize((1000, 1000))

		foreground_blurred = foreground
		for _ in range(6):
			foreground_blurred = foreground_blurred.filter(ImageFilter.BLUR)
		foreground_blurred.putalpha(128)

		img = Image.open(BytesIO(self.image))
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

		self.image = ready_bytes.getvalue()



	def publish_to_instagram(self, image_url):
		"""Публікація в Instagram. Повертає URI поста"""
		payload = {
			"image_url":  image_url, 
			"access_token": config["FACEBOOK_TOKEN"]
		}

		if self.text != "None":
			payload["caption"]= self.text

		r = requests.post(IG_UPLOAD_URL, data=payload)
		post_id = r.json()["id"]
		r_publish = requests.post(IG_POST_URL, params={"creation_id": post_id, "access_token": config["FACEBOOK_TOKEN"]})

		payload = {
			"fields": "permalink", 
			"access_token": config["FACEBOOK_TOKEN"]
		}

		r = requests.get(f"https://graph.facebook.com/v19.0/{r_publish.json()['id']}", params=payload)
		return r.json()["permalink"]


	def publish_to_facebook(self, image_url):
		"""Публікація в Facebook. Повертає URI поста"""
		payload = {
			"url": image_url, 
			"access_token": config["FACEBOOK_TOKEN"]
		}

		if self.text != "None":
			payload["message"]= self.text
		
		r = requests.post(FB_POST_URL, params=payload)
		payload = {
			"fields": "permalink_url",
			"access_token": config["FACEBOOK_TOKEN"]
		}
		r = requests.get(f"https://graph.facebook.com/v19.0/{r.json()['post_id']}", params=payload)
		return r.json()["permalink_url"]
			


async def consume():
	consumer = AIOKafkaConsumer(
		'ig', 'fb', "e",
		bootstrap_servers=config["KAFKA_HOST"],
		group_id="main")
	await consumer.start()
	try:
		async for msg in consumer:
			data = eval(msg.value)
			p = Post(data["image"], data['text'])
			if msg.topic == "ig":
				p.square_image()
				image_url = p.upload_image_to_server()
				p.publish_to_instagram(image_url)
			elif msg.topic == "fb":
				image_url = p.upload_image_to_server()
				p.publish_to_facebook(image_url)
			elif msg.topic == "e":
				image_url = p.upload_image_to_server()
				p.publish_to_facebook(image_url)

				p.square_image()
				image_url = p.upload_image_to_server()
				p.publish_to_instagram(image_url)
	finally:
		await consumer.stop()

if __name__ == "__main__":
	while True:
		asyncio.run(consume())