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

data = {}

def load_data():
	try:
		with open("../config.json") as config_file:
			data = json.load(config_file)
			return data
	except FileNotFoundError:
		print("Файл не знайдено. Будь ласка, переконайтеся, що файл 'config.json' існує.")
	except json.JSONDecodeError:
		print("Помилка декодування JSON. Будь ласка, переконайтеся, що файл 'config.json' містить коректний JSON.")
	except Exception as e:
		print(f"Виникла помилка: {e}")


data = load_data()




def blur(b_data):
	foreground = Image.open(BytesIO(b_data))
	foreground = foreground.crop(
	(foreground.width / 2, 0, foreground.width, foreground.height))  # обрізання зображення, залишаємо праву половину

	# Збільшення до розміру 1000x1000 пікселів
	foreground = foreground.resize((1000, 1000))

	# Заблюрювання зображення з більшим рівнем розмиття
	foreground_blurred = foreground
	for _ in range(5):
		foreground_blurred = foreground_blurred.filter(ImageFilter.BLUR)

	# Встановлення прозорості для зображення
	alpha = 128  # Значення прозорості (0 - повністю прозорий, 255 - повністю непрозорий)
	foreground_blurred.putalpha(alpha)

	# Відкриття початкового зображення
	img = Image.open(BytesIO(b_data))
	# Створення пустої картинки
	background = Image.new("RGB", (800, 800), "black")

	# Вставка заблюреного зображення на пусту картинку
	background.paste(foreground_blurred, (0, 0), foreground_blurred)

	# Вставка початкового зображення поверх заблюреного
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
	img_byte_arr = io.BytesIO()

	#output_filename = url[len("https://lyceum.ztu.edu.ua/wp-content/uploads/"):].replace("/", "_")

	background.save(img_byte_arr, format="JPEG")
	return img_byte_arr.getvalue()


async def consume():
	consumer = AIOKafkaConsumer(
		'ig', 'fb', "e",
		bootstrap_servers=data["KAFKA_HOST"],
		group_id="main")
	# Get cluster layout and join group `my-group`
	await consumer.start()
	try:
		# Consume messages
		async for msg in consumer:
			if msg.topic == "ig":
				data_dict = eval(msg.value)
				print("recived")
				img = base64.b64encode(blur(data_dict["image"])).decode('utf-8')
				r = requests.post("https://api.imgbb.com/1/upload", data={"key": data["IMGBB_TOKEN"], "image":  img, "filename": "image.png"})
				print(r.text)
				print(r.json()["data"]["url"])

				if data_dict['text'] != "None":
					payload = {
						"caption": f"{data_dict['text']}",
						"image_url": r.json()["data"]["url"], 
						"access_token": data["FACEBOOK_TOKEN"]
					}
				else:
					payload = {
						"image_url": r.json()["data"]["url"], 
						"access_token": data["FACEBOOK_TOKEN"]
					}

				r = requests.post(IG_UPLOAD_URL, data=payload)
				print(r.text)
				post_id = r.json()["id"]
				print(post_id)
				r_p = requests.post(IG_POST_URL, params={"creation_id": post_id, "access_token": data["FACEBOOK_TOKEN"]})
				print(r_p.text)

			# print("consumed: ", msg.topic, msg.partition, msg.offset,
			# 	  msg.key, msg.value, msg.timestamp)

	finally:
		# Will leave consumer group; perform autocommit if enabled.
		await consumer.stop()

while True:
	asyncio.run(consume())