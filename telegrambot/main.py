import json
import io

import aiogram
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove, BufferedInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
import aiokafka
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncio
import requests

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

async def send_to_broker(theme, image_bytes, text):
	producer = AIOKafkaProducer(bootstrap_servers=data["KAFKA_HOST"])
	await producer.start()
	try:
		await producer.send(theme,  bytes(str({"image": image_bytes, "text": text}), "utf-8"))
	finally:
		await producer.stop()


bot = Bot(token=data["TELEGRAM_BOT_TOKEN"], parse_mode="HTML")
dp = Dispatcher()

@dp.message(F.photo)
async def get_photo(message: types.Message):
	b_photo = io.BytesIO()
	file = await bot.get_file(message.photo[-1].file_id)
	file_path = file.file_path
	image = await bot.download_file(file_path, b_photo)

	builder = InlineKeyboardBuilder()
	builder.button(text="📸 В Instagram", callback_data="sig")
	builder.button(text="💙 В Facebook", callback_data="sfb")
	builder.row(InlineKeyboardButton(text="📣 У всі соц. мережі", callback_data="se"))
	builder.row(InlineKeyboardButton(text="❌ НЕ публікувати", callback_data="n"))

	await bot.send_photo(chat_id=data["TELEGRAM_ADMIN_ID"], photo=BufferedInputFile(image.read(), filename="image.png"), 
		caption=f"*separator*{message.caption}*separator*{message.from_user.id}*separator*\n{message.from_user.first_name} {message.from_user.last_name} @{message.from_user.username} <code>{message.from_user.id}</code>",
		reply_markup=builder.as_markup()
	)


@dp.callback_query()
async def q_handler(query: types.CallbackQuery):
	if query.data[0] == "s":
		args = query.message.caption.split("*separator*")[1:]
		b_photo = io.BytesIO()
		file = await bot.get_file(query.message.photo[-1].file_id)
		file_path = file.file_path
		image = await bot.download_file(file_path, b_photo)

		try:
			await send_to_broker(query.data[1:], image.read(), args[0])
			await query.answer("Успішно відправлено на публікацію")
		except aiokafka.errors.KafkaConnectionError:
			await query.answer("Вибачте, сталася помилка при спробі підключення до брокера. Повідомте розробника.")
	elif query.data[0] == "n":
		await query.answer("Успішно відхилено публікацію", show_alert=True)
		await bot.delete_message(query.message.chat.id, query.message.message_id)


if __name__ == "__main__":
	dp.run_polling(bot)