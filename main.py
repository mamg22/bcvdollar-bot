import asyncio
import datetime
import logging
from os import getenv
import sqlite3
import time

import aiohttp
from aiogram import Bot, Dispatcher
import aiogram.exceptions
from aiogram.filters import Command, CommandObject
from aiogram.types import Message, BotCommand
from dotenv import load_dotenv
import psycopg

from database import create_database, get_db
import rates

STATS_URL = "https://www.bcv.org.ve/estadisticas/tipo-cambio-de-referencia-smc"
WEEKDAYS = "Lunes Martes Miercoles Jueves Viernes Sábado Domingo".split()


load_dotenv()


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


dp = Dispatcher()


START_MSG = """
¡Hola! Soy un bot diseñado para proveer las tasas de dólar del Banco Central de Venezuela.
Usa el comando /ayuda para conocer las opciones disponibles.

Proyecto no afiliado con el Banco Central de Venezuela.
""".strip()

HELP_MSG = """
Comandos:
/tasa para obtener la tasa actual
/tasa [fecha] para obtener la tasa efectiva para un día específico
/ayuda para obtener ayuda sobre el uso general del bot
/fechas para obtener ayuda sobre formatos de fecha soporatados
""".strip()


HELP_DATEFMT = """
Se reconocen los siguientes formatos de fecha:
día/mes/año
día-mes-año
año-mes-dia
día/mes
día-mes

Ejemplos: 20/07/2025, 20-7-25, 2025-07-20, 20/07
"""


@dp.message(Command("start"))
async def command_start_handler(message: Message) -> None:
    await message.answer(START_MSG)


@dp.message(Command("help", "ayuda"))
async def command_help_handler(message: Message) -> None:
    await message.answer(HELP_MSG)


@dp.message(Command("dates", "fechas"))
async def command_help_dates(message: Message) -> None:
    await message.answer(HELP_DATEFMT)


FORMATS = ["%d/%m/%y", "%d/%m/%Y", "%d-%m-%y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m", "%d-%m"]


def try_parse_date(date_str: str) -> datetime.datetime:
    for fmt in FORMATS:
        try:
            if "%y" in fmt or "%Y" in fmt:
                dt = datetime.datetime.strptime(date_str, fmt)
            else:
                fmt += ";;%Y"
                year = datetime.datetime.now().year
                dt = datetime.datetime.strptime(date_str + ";;" + str(year), fmt)
        except ValueError:
            continue
        else:
            return dt

    raise ValueError(f"Could not parse date: {date_str}")


RATE_FORMAT = """
Tasa efectiva para el dia {target_weekday} {target_date:%Y-%m-%d}:
Tasa del dia {weekday} {date:%Y-%m-%d}
BsD. {rate:.4f}
""".strip()


@dp.message(Command("rate", "tasa"))
async def command_rate(
    message: Message, command: CommandObject, db_conn: sqlite3.Connection
) -> None:
    if command.args is not None:
        try:
            target_date = try_parse_date(command.args.strip())
        except ValueError:
            await message.answer(
                "Fecha invalida, use el comando /fechas para conocer los formatos soportados"
            )
            return
    else:
        target_date = datetime.datetime.now()

    if target_date < datetime.datetime(2020, 3, 30):
        await message.answer("No hay tasas de cambio disponibles antes de 2020-03-30")
        return

    cur = db_conn.cursor()
    cur.execute(
        """SELECT effective_at, value
        FROM Rates
        WHERE effective_at <= ?
        ORDER BY effective_at DESC
        LIMIT 1""",
        (target_date,),
    )
    rate_data = cur.fetchone()

    if rate_data is not None:
        date, rate = rate_data
    else:
        await message.answer("Error: No se pudo obtener la información de la tasa")
        return

    await message.answer(
        RATE_FORMAT.format(
            target_weekday=WEEKDAYS[target_date.weekday()],
            target_date=target_date,
            weekday=WEEKDAYS[date.weekday()],
            date=date,
            rate=rate,
        )
    )


UPDATE_FORMAT = """
🚨 Nueva tasa Dólar 🚨

Efectiva el día {weekday} {date:%Y-%m-%d}
BsD. {rate:.4f} ({change:+.4f})
""".strip()


def get_next_check_delay(short_term: bool) -> float:
    day_delta = datetime.timedelta(days=1)
    now = datetime.datetime.now()
    next_day = now + day_delta

    # Skip checking on weekends
    if next_day.weekday() >= 5:
        next_day += day_delta * (7 - next_day.weekday())

    next_check = next_day.replace(hour=13, minute=0, second=0)

    if short_term and now.weekday() < 5:
        logger.debug("Next check: In 15 minutes")
        return 15 * 60

    logger.debug(f"Next check: {next_check}")

    return (next_check - now).total_seconds()


async def broadcast_update(bot: Bot, db_conn: sqlite3.Connection, new_rates: int):
    cur = db_conn.cursor()
    cur.execute(
        """SELECT effective_at, value,
            value - LAG(value, 1) OVER (ORDER BY effective_at) AS change
        FROM Rates
        ORDER BY effective_at DESC
        LIMIT ?""",
        (min(new_rates, 15),),
    )
    data = cur.fetchall()

    for rate_data in reversed(data):
        if rate_data is not None:
            date, rate, change = rate_data
        else:
            return

        change = change if change is not None else rate

        text = UPDATE_FORMAT.format(
            weekday=WEEKDAYS[date.weekday()], date=date, rate=rate, change=change
        )

        await bot.send_message(
            chat_id="@bcvdolarbot",
            text=text,
        )


async def update_timer(bot: Bot, db_conn: sqlite3.Connection):
    while True:
        new_rates = await rates.store_rates(db_conn)
        logger.debug(f"Finished checking for rate updates, found {new_rates} new rates")
        if new_rates > 0:
            await broadcast_update(bot, db_conn, new_rates)
            # If a new rate is added, wait for the next day for a new one
            await asyncio.sleep(get_next_check_delay(short_term=False))
        else:
            await asyncio.sleep(get_next_check_delay(short_term=True))


COMMANDS_ES = [
    BotCommand(
        command="ayuda",
        description="Información sobre uso y comandos del bot",
    ),
    BotCommand(
        command="fechas",
        description="Obtener ayuda sobre formatos de fecha soporatados",
    ),
    BotCommand(
        command="tasa",
        description="Obtener tasa actual, o tasa para una determinada fecha",
    ),
]

COMMANDS = [
    BotCommand(
        command="help",
        description="Bot usage and command information",
    ),
    BotCommand(
        command="dates",
        description="Get help about supported date formats",
    ),
    BotCommand(
        command="rate",
        description="Get current rate, or rate for a given date",
    ),
]


async def main() -> None:
    token = getenv("BOT_TOKEN")
    if token is None:
        raise RuntimeError("Required BOT_TOKEN environment variable is undefined!")

    bot = Bot(token=token)
    await bot.set_my_commands(commands=COMMANDS_ES, language_code="es")
    await bot.set_my_commands(commands=COMMANDS)

    db_conn = sqlite3.connect(
        get_db(),
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
    create_database()

    logger.debug(f"Loaded database at {get_db()}")

    async with asyncio.TaskGroup() as tg:
        tg.create_task(update_timer(bot, db_conn))
        tg.create_task(dp.start_polling(bot, db_conn=db_conn))


if __name__ == "__main__":
    for n in range(1, 11):
        try:
            asyncio.run(main())
        except aiogram.exceptions.TelegramNetworkError:
            logger.warning(
                f"Failed initial connection, retrying in 10 seconds attempt {n:2d}/10"
            )
            if n < 10:
                time.sleep(10)
            else:
                raise
