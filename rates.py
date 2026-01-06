import datetime
import decimal

import aiohttp
from lxml import html
import psycopg
import xlrd
from yarl import URL


RATES_URL = URL("https://www.bcv.org.ve/estadisticas/tipo-cambio-de-referencia-smc")
VE_TZ = datetime.timezone(-datetime.timedelta(hours=4))
REDENOMINATION_DAY = datetime.datetime(2021, 10, 1, tzinfo=VE_TZ)
REDENOMINATION_FACTOR = 1_000_000


async def get_sheet_urls(session: aiohttp.ClientSession):
    next_url = RATES_URL

    urls = []

    while next_url:
        async with session.get(next_url, ssl=False) as res:
            document = html.fromstring(await res.text())
            main_block = document.get_element_by_id("block-system-main")

            urls.extend(
                el.attrib["href"]
                for el in map(
                    lambda icon: icon.getparent(),
                    main_block.find_class("file-icon"),
                )
                if el is not None
            )

            pagination = main_block.find_class("pagination")[0]

            if next := pagination.find_class("next"):
                anchor = next[0][0]
                next_url = next_url.join(URL(anchor.attrib["href"]))
            else:
                break

    return urls


def get_sheet_rate(sheet: xlrd.sheet.Sheet):
    date_cell = sheet[4][3]
    rate_date = datetime.datetime.strptime(
        date_cell.value.split()[-1], "%d/%m/%Y"
    ).astimezone(VE_TZ)

    value = decimal.Decimal(sheet[14][-1].value)

    if rate_date < REDENOMINATION_DAY:
        value /= REDENOMINATION_FACTOR

    return rate_date, value


async def get_rate_chunks():
    async with aiohttp.ClientSession() as session:
        sheet_urls = await get_sheet_urls(session)

        for sheet_url in sheet_urls:
            async with session.get(sheet_url, ssl=False) as res:
                with xlrd.open_workbook(
                    file_contents=(await res.read()), on_demand=True
                ) as book:
                    yield (get_sheet_rate(sheet) for sheet in book)


async def store_rates(db_conn: psycopg.AsyncConnection):
    new_rows = 0
    async with db_conn.cursor() as acur:
        async for chunk in get_rate_chunks():
            await acur.executemany(
                "INSERT INTO Rates VALUES (%s, %s) ON CONFLICT DO NOTHING",
                chunk,
            )
            await db_conn.commit()

            count = acur.rowcount
            new_rows += count

            if count == 0:
                break

    return new_rows


async def rate_at(dt: datetime.datetime, db_conn: psycopg.AsyncConnection):
    async with db_conn.cursor() as acur:
        await acur.execute(
            """SELECT effective_at, value
            FROM Rates
            WHERE effective_at <= %s
            ORDER BY effective_at DESC
            LIMIT 1""",
            (dt,),
        )
        return await acur.fetchone()
