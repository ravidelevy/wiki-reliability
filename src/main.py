import asyncio
from crawler import crawl, save

async def main():
    data = await crawl("data", number_of_records=1350)
    save(data, "pages")

if __name__ == '__main__':
    asyncio.run(main())
