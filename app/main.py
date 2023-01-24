import pydantic
from fastapi import FastAPI, Request, status, HTTPException
from fastapi.responses import RedirectResponse
from .databases import MongoDatabase
from typing import List

app = FastAPI()

mongo = MongoDatabase()
schedule_collection = 'schedule'


@app.get("/")
async def home(request: Request):
    redirect_url = request.url_for('crawlers')
    return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)


async def get_crawlers_logs() -> dict:
    names = mongo.database.list_collection_names()
    crawler_logs = {}
    for name in names:
        if name == schedule_collection:
            continue
        if 'time_' in name:
            x = name[5:]
            crawler_logs[x] = await get_crawler_logs(x)

    return crawler_logs


@app.get("/crawlers/", status_code=status.HTTP_200_OK)
async def crawlers():
    logs = await get_crawlers_logs()
    return logs


async def get_crawler_logs(crawler_name: str) -> List[dict]:
    if crawler_name == schedule_collection:
        raise Exception(f"{crawler_name} is not a crawler name")
    collection = mongo.database[f'time_{crawler_name}']

    logs = [
        {
            'date_time': x['date_time'],
            'time_spent': x['time_spent'],
            'state': x['state'],
        }
        for x in collection.find()
    ]

    return list(reversed(logs))


@app.get("/crawlers/{crawler_name}/", status_code=status.HTTP_200_OK)
async def crawler(crawler_name: str):
    logs = await get_crawler_logs(crawler_name)
    return logs


async def get_schedule_log(crawler_name: str) -> dict:
    collection = mongo.database[schedule_collection]

    return collection.find_one(crawler_name=crawler_name)


async def get_schedule_logs() -> List[dict]:
    collection = mongo.database[schedule_collection]

    logs = [
        {
            'crawler_name': x['crawler_name'],
            'day': x['day'],
            'hour': x['hour'],
            'minutes': x['minutes'],
            'seconds': x['seconds'],
        }
        for x in collection.find()
    ]

    return logs


@app.get("/schedules/", status_code=status.HTTP_200_OK)
async def schedules():
    logs = await get_schedule_logs()
    return logs


class Schedule(pydantic.BaseModel):
    crawler_name: str
    day: int
    hour: int
    minutes: int
    seconds: int


async def update_schedule_log(schedule: Schedule) -> Schedule:
    collection = mongo.database[schedule_collection]

    query = {'crawler_name': {'$eq': schedule.crawler_name}}
    new_values = {"$set": schedule.dict()}
    if collection.find_one(query) is None:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail='There is no crawler with this name')

    collection.update_one(query, new_values)

    return schedule


@app.put("/schedules/", status_code=status.HTTP_426_UPGRADE_REQUIRED)
async def update_schedule(schedule: Schedule):
    result = await update_schedule_log(schedule)
    return result


async def create_schedule_log(schedule: Schedule) -> Schedule:
    collection = mongo.database[schedule_collection]
    query = {'crawler_name': {'$eq': schedule.crawler_name}}
    if collection.find_one(query):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail='crawler with this name already exist')

    collection.insert_one(schedule.dict())

    return schedule


@app.post("/schedules/", status_code=status.HTTP_201_CREATED)
async def create_schedule(schedule: Schedule):
    result = await create_schedule_log(schedule)
    return result


async def get_information(crawler_name: str) -> List[dict]:
    if crawler_name == schedule_collection:
        raise Exception(f"{crawler_name} is not a crawler name")
    collection = mongo.database[f'data_{crawler_name}']
    logs = []
    if crawler_name == 'mopon':
        logs = [
            {
                "id": x["id"],
                "name": x["name"],
                "title": x["title"],
                "code": x["code"],
            }
            for x in collection.find()
        ]
    elif crawler_name == 'digikala':
        logs = [
            {
                'id': x['id'],
                'title': x['title'],
                'category': x['category'],
                'exist': x['exist'],
                'rrp_price': x['rrp_price'],
                'selling_price': x['selling_price'],
            }
            for x in collection.find()
        ]

    return list(reversed(logs))


@app.get("/information/{crawler_name}/", status_code=status.HTTP_200_OK)
async def crawler(crawler_name: str):
    info = await get_information(crawler_name)
    return info
