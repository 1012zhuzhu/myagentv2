import asyncio

from app.schemas.tooldefinition import Tool


async def get_weather(args: dict):
    city = args["city"]

    print(f"[START] weather: {city}")
    await asyncio.sleep(2)
    print(f"[END] weather: {city}")

    return {
        "city": city,
        "weather": "晴天",
        "temperature": 25
    }


async def get_weather_2(args: dict):
    city = args["city"]

    print(f"[START] weather_2: {city}")
    await asyncio.sleep(2)
    print(f"[END] weather_2: {city}")

    return {
        "city": city,
        "weather": "多云",
        "temperature": 20
    }


async def get_weather_3(args: dict):
    city = args["city"]

    print(f"[START] weather_3: {city}")
    await asyncio.sleep(2)
    print(f"[END] weather_3: {city}")

    return {
        "city": city,
        "weather": "小雨",
        "temperature": 18
    }


weather_tool = Tool(
    name="weather",
    description="查询城市天气",
    parameters={
        "type": "object",
        "properties": {
            "city": {
                "type": "string",
                "description": "要查询天气的城市"
            }
        },
        "required": ["city"]
    },
    execute=get_weather,
    cacheable=True,
    cache_ttl=60,
)


weather_tool_2 = Tool(
    name="weather_2",
    description="查询城市天气",
    parameters=weather_tool.parameters,
    execute=get_weather_2
)


weather_tool_3 = Tool(
    name="weather_3",
    description="查询城市天气",
    parameters=weather_tool.parameters,
    execute=get_weather_3
)