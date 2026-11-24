
import httpx

from app.schemas.tooldefinition import Tool


GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


WEATHER_CODES = {
    0: "晴天",
    1: "大部晴朗",
    2: "局部多云",
    3: "阴天",
    45: "有雾",
    48: "雾凇",
    51: "小毛毛雨",
    53: "中等毛毛雨",
    55: "强毛毛雨",
    61: "小雨",
    63: "中雨",
    65: "大雨",
    71: "小雪",
    73: "中雪",
    75: "大雪",
    80: "小阵雨",
    81: "中阵雨",
    82: "强阵雨",
    95: "雷雨",
    96: "雷雨伴有小冰雹",
    99: "雷雨伴有大冰雹",
}


async def fetch_weather(city: str) -> dict:
    async with httpx.AsyncClient(timeout=15.0) as client:

        # 1. 根据城市名称查询经纬度
        geo_response = await client.get(
            GEOCODING_URL,
            params={
                "name": city,
                "count": 1,
                "language": "zh",
                "format": "json",
            },
        )
        geo_response.raise_for_status()

        geo_data = geo_response.json()
        locations = geo_data.get("results", [])

        if not locations:
            raise ValueError(f"找不到城市：{city}")

        location = locations[0]
        latitude = location["latitude"]
        longitude = location["longitude"]

        # 2. 根据经纬度查询当前天气
        weather_response = await client.get(
            FORECAST_URL,
            params={
                "latitude": latitude,
                "longitude": longitude,
                "current": (
                    "temperature_2m,"
                    "relative_humidity_2m,"
                    "apparent_temperature,"
                    "precipitation,"
                    "weather_code,"
                    "wind_speed_10m"
                ),
                "timezone": "auto",
            },
        )
        weather_response.raise_for_status()

        weather_data = weather_response.json()
        current = weather_data.get("current")

        if not current:
            raise ValueError(f"未获取到 {city} 的天气数据")

        weather_code = current["weather_code"]

        # 3. 整理成 Agent 能理解的结果
        return {
            "city": location["name"],
            "country": location.get("country"),
            "time": current["time"],
            "weather": WEATHER_CODES.get(
                weather_code,
                f"未知天气代码：{weather_code}",
            ),
            "temperature": current["temperature_2m"],
            "feels_like": current["apparent_temperature"],
            "humidity": current["relative_humidity_2m"],
            "precipitation": current["precipitation"],
            "wind_speed": current["wind_speed_10m"],
        }


async def get_weather(args: dict):
    return await fetch_weather(args["city"])


weather_parameters = {
    "type": "object",
    "properties": {
        "city": {
            "type": "string",
            "description": "要查询天气的城市名称，例如上海、北京、东京",
        }
    },
    "required": ["city"],
}


weather_tool = Tool(
    name="weather",
    description="查询指定城市的真实实时天气",
    parameters=weather_parameters,
    execute=get_weather,
    cacheable=True,
    cache_ttl=60,
)
