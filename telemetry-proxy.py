#!/usr/bin/env python3

import logging
from lzma import decompress
from asyncio import gather
import json
from aiohttp import web
from msgpack import unpackb

import aiomqtt

from evnotify import EVNotify
from abrp import Abrp
from influxdb import InfluxDB

with open('config.json', 'r') as config:
    C = json.loads(config.read())

SVC_SETTINGS = {}

#logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

AUTH_KEYS = C['auth_keys']
ABRP_API_KEY = C['abrp_api_key']


EVN = {}
async def xmit_evnotify(carid, settings, data, fields):
    if carid not in EVN:
        EVN[carid] = EVNotify(settings)

    await EVN[carid].transmit(fields)


ABRP = {}
async def xmit_abrp(carid, settings, data, fields):
    if carid not in ABRP:
        ABRP[carid] = Abrp(settings, ABRP_API_KEY)

    await ABRP[carid].transmit(fields)


INFLUXDB = {}
async def xmit_influxdb(carid, settings, data, fields):
    if carid not in INFLUXDB:
        INFLUXDB[carid] = InfluxDB(settings, carid)

    await INFLUXDB[carid].transmit(data)


async def xmit_mqtt(carid, settings, data, fields):
    if 'SOC_DISPLAY' in fields:
        async with aiomqtt.Client(C['mqtt_server']) as client:
            await client.publish(f'Car/{carid}/SOC', payload=float(fields['SOC_DISPLAY']))


SERVICES = {
        'evnotify': xmit_evnotify,
        'abrp': xmit_abrp,
        'influxdb': xmit_influxdb,
        'mqtt': xmit_mqtt,
        }

routes = web.RouteTableDef()


@routes.post(r'/setsvcsettings/{id}')
async def set_service_settings(request):
    if request.headers.get('Authorization') not in AUTH_KEYS:
        return web.Response(status=401)

    carid = request.match_info['id']

    data = await request.read()
    settings = unpackb(decompress(data), use_list=False)  # tuples are quicker
    SVC_SETTINGS[carid] = settings

    return web.Response()

last_fields = {}

@routes.post('/transmit/{id}')
async def transmit(request):
    if request.headers.get('Authorization') not in AUTH_KEYS:
        return web.Response(status=401)

    carid = request.match_info['id']

    if carid not in SVC_SETTINGS:
        return web.Response(status=402, text='settings required')

    data = await request.read()
    points = unpackb(decompress(data), use_list=False)

    if carid not in last_fields:
        last_fields[carid] = {}

    last_fields[carid].update(points[-1]['fields'])  # we only get changed fields. Keep unchanged fields around
    
    #log.debug(f'{len(points)=}')

    svc_settings = SVC_SETTINGS[carid]
    await gather(*[SERVICES[svc](carid, settings, points, last_fields[carid])
                   for svc, settings in svc_settings.items()])

    return web.Response()

if __name__ == '__main__':
    app = web.Application()
    app.add_routes(routes)
    web.run_app(app, host='::1', port=8001)
