#!/usr/bin/env python3

import logging
from lzma import compress, decompress
from asyncio import gather
import json
from aiohttp import web
from msgpack import packb, unpackb

from evnotify import EVNotify
from abrp import Abrp
from influxdb import InfluxDB
from mqtt import Mqtt

with open('config.json', 'r') as config:
    C = json.loads(config.read())

# logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

AUTH_KEYS = C['auth_keys']
ABRP_API_KEY = C['abrp_api_key']


def msg_encode(msg):
    """ encode and compress message """
    return compress(packb(msg))


def msg_decode(msg):
    """ decompress and decode message """
    return unpackb(decompress(msg), use_list=False)


SERVICES = {}

routes = web.RouteTableDef()


@routes.post(r'/setsvcsettings/{id}')
async def set_service_settings(request):
    """ get service configs from client and initialize backends accordingly """
    if request.headers.get('Authorization') not in AUTH_KEYS:
        return web.Response(status=401)

    carid = request.match_info['id']

    data = await request.read()
    settings = unpackb(decompress(data), use_list=False)  # tuples are quicker

    # not sure how to hadle mqtt yet. Hardcode for now...
    if 'mqtt' not in settings:
        settings['mqtt'] = {'mqtt_server': C['mqtt_server'], 'enable': True}

    SERVICES[carid] = {}

    fields = set()
    for service, svc_settings in settings.items():
        if svc_settings.get('enable') is not True:
            continue

        match service:
            case 'mqtt':
                svc = Mqtt(svc_settings, carid)
            case 'influxdb':
                svc = InfluxDB(svc_settings, carid)
            case 'abrp':
                svc = Abrp(svc_settings, ABRP_API_KEY)
            case 'evnotify':
                svc = EVNotify(svc_settings)
            case _:
                # Unknown service, skip rest of loop
                continue

        SERVICES[carid][service] = svc
        svc_fields = svc.which_fields()
        if svc_fields is None:
            fields = None
        if fields is not None:
            fields |= svc.which_fields()

    return web.Response(body=msg_encode({'fields': fields}))


@routes.post('/transmit/{id}')
async def transmit(request):
    """ receive data and forward it to backends """
    if request.headers.get('Authorization') not in AUTH_KEYS:
        return web.Response(status=401)

    carid = request.match_info['id']

    if carid not in SERVICES:
        return web.Response(status=402, text='settings required')

    data = await request.read()
    points = msg_decode(data)

    services = [svc.transmit(points)
                for svc in SERVICES[carid].values()]
    await gather(*services)

    return web.Response()


if __name__ == '__main__':
    app = web.Application()
    app.add_routes(routes)
    web.run_app(app, host='::1', port=8001)
