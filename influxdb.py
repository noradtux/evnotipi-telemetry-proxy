""" write data to influxdb """
from datetime import datetime, timezone
import logging
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
import pyrfc3339
from aiohttp.client_exceptions import ClientConnectorError

log = logging.getLogger('InfluxDB')

INT_FIELD_LIST = {'charging', 'fanFeedback', 'fanStatus', 'fix_mode',
                  'normalChargePort', 'rapidChargePort', 'submit_queue_len'}
STR_FIELD_LIST = {'carid', 'cartype', 'akey', 'gps_device'}


class InfluxDB():
    """ write data to influxdb """
    def __init__(self, settings, carid):
        log.info('Initializing InfluxDB')

        self._vehicle_id = carid
        self._bucket = settings['bucket']
        self._influx = InfluxDBClientAsync(
                url=settings['url'],
                org=settings['org'],
                token=settings['token'])
        self._iwrite = self._influx.write_api()

    async def close(self):
        await self._influx.close()

    async def transmit(self, dataset):
        """ forward data to db as received """
        points = []
        for data in dataset:
            if not 'timestamp' in data:
                log.error('Timestamp missing %s', data)
                continue

            point = {'measurement': 'telemetry', 'tags': {}}

            fields = {}
            for key, value in data.items():
                if value is None:
                    continue

                if key in STR_FIELD_LIST:
                    point['tags'][key] = str(value)
                else:
                    fields[key] = int(value) if key in INT_FIELD_LIST else float(value)

            point['time'] = pyrfc3339.generate(datetime.fromtimestamp(
                data['timestamp'], timezone.utc))
            point['fields'] = fields

            points.append(point)

        try:
            log.debug('enqueue %s', points)
            await self._iwrite.write(bucket=self._bucket,
                                     record=points)
            points.clear()
        except InfluxDBError as exception:
            log.warning("InfluxDBError(%s)", str(exception))
        except ClientConnectorError as exception:
            log.warning("aiohttp ClientConnectorError (%s)", str(exception))

    @staticmethod
    def which_fields():
        """ Return set of fields this module likes to use """
        return None     # None meaning "all"
