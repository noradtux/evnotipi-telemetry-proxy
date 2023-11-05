import logging
import aiomqtt

log = logging.getLogger('MQTT')


class Mqtt():
    """ State wrapper """

    def __init__(self, settings, carid):
        self._mqtt_server = settings['mqtt_server']
        self._carid = carid

    async def transmit(self, fields):
        mqtt_server = self._mqtt_server
        carid = self._carid

        if 'SOC_DISPLAY' in fields:
            async with aiomqtt.Client(mqtt_server) as client:
                await client.publish(f'Car/{carid}/SOC',
                                     payload=float(fields['SOC_DISPLAY']))

    @staticmethod
    def which_fields():
        """ Return set of fields this module likes to use """
        return set(['SOC_DISPLAY'])
