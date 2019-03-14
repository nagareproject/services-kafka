# --
# Copyright (c) 2008-2019 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

import time

from nagare.admin import command


class Commands(command.Commands):
    DESC = 'Kafka messaging subcommands'


class Command(command.Command):
    WITH_STARTED_SERVICES = True

    def run_with_channel(self, kafka_consumer_service=None, kafka_producer_service=None, **config):
        try:
            self.run(kafka_consumer_service, kafka_producer_service, **config)
        except KeyboardInterrupt:
            pass

        kafka_consumer_service.close()
        kafka_producer_service.close()

        return 0

    def _run(self, command_names, **params):
        return super(Command, self)._run(command_names, self.run_with_channel, **params)


class Receive(Command):
    DESC = 'receive data from Kafka topics'

    def __init__(self, name, dist, **config):
        super(Receive, self).__init__(name, dist, **config)
        self.nb = 0

    def set_arguments(self, parser):
        parser.add_argument('-t', '--topics', action='append', help='name of the topics to receive from')
        super(Receive, self).set_arguments(parser)

    def run(self, consumer, _, topics):
        if topics:
            consumer.subscribe(topics)

        topics = ['`{}`'.format(topic) for topic in sorted(consumer.subscription())]
        print('Listening on topics {} ...'.format(', '.join(topics)))

        for msg in consumer:
            print('- {} --------------------'.format(self.nb))

            if msg.key is not None:
                print('Key  : {}'.format(msg.key))
            print('Value: {}'.format(msg.value))
            print('')

            properties = msg._asdict()
            del properties['key']
            del properties['value']

            print('Properties:')
            padding = len(max(properties, key=len))
            for k, v in sorted(properties.items()):
                print(' - {}: {}'.format(k.ljust(padding), v))

            self.nb += 1
            print('')


class Send(Command):
    DESC = 'send data on a Kafka topic'

    def set_arguments(self, parser):
        parser.add_argument('topic', help='kafka topic')
        parser.add_argument('-k', '--key', help='message key')
        parser.add_argument(
            '-l', '--loop', action='store_true',
            help='infinite loop sending <data> each 2 secondes'
        )

        parser.add_argument('value', help='string to send')

        super(Send, self).set_arguments(parser)

    @staticmethod
    def run(_, producer, loop, topic, key, value):
        print("Sending on topic `{}`".format(topic))

        if key is not None:
            key = key.encode('utf-8')

        while True:
            producer.send(topic, value.encode('utf-8'), key).get()

            if not loop:
                break

            time.sleep(1)
