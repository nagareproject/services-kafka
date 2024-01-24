# Encoding: utf-8

# --
# Copyright (c) 2008-2024 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

"""Provides the classes to interact with Kafka."""

from __future__ import absolute_import

import kafka

from nagare.server import reference
from nagare.services import plugin


def create_consumer_config_spec():
    TYPE_TRANSLATION = {bool: 'boolean', str: 'string', int: 'integer'}

    config_spec = {
        name: '%s(default=%r)' % (TYPE_TRANSLATION[type(default)], default)
        for name, default in kafka.KafkaConsumer.DEFAULT_CONFIG.items()
        if type(default) in TYPE_TRANSLATION
    }

    config_spec.update(
        dict(
            {
                name: '{}(default={})'.format(type_, kafka.KafkaConsumer.DEFAULT_CONFIG[name])
                for name, type_ in (
                    ('group_id', 'string'),
                    ('receive_buffer_bytes', 'integer'),
                    ('send_buffer_bytes', 'integer'),
                    ('ssl_cafile', 'string'),
                    ('ssl_certfile', 'string'),
                    ('ssl_keyfile', 'string'),
                    ('ssl_crlfile', 'string'),
                    ('ssl_password', 'string'),
                    ('sasl_mechanism', 'string'),
                    ('sasl_plain_username', 'string'),
                    ('sasl_plain_password', 'string'),
                    ('sasl_kerberos_domain_name', 'string'),
                    ('api_version', 'int_list'),
                )
            },
            topics='string_list(default=list())',
            bootstrap_servers='string_list(default=list("localhost"))',
            key_deserializer='string(default=None)',
            value_deserializer='string(default=None)',
            default_offset_commit_callback='string(default=None)',
            partition_assignment_strategy='string_list(default=None)',
            consumer_timeout_ms='integer(default=None)',
            metric_reporters='string_list(default=None)',
            selector='string(default=None)',
        )
    )

    return config_spec


class KafkaConsumer(plugin.Plugin, kafka.KafkaConsumer):
    """The Kafka client service."""

    CONFIG_SPEC = dict(plugin.Plugin.CONFIG_SPEC, **create_consumer_config_spec())

    def __init__(
        self,
        name,
        dist,
        topics=(),
        key_deserializer=None,
        value_deserializer=None,
        default_offset_commit_callback=None,
        partition_assignment_strategy=None,
        consumer_timeout_ms=None,
        socket_options=None,
        ssl_context=None,
        metric_reporters=None,
        selector=None,
        **config,
    ):
        plugin.Plugin.__init__(
            self,
            name,
            dist,
            topics=topics,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
            default_offset_commit_callback=default_offset_commit_callback,
            partition_assignment_strategy=partition_assignment_strategy,
            consumer_timeout_ms=consumer_timeout_ms,
            socket_options=socket_options,
            ssl_context=ssl_context,
            metric_reporters=metric_reporters,
            selector=selector,
            **config,
        )

        if key_deserializer:
            key_deserializer, _ = reference.load_object(key_deserializer)[0]

        if value_deserializer:
            value_deserializer, _ = reference.load_object(value_deserializer)[0]

        if default_offset_commit_callback:
            default_offset_commit_callback, _ = reference.load_object(default_offset_commit_callback)[0]
        else:
            default_offset_commit_callback = kafka.KafkaConsumer.DEFAULT_CONFIG['default_offset_commit_callback']

        if partition_assignment_strategy is not None:
            partition_assignment_strategy = [
                reference.load_object(strategie)[0] for strategie in partition_assignment_strategy
            ]
        else:
            partition_assignment_strategy = kafka.KafkaConsumer.DEFAULT_CONFIG['partition_assignment_strategy']

        if consumer_timeout_ms is None:
            consumer_timeout_ms = kafka.KafkaConsumer.DEFAULT_CONFIG['consumer_timeout_ms']

        if socket_options is None:
            socket_options = kafka.KafkaConsumer.DEFAULT_CONFIG['socket_options']

        if metric_reporters is not None:
            metric_reporters = [reference.load_object(reporter)[0] for reporter in metric_reporters]
        else:
            metric_reporters = kafka.KafkaConsumer.DEFAULT_CONFIG['metric_reporters']

        if selector is not None:
            selector, _ = reference.load_object(selector)[0]
        else:
            selector = kafka.KafkaConsumer.DEFAULT_CONFIG['selector']

        kafka.KafkaConsumer.__init__(
            self,
            *topics,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
            default_offset_commit_callback=default_offset_commit_callback,
            partition_assignment_strategy=partition_assignment_strategy,
            consumer_timeout_ms=consumer_timeout_ms,
            socket_options=socket_options,
            ssl_context=ssl_context,
            metric_reporters=metric_reporters,
            selector=selector,
            **config,
        )


def create_producer_config_spec():
    TYPE_TRANSLATION = {bool: 'boolean', str: 'string', int: 'integer'}

    config_spec = {
        name: '%s(default=%r)' % (TYPE_TRANSLATION[type(default)], default)
        for name, default in kafka.KafkaProducer.DEFAULT_CONFIG.items()
        if type(default) in TYPE_TRANSLATION
    }

    config_spec.update(
        dict(
            {
                name: '{}(default={})'.format(type_, kafka.KafkaProducer.DEFAULT_CONFIG[name])
                for name, type_ in (
                    ('client_id', 'string'),
                    ('compression_type', 'string'),
                    ('receive_buffer_bytes', 'integer'),
                    ('send_buffer_bytes', 'integer'),
                    ('ssl_cafile', 'string'),
                    ('ssl_certfile', 'string'),
                    ('ssl_keyfile', 'string'),
                    ('ssl_crlfile', 'string'),
                    ('ssl_password', 'string'),
                    ('api_version', 'int_list'),
                    ('sasl_mechanism', 'string'),
                    ('sasl_plain_username', 'string'),
                    ('sasl_plain_password', 'string'),
                    ('sasl_kerberos_domain_name', 'string'),
                )
            },
            key_serializer='string(default=None)',
            value_serializer='string(default=None)',
            partitioner='string(default=None)',
            metric_reporters='string_list(default=None)',
            selector='string(default=None)',
        )
    )

    return config_spec


class KafkaProducer(plugin.Plugin, kafka.KafkaProducer):
    CONFIG_SPEC = dict(plugin.Plugin.CONFIG_SPEC, **create_producer_config_spec())

    def __init__(
        self,
        name,
        dist,
        key_serializer=None,
        value_serializer=None,
        partitioner=None,
        socket_options=None,
        ssl_context=None,
        metric_reporters=None,
        selector=None,
        **config,
    ):
        plugin.Plugin.__init__(
            self,
            name,
            dist,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            partitioner=partitioner,
            socket_options=socket_options,
            ssl_context=ssl_context,
            metric_reporters=metric_reporters,
            selector=selector,
            **config,
        )

        if key_serializer:
            key_serializer, _ = reference.load_object(key_serializer)[0]

        if value_serializer:
            value_serializer, _ = reference.load_object(value_serializer)[0]

        if partitioner is not None:
            partitioner, _ = reference.load_object(partitioner)[0]
        else:
            partitioner = kafka.KafkaProducer.DEFAULT_CONFIG['partitioner']

        if socket_options is None:
            socket_options = kafka.KafkaProducer.DEFAULT_CONFIG['socket_options']

        if metric_reporters is not None:
            metric_reporters = [reference.load_object(reporter)[0] for reporter in metric_reporters]
        else:
            metric_reporters = kafka.KafkaProducer.DEFAULT_CONFIG['metric_reporters']

        if selector is not None:
            selector, _ = reference.load_object(selector)[0]
        else:
            selector = kafka.KafkaProducer.DEFAULT_CONFIG['selector']

        kafka.KafkaProducer.__init__(
            self,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            partitioner=partitioner,
            socket_options=socket_options,
            ssl_context=ssl_context,
            metric_reporters=metric_reporters,
            selector=selector,
            **config,
        )
