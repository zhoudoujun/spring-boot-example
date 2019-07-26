package com.adou.example.utils.kafka.producer.handler;

import java.io.Serializable;

import kafka.producer.Partitioner;
import kafka.serializer.Encoder;

interface IProducerHandler<V extends Serializable> extends Partitioner, Encoder<V> {
}
