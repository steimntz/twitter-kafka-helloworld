@file:JvmName("Application")

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.concurrent.CountDownLatch
import java.util.Properties as p

val kafkaProperties = p().also {
    it.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    it.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.canonicalName)
    it.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.canonicalName)
}

val logger = LoggerFactory.getLogger("MAIN")

fun main(args: Array<String>) {
    val latch = CountDownLatch(1)

    val kafkaProducer = KafkaProducer<String, String>(kafkaProperties)

    TwitterThread(args[0], args[1], args[2], args[3], latch) { msg ->
        val record = ProducerRecord<String, String>("twitter", msg)
        kafkaProducer.send(record) { metadata, ex ->
            if (ex == null) {
                logger.trace("Receive metadata: \n" +
                        "Topic: " + metadata.topic() + "\n" +
                        "Partition: " + metadata.partition() + "\n" +
                        "Offset: " + metadata.offset() + "\n" +
                        "Timestamp: " + metadata.timestamp());
            } else {
                logger.error("Error on producing", ex)
            }
        }
        kafkaProducer.flush()
    }.run()

    kafkaProducer.close()
}
