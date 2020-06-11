import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Constants.STREAM_HOST
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.event.Event
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig.*
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.Properties as p

val kafkaProducer = KafkaProducer<String, String>(p().also {
    it.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    it.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.canonicalName)
    it.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.canonicalName)
})

fun main() {
    val latch = CountDownLatch(1)
    TwitterThread(latch).run()
}

class TwitterThread(val latch: CountDownLatch) : Runnable {

    val logger = LoggerFactory.getLogger(TwitterThread::class.java)

    override fun run() {
        val msqQueue = LinkedBlockingQueue<String>(100000)
        val eventQueue = LinkedBlockingQueue<Event>(1000)
        val hoseBirdHosts = HttpHosts(STREAM_HOST)
        val endPoint = StatusesFilterEndpoint()

        endPoint.trackTerms(listOf("PC Siqueira"))

        val oAuth = OAuth1(
        )
        val builder = ClientBuilder()
            .name("Hosebird-Client-01")
            .hosts(hoseBirdHosts)
            .authentication(oAuth)
            .endpoint(endPoint)
            .processor(StringDelimitedProcessor(msqQueue))
            .eventMessageQueue(eventQueue)

        val client = builder.build()

        client.connect()
        try {
            while (!client.isDone) {
                val msg = msqQueue.take()
                println(msg)
            }
        } catch (ex: InterruptedException) {
            logger.error("Shutdown...")
        } catch (ex: Exception) {
            logger.error("Client error: ", ex)
        } finally {
            client.stop()
            latch.countDown()
        }
    }

}
