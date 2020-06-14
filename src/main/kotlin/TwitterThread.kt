import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.event.Event
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.OAuth1
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue

class TwitterThread(val consumerKey: String, val consumerSecret: String, val token: String, val tokenSecret: String, val terms: List<String>, val latch: CountDownLatch, val onMsgReceived: (String) -> Unit) : Runnable {

    val logger = LoggerFactory.getLogger(TwitterThread::class.java)

    override fun run() {
        val msqQueue = LinkedBlockingQueue<String>(100000)
        val eventQueue = LinkedBlockingQueue<Event>(1000)
        val hoseBirdHosts = HttpHosts(Constants.STREAM_HOST)
        val endPoint = StatusesFilterEndpoint()

        endPoint.trackTerms(terms)

        val oAuth = OAuth1(consumerKey, consumerSecret, token, tokenSecret)
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
                onMsgReceived(msg)
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
