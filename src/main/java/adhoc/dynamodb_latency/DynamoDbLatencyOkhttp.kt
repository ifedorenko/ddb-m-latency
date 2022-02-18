package adhoc.dynamodb_latency

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.google.common.io.BaseEncoding
import com.google.common.util.concurrent.RateLimiter
import com.google.gson.stream.JsonWriter
import okhttp3.OkHttpClient
import org.slf4j.LoggerFactory
import java.io.StringWriter
import java.lang.Thread.sleep
import java.time.Duration
import kotlin.random.Random


val log = LoggerFactory.getLogger("ddb-m-latency")

fun main() {
    val db = AmazonDynamoDBClientBuilder.standard().build()
    createTableIfNecessary(db)

    val credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance()

    fun cooloff() {
        // wait 5 minutes to be able to distinguish diffirent tests in CloudWatch
        val delay =  Duration.ofMinutes(5)
        log.info("sleeping {} min", delay.toMinutes())
        sleep(delay.toMillis())
    }

    val okhttp = OkHttpClient.Builder()
//        .protocols(listOf(Protocol.HTTP_1_1))
        .addNetworkInterceptor(AWS4SingInterceptor(credentialsProvider.credentials, AWS_REGION))
        .build()

    log.info("=== putItem payload M ===")
    okhttp.testPutItem(::payloadM_100_WCU)
    cooloff()

    log.info("=== putItem payload B 100 WCU ===")
    okhttp.testPutItem(::payloadB_100_WCU)
    cooloff()

    log.info("=== putItem payload B 277 KB ===")
    okhttp.testPutItem(::payloadB_277_KB)
}

fun OkHttpClient.testPutItem(payloadJsonFactory: () -> String) {
    // pace requests at 2 per second to avoid capacity-based throttling
    val pacer = RateLimiter.create(2.0)
    repeat(1000) {
        pacer.acquire()
        // delete the item to start from clean slate
        this.deleteItem(it)

        pacer.acquire()
        // execute PutItem request and print the result
        val result = this.putItem(it, payloadJsonFactory())
        log.info("iteration: $it bodySizeB: ${result.bodySize} elapsedMs: ${result.duration.toMillis()} wcu: ${result.consumedCapacity}")
    }
}

// Generate "nested maps" payload with total size needed to consume 100 WCUs and the following shape:
//
// { "M": {
//     "0": { "M": {
//         "a": { "B": "..." },
//         "b": { "B": "..." },
//         ...
//       }
//     },
//     "1": { "M": {
//       ...
//     },
//     ...
//   }
// }
fun payloadM_100_WCU(): String {
    fun JsonWriter.valueB(bytes: ByteArray) = apply {
        this.beginObject()
        this.name("B").value(BaseEncoding.base64().encode(bytes))
        this.endObject()
    }

    fun JsonWriter.innerPayloadM() = apply {
        val len = 7
        this.beginObject().name("M").beginObject()
        listOf("a", "b", "c", "d", "e", "f", "g", "h", "i", "j").forEach { char ->
            this.name(char)
            this.valueB(Random.nextBytes(len))
        }
        this.endObject().endObject()
    }

    val buf = StringWriter()
    val json = JsonWriter(buf)
    json.beginObject().name("M").beginObject()
    repeat(1050) { count ->
        json.name(count.toString())
        json.innerPayloadM()
    }
    json.endObject().endObject()
    json.close()
    return buf.toString()
}

// Generate "binary" payload with total size needed to consume 100 WCUs
fun payloadB_100_WCU(): String {
    // oddly, payload needs to be 102 KB to hit 100 WCU. is 1 wcu 1 KiB?
    return payloadB(102_000)
}

// Generate "binary" payload with total size matched size of payloadM_100_WCU payload
fun payloadB_277_KB(): String {
    return payloadB(208_000)
}

fun payloadB(size: Int): String {
    val bytes = Random.nextBytes(size)
    val base64 = BaseEncoding.base64().encode(bytes)
    return """{"B": "$base64"}"""
}
