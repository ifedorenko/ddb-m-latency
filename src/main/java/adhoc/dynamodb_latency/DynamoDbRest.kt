package adhoc.dynamodb_latency

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition
import com.amazonaws.services.dynamodbv2.model.BillingMode
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement
import com.amazonaws.services.dynamodbv2.model.KeyType
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import com.amazonaws.services.dynamodbv2.model.StreamSpecification
import com.amazonaws.services.dynamodbv2.model.StreamViewType
import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.amazonaws.services.dynamodbv2.util.TableUtils
import com.google.common.base.Stopwatch
import com.google.gson.JsonParser
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.time.Duration
import java.util.zip.GZIPOutputStream

const val TABLE_NAME = "ifedorenko-ddb-m-payload-20220210.adhoc"
const val AWS_REGION = "us-east-1"
const val DYNAMODB_ENDPOINT = "https://dynamodb.us-east-1.amazonaws.com"

// this allows 100 unthrottled 100KB putItem per second
const val PROVISIONED_THROUGHPUT = 10_000L

fun createTableIfNecessary(db: AmazonDynamoDB): TableDescription {
    try {
        return db.describeTable(DescribeTableRequest(TABLE_NAME)).table
    } catch (_: ResourceNotFoundException) {
        println("Table $TABLE_NAME does not exist, creating...")
        val table = db.createTable(
            CreateTableRequest()
                .withTableName(TABLE_NAME)
                .withAttributeDefinitions(
                    AttributeDefinition("hk", ScalarAttributeType.N),
                )
                .withKeySchema(KeySchemaElement("hk", KeyType.HASH))
                .withStreamSpecification(
                    StreamSpecification().withStreamEnabled(true).withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                )
                .withBillingMode(BillingMode.PROVISIONED)
                .withProvisionedThroughput(
                    ProvisionedThroughput(PROVISIONED_THROUGHPUT, PROVISIONED_THROUGHPUT)
                )
        ).tableDescription
        println("Waiting for table $TABLE_NAME to get created...")
        TableUtils.waitUntilActive(db, TABLE_NAME)
        println("Created table $TABLE_NAME.")
        return table
    }
}


data class DynamoRestResult(
    val bodySize: Int,
    val duration: Duration,
    val consumedCapacity: Int,
)

// executes https post with provided action and body against DynamoDb rest endpoint
fun OkHttpClient.dynamoRest(action: String, bodyJson: String): DynamoRestResult {
    val body = bodyJson.toByteArray(Charsets.UTF_8)

    val request = Request.Builder()
        .url(DYNAMODB_ENDPOINT)
        .addHeader("X-Amz-Target", action)
        .addHeader("Content-Type", "application/x-amz-json-1.0")
//        .addHeader("Content-Encoding", "gzip")
        .addHeader("Accept-Encoding", "identity")
        .post(body.toRequestBody())
        .build()

    val stopwatch = Stopwatch.createStarted()
    val consumedCapacity = this.newCall(request).execute().use { response ->
        if (!response.isSuccessful) {
            println("${response.code}/${response.message}")
            for ((name, value) in response.headers) {
                println("$name: $value")
            }
            println(response.body?.string())
            throw IOException("Unexpected code $response")
        }

        // assume responses always have consumedCapacity... good enough for the immediate purpose
        val json = JsonParser.parseReader(response.body!!.charStream())
        json.asJsonObject
            .getAsJsonObject("ConsumedCapacity")
            .get("CapacityUnits").asBigDecimal.toInt()
    }

    return DynamoRestResult(body.size, stopwatch.elapsed(), consumedCapacity)
}

fun OkHttpClient.putItem(key: Int, payloadJson: String): DynamoRestResult {
    // https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html
    val bodyJson = """
        {
            "TableName": "$TABLE_NAME",
            "Item": {
                "hk": { "N": "$key" },
                "payload": $payloadJson
            },
            "ReturnConsumedCapacity": "TOTAL"
        }
    """.trimIndent()

    return this.dynamoRest("DynamoDB_20120810.PutItem", bodyJson)
}

fun OkHttpClient.deleteItem(key: Int): DynamoRestResult {
    // https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html
    val bodyJson = """
        {
            "TableName": "$TABLE_NAME",
            "Key": {
                "hk": { "N": "$key" }
            },
            "ReturnConsumedCapacity": "TOTAL"
        }
    """.trimIndent()

    return this.dynamoRest("DynamoDB_20120810.DeleteItem", bodyJson)
}

fun ByteArray.gzip(): ByteArray {
    val buf = ByteArrayOutputStream()
    GZIPOutputStream(buf).use { it.write(this) }
    return buf.toByteArray()
}
