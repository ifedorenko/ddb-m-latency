package adhoc.dynamodb_latency

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSSessionCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.BasicSessionCredentials
import com.amazonaws.auth.SigningAlgorithm
import com.amazonaws.auth.internal.SignerConstants
import com.amazonaws.util.BinaryUtils
import com.amazonaws.util.StringUtils
import com.google.common.collect.ImmutableList
import com.google.common.hash.Hashing
import okhttp3.Headers
import okhttp3.Interceptor
import okhttp3.RequestBody
import okhttp3.Response
import okio.HashingSink
import okio.blackholeSink
import okio.buffer
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import javax.crypto.spec.SecretKeySpec

/**
 * Quick&dirty OkHttp AWS request signature v4 implementation.
 *
 * See https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html
 *
 * [com.amazonaws.auth.AWS4Signer] was used as the source of copy&paste inspiration.
 */
internal class AWS4SingInterceptor(
    credentials: AWSCredentials,
    private val region: String,
) : Interceptor {

    private val credentials = sanitizeCredentials(credentials)

    // TODO this fails when date changes... good enough for "quick&dirty"
    private val signingKey = newSigningKey(this.credentials, DATE.format(ZonedDateTime.now(ZoneOffset.UTC)), region)

    private fun getCanonicalHeaders(headers: Headers): List<Pair<String, String>> {
        val sorted = ArrayList<Pair<String, String>>(headers.size)
        headers.forEach { (header, value) ->
            val lowerCase = StringUtils.lowerCase(header)
            if (!listOfHeadersToIgnoreInLowerCase.contains(lowerCase)) {
                sorted.add(Pair(lowerCase.toCompactedString(), value.toCompactedString()))
            }
        }
        sorted.sortWith { a, b -> String.CASE_INSENSITIVE_ORDER.compare(a.first, b.first) }
        return sorted
    }

    // com.amazonaws.auth.AWS4Signer.getCanonicalizedHeaderString
    private fun String.toCompactedString(): String {
        val compact = StringBuilder()
        StringUtils.appendCompactedString(compact, this)
        return compact.toString()
    }

    private fun getCanonicalHeadersString(headers: List<Pair<String, String>>): String {
        val canonical = StringBuilder()
        headers.forEach { (header, value) ->
            canonical.append(header).append(":").append(value).append(SignerConstants.LINE_SEPARATOR)
        }
        return canonical.toString()
    }

    private fun getSignedHeadersString(headers: List<Pair<String, String>>): String {
        return headers.joinToString(separator = ";") { (header, _) -> "$header" }
    }

    override fun intercept(chain: Interceptor.Chain): Response {
//        val stopwatch = Stopwatch.createStarted()
        val request = chain.request()
        val body = request.body
        if (body == null) {
            return chain.proceed(request)
        }

        val headers = request.headers.newBuilder()

        val now = ZonedDateTime.now(ZoneOffset.UTC)
        val date = DATE.format(now)
        val datetime = DATETIME.format(now)

//        headers.add(HOST, request.url.host)
        headers.add(SignerConstants.X_AMZ_DATE, datetime)
        if (credentials is AWSSessionCredentials) {
            headers.add(SignerConstants.X_AMZ_SECURITY_TOKEN, credentials.sessionToken)
        }

        val canonicalHeaders = getCanonicalHeaders(headers.build())

        // Task 1: Create a canonical request for Signature Version 4
        // https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
        val canonicalRequest = StringBuilder()
            .append(request.method).append(SignerConstants.LINE_SEPARATOR)
            .append("/").append(SignerConstants.LINE_SEPARATOR) // TODO proper CanonicalURI
            .append("").append(SignerConstants.LINE_SEPARATOR) // TODO proper CanonicalQueryString
            .append(getCanonicalHeadersString(canonicalHeaders)).append(SignerConstants.LINE_SEPARATOR)
            .append(getSignedHeadersString(canonicalHeaders)).append(SignerConstants.LINE_SEPARATOR)
            .append(body.sha256())
            .toString()

        // Task 2: Create a string to sign for Signature Version 4
        // https://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html
        val stringToSign = StringBuilder()
            .append(SignerConstants.AWS4_SIGNING_ALGORITHM).append(SignerConstants.LINE_SEPARATOR) //  For SHA256, AWS4-HMAC-SHA256 is the algorithm.
            .append(datetime).append(SignerConstants.LINE_SEPARATOR) // request date value
            .append(scope(date)).append(SignerConstants.LINE_SEPARATOR) // getScope
            .append(BinaryUtils.toHex(sha256(canonicalRequest)))
            .toString()

        // Task 3: Calculate the signature for AWS Signature Version 4
        // https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html
//        val signingKey = newSigningKey(credentials, date, region)
        val signature = sign(stringToSign, signingKey, SigningAlgorithm.HmacSHA256)

        // Task 4: Add the signature to the HTTP request
        // https://docs.aws.amazon.com/general/latest/gr/sigv4-add-signature-to-request.html
        headers.add(
            SignerConstants.AUTHORIZATION,
            StringBuilder()
                .append(SignerConstants.AWS4_SIGNING_ALGORITHM).append(" ")
                .append("Credential=${credentials.getAWSAccessKeyId()}/${scope(date)}").append(", ")
                .append("SignedHeaders=${getSignedHeadersString(canonicalHeaders)}").append(", ")
                .append("Signature=${BinaryUtils.toHex(signature)}")
                .toString()
        )

//        println("signing ${stopwatch.elapsed(MILLISECONDS)}")

        return chain.proceed(request.newBuilder().headers(headers.build()).build())
    }

    private fun RequestBody.sha256(): String {
        val hasher = HashingSink.sha256(blackholeSink())
        hasher.buffer().use { this.writeTo(it) }
        return hasher.hash.hex()
    }

    private fun sha256(text: String): ByteArray {
        return Hashing.sha256().hashString(text, Charsets.UTF_8).asBytes()
    }

    private fun scope(date: String): String {
        return "$date/$region/$SERVICE/${SignerConstants.AWS4_TERMINATOR}"
    }

    // copy&paste from com.amazonaws.auth.AbstractAWSSigner.sanitizeCredentials
    private fun sanitizeCredentials(credentials: AWSCredentials): AWSCredentials {
        var accessKeyId: String? = null
        var secretKey: String? = null
        var token: String? = null
        synchronized(credentials) {
            accessKeyId = credentials.awsAccessKeyId
            secretKey = credentials.awsSecretKey
            if (credentials is AWSSessionCredentials) {
                token = credentials.sessionToken
            }
        }
        if (secretKey != null) secretKey = secretKey!!.trim { it <= ' ' }
        if (accessKeyId != null) accessKeyId = accessKeyId!!.trim { it <= ' ' }
        if (token != null) token = token!!.trim { it <= ' ' }
        return if (credentials is AWSSessionCredentials) {
            BasicSessionCredentials(accessKeyId, secretKey, token)
        } else BasicAWSCredentials(accessKeyId, secretKey)
    }

    private fun newSigningKey(
        credentials: AWSCredentials,
        dateStamp: String,
        regionName: String,
    ): ByteArray {
        val kSecret = ("AWS4" + credentials.awsSecretKey).toByteArray(Charsets.UTF_8)
        val kDate: ByteArray = sign(dateStamp, kSecret, SigningAlgorithm.HmacSHA256)
        val kRegion: ByteArray = sign(regionName, kDate, SigningAlgorithm.HmacSHA256)
        val kService: ByteArray = sign(SERVICE, kRegion, SigningAlgorithm.HmacSHA256)
        return sign(SignerConstants.AWS4_TERMINATOR, kService, SigningAlgorithm.HmacSHA256)
    }

    private fun sign(
        stringData: String,
        key: ByteArray,
        algorithm: SigningAlgorithm,
    ): ByteArray {
        val data = stringData.toByteArray(StringUtils.UTF8)
        return sign(data, key, algorithm)
    }

    private fun sign(
        data: ByteArray,
        key: ByteArray,
        algorithm: SigningAlgorithm,
    ): ByteArray {
        val mac = algorithm.mac
        mac.init(SecretKeySpec(key, algorithm.toString()))
        return mac.doFinal(data)
    }

    companion object {
        val SERVICE = "dynamodb"
        val DATETIME = DateTimeFormatter.ofPattern("YYYYMMdd'T'HHmmss'Z'")
        val DATE = DateTimeFormatter.ofPattern("YYYYMMdd")

        val listOfHeadersToIgnoreInLowerCase = ImmutableList.of("connection", "x-amzn-trace-id")
    }
}