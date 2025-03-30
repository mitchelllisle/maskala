import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import org.mitchelllisle.utils.AWSSecrets
import org.scalatest.flatspec.AnyFlatSpec

import com.amazonaws.services.secretsmanager.AWSSecretsManager
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder


class AWSSecretsTest extends AnyFlatSpec {
  case class Secret(username: String, password: String)
  implicit val configDecoder: Decoder[Secret] = deriveDecoder[Secret]

  private def getLocalStackClient: AWSSecretsManager = {
    val localstackEndpoint = "http://localhost:4566"
    val awsRegion = "us-east-1"

    AWSSecretsManagerClientBuilder.standard()
      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(localstackEndpoint, awsRegion))
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("test", "test")))
      .build()
  }

  "getting a secret" should "return the string value" in {
    val secrets = new AWSSecrets(getLocalStackClient)
    val secret = secrets.getJsonAs[Secret]("arn:aws:secretsmanager:us-east-1:000000000000:secret:test-secret-bWjlVz")
    assert(secret.username == "admin")
    assert(secret.password == "password")
  }
}
