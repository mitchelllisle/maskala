package org.mitchelllisle.utils

import com.amazonaws.services.secretsmanager.AWSSecretsManager
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest
import io.circe.Decoder
import io.circe.parser.decode


/** A utility class to interact with AWS Secrets Manager.
 *
 * This class provides methods to retrieve and parse secrets stored in AWS Secrets Manager.
 *
 * @param client The AWSSecretsManager client instance used to access the secrets.
 *               Defaults to a standard client built from the AWSSecretsManagerClientBuilder.
 */
class AWSSecrets(client: AWSSecretsManager = AWSSecretsManagerClientBuilder.standard.build()) {

  /** Converts a JSON string to a case class using implicit Circe decoding.
   *
   * @param json The JSON string to be parsed.
   * @tparam T The type of the case class to which the JSON is to be decoded.
   * @return An instance of the case class T.
   * @throws RuntimeException if unable to parse the JSON string.
   */
  private def stringToCaseClass[T](json: String)(implicit decoder: Decoder[T]): T = {
    decode[T](json) match {
      case Left(error) => throw new RuntimeException(s"Unable to parse json: ${error.getMessage}")
      case Right(value) => value
    }
  }

  /** Retrieves a JSON secret from AWS Secrets Manager and converts it to a case class.
   *
   * @param name The name of the secret to retrieve.
   * @tparam T The type of the case class to which the JSON secret is to be decoded.
   * @return An instance of the case class T.
   * @throws RuntimeException if unable to parse the JSON secret.
   */
  def getJsonAs[T](name: String)(implicit decoder: Decoder[T]): T = {
    val secret = get(name)
    stringToCaseClass[T](secret)
  }

  /** Retrieves a secret as a string from AWS Secrets Manager.
   *
   * @param name The name of the secret to retrieve.
   * @return The secret as a string.
   */
  def get(name: String): String = {
    val request = new GetSecretValueRequest().withSecretId(name)
    client.getSecretValue(request).getSecretString
  }
}
