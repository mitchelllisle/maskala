package org.mitchelllisle.analysers

import org.apache.spark.sql.{DataFrame, functions => F}
import java.security.MessageDigest
import java.time.Instant
import scala.annotation.tailrec

case class MerkleProof(
                        rootHash: String,
                        recordCount: Long,
                        leafHashes: Seq[String],
                        timestamp: Instant = Instant.now()
                      )

case class DeletionProof(
                          beforeProof: MerkleProof,
                          afterProof: MerkleProof,
                          deletedRecordHashes: Seq[String],
                          merklePathProofs: Seq[String]
                        )

/** MerkleTreeAnalyser provides cryptographic proof capabilities for data retention and deletion verification.
 * This complements the KHyperLogLogAnalyser by adding tamper-evident audit trails.
 */
object MerkleTree {
  /** Main entry point for creating a Merkle proof. This is the standard way to use MerkleTree.
   *
   * @param data The DataFrame to create proof for
   * @param columns Columns to include in leaf hashes
   * @param idColumn Primary key column
   * @return MerkleProof containing root hash and metadata
   */
  def apply(data: DataFrame, columns: Seq[String], idColumn: String): MerkleProof = {
    createMerkleProof(data, columns, idColumn)
  }

  private def sha256(input: String): String = {
    MessageDigest.getInstance("SHA-256")
      .digest(input.getBytes("UTF-8"))
      .map("%02x".format(_)).mkString
  }

  /** Creates leaf hashes for each record in the DataFrame.
   *
   * @param data The DataFrame containing records to hash
   * @param columns Columns to include in the hash (for privacy, exclude sensitive data)
   * @param idColumn Primary key column for tracking individual records
   * @return DataFrame with record hashes
   */
  def createLeafHashes(data: DataFrame, columns: Seq[String], idColumn: String): DataFrame = {
    data.withColumn(
      "leaf_hash",
      F.sha2(
        F.concat_ws("|",
          F.col(idColumn), // Include ID for uniqueness
          F.to_json(F.struct(columns.map(F.col): _*))
        ),
        256
      )
    ).select(F.col(idColumn), F.col("leaf_hash"))
  }

  /** Builds a Merkle tree from leaf hashes and returns the root hash.
   * This is done in Spark for scalability, though it requires collecting intermediate results.
   *
   * @param hashes Sequence of leaf hash strings
   * @return The root hash of the Merkle tree
   */
  @tailrec
  private def buildMerkleTree(hashes: Seq[String]): String = {
    if (hashes.length == 1) {
      hashes.head
    } else {
      val pairedHashes = hashes.grouped(2).map {
        case Seq(left, right) => sha256(left + right)
        case Seq(single) => single // Odd number case - promote single hash up
      }.toSeq
      buildMerkleTree(pairedHashes)
    }
  }

  /** Creates a complete Merkle proof for the current state of data.
   *
   * @param data The DataFrame to create proof for
   * @param columns Columns to include in leaf hashes
   * @param idColumn Primary key column
   * @return MerkleProof containing root hash and metadata
   */
  def createMerkleProof(data: DataFrame, columns: Seq[String], idColumn: String): MerkleProof = {
    val leafHashesDF = createLeafHashes(data, columns, idColumn).orderBy(idColumn)
    val leafHashes = leafHashesDF.select("leaf_hash").collect().map(_.getString(0)).toSeq
    val recordCount = leafHashesDF.count()

    val rootHash = if (leafHashes.nonEmpty) buildMerkleTree(leafHashes) else "empty_tree"

    MerkleProof(
      rootHash = rootHash,
      recordCount = recordCount,
      leafHashes = leafHashes,
      timestamp = Instant.now()
    )
  }

  /** Verifies that specific records have been deleted by comparing before/after Merkle proofs.
   *
   * @param beforeData DataFrame before deletion
   * @param afterData DataFrame after deletion
   * @param deletedIds Sequence of IDs that should have been deleted
   * @param columns Columns used in leaf hash creation
   * @param idColumn Primary key column
   * @return DeletionProof with cryptographic evidence of deletion
   */
  def verifyDeletion(
                      beforeData: DataFrame,
                      afterData: DataFrame,
                      deletedIds: Seq[String],
                      columns: Seq[String],
                      idColumn: String
                    ): DeletionProof = {

    val beforeProof = createMerkleProof(beforeData, columns, idColumn)
    val afterProof = createMerkleProof(afterData, columns, idColumn)

    // Get hashes of deleted records for verification
    val deletedRecordHashes = beforeData
      .filter(F.col(idColumn).isin(deletedIds: _*))
      .transform(df => createLeafHashes(df, columns, idColumn))
      .select("leaf_hash")
      .collect()
      .map(_.getString(0))
      .toSeq

    // Create simplified Merkle path proofs (in production, you'd want full paths)
    val merklePathProofs = deletedRecordHashes.map(hash =>
      s"proof_${sha256(hash + beforeProof.rootHash)}"
    )

    DeletionProof(
      beforeProof = beforeProof,
      afterProof = afterProof,
      deletedRecordHashes = deletedRecordHashes,
      merklePathProofs = merklePathProofs
    )
  }

  /** Validates a deletion proof by checking that the root hashes are different
   * and that the expected number of records were removed.
   *
   * @param deletionProof The proof to validate
   * @param expectedDeletions Number of records expected to be deleted
   * @return Boolean indicating if the deletion proof is valid
   */
  def validateDeletionProof(deletionProof: DeletionProof, expectedDeletions: Int): Boolean = {
    val DeletionProof(before, after, deletedHashes, _) = deletionProof

    // Check that root hashes are different (data changed)
    val rootHashesChanged = before.rootHash != after.rootHash

    // Check that record count decreased by expected amount
    val recordCountCorrect = (before.recordCount - after.recordCount) == expectedDeletions

    // Check that we have proof hashes for all deleted records
    val deletedHashesCorrect = deletedHashes.length == expectedDeletions

    // Verify none of the deleted record hashes appear in after state
    val deletedHashesRemoved = !after.leafHashes.exists(deletedHashes.contains)

    rootHashesChanged && recordCountCorrect && deletedHashesCorrect && deletedHashesRemoved
  }

  /** Combined analysis that creates both uniqueness analysis and retention proof.
   * Useful for comprehensive privacy and retention auditing.
   *
   * @param dataFrame The DataFrame to analyze
   * @param groupByColumns Columns for uniqueness analysis
   * @param userIdColumn User ID column
   * @param retentionColumns Columns to include in Merkle proof (may be different for privacy)
   * @param k Parameter for KHyperLogLog analysis
   * @return Tuple of (uniqueness analysis results, Merkle proof)
   */
  def combinedPrivacyAnalysis(
                               dataFrame: DataFrame,
                               groupByColumns: Seq[String],
                               userIdColumn: String,
                               retentionColumns: Seq[String],
                               k: Int = 2048
                             ): (DataFrame, MerkleProof) = {

    val uniquenessAnalysis = KHyperLogLogAnalyser.apply(
      dataFrame,
      groupByColumns,
      userIdColumn,
      k
    )

    val merkleProof = createMerkleProof(
      dataFrame,
      retentionColumns,
      userIdColumn
    )

    (uniquenessAnalysis, merkleProof)
  }
}