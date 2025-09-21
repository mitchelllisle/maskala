import org.apache.spark.sql.DataFrame
import org.mitchelllisle.analysers.MerkleTree
import org.scalatest.flatspec.AnyFlatSpec
import java.time.Instant

class MerkleTreeTest extends AnyFlatSpec with SparkFunSuite {
  import spark.implicits._

  val testData: DataFrame = Seq(
    ("user1", "alice@email.com", "25", "NYC"),
    ("user2", "bob@email.com", "30", "LA"),
    ("user3", "charlie@email.com", "35", "Chicago"),
    ("user4", "diana@email.com", "28", "NYC")
  ).toDF("user_id", "email", "age", "city")

  val columns = Seq("email", "age", "city")
  val idColumn = "user_id"

  "apply" should "be the standard way to create MerkleProof" in {
    val proof = MerkleTree.apply(testData, columns, idColumn)

    assert(proof.rootHash.nonEmpty)
    assert(proof.recordCount == 4)
    assert(proof.leafHashes.length == 4)
    assert(proof.timestamp.isInstanceOf[Instant])
    assert(proof.timestamp.isBefore(Instant.now().plusSeconds(1)))
  }

  "apply" should "produce same result as createMerkleProof" in {
    val applyProof = MerkleTree.apply(testData, columns, idColumn)
    val createProof = MerkleTree.createMerkleProof(testData, columns, idColumn)

    assert(applyProof.rootHash == createProof.rootHash)
    assert(applyProof.recordCount == createProof.recordCount)
    assert(applyProof.leafHashes == createProof.leafHashes)
  }

  "createLeafHashes" should "generate unique hashes for each record" in {
    val result = MerkleTree.createLeafHashes(testData, columns, idColumn)

    assert(result.columns.contains("leaf_hash"))
    assert(result.count() == 4)

    val hashes = result.select("leaf_hash").collect().map(_.getString(0))
    assert(hashes.distinct.length == 4) // All hashes should be unique
    assert(hashes.forall(_.length == 64)) // SHA-256 produces 64 char hex strings
  }

  "createMerkleProof" should "generate consistent root hash for same data" in {
    val proof1 = MerkleTree.createMerkleProof(testData, columns, idColumn)
    val proof2 = MerkleTree.createMerkleProof(testData, columns, idColumn)

    assert(proof1.rootHash == proof2.rootHash)
    assert(proof1.recordCount == 4)
    assert(proof1.leafHashes.length == 4)
  }

  "createMerkleProof" should "generate different root hash for different data" in {
    val modifiedData = testData.withColumn("age", $"age" + 1)

    val originalProof = MerkleTree.createMerkleProof(testData, columns, idColumn)
    val modifiedProof = MerkleTree.createMerkleProof(modifiedData, columns, idColumn)

    assert(originalProof.rootHash != modifiedProof.rootHash)
  }

  "createMerkleProof" should "handle empty DataFrame" in {
    val emptyData = testData.filter($"user_id" === "nonexistent")
    val proof = MerkleTree.createMerkleProof(emptyData, columns, idColumn)

    assert(proof.rootHash == "empty_tree")
    assert(proof.recordCount == 0)
    assert(proof.leafHashes.isEmpty)
  }

  "verifyDeletion" should "create valid deletion proof when records are deleted" in {
    val afterData = testData.filter($"user_id" =!= "user2")
    val deletedIds = Seq("user2")

    val deletionProof = MerkleTree.verifyDeletion(
      testData, afterData, deletedIds, columns, idColumn
    )

    assert(deletionProof.beforeProof.recordCount == 4)
    assert(deletionProof.afterProof.recordCount == 3)
    assert(deletionProof.beforeProof.rootHash != deletionProof.afterProof.rootHash)
    assert(deletionProof.deletedRecordHashes.length == 1)
  }

  "verifyDeletion" should "handle multiple deletions" in {
    val afterData = testData.filter(!$"user_id".isin("user1", "user3"))
    val deletedIds = Seq("user1", "user3")

    val deletionProof = MerkleTree.verifyDeletion(
      testData, afterData, deletedIds, columns, idColumn
    )

    assert(deletionProof.beforeProof.recordCount == 4)
    assert(deletionProof.afterProof.recordCount == 2)
    assert(deletionProof.deletedRecordHashes.length == 2)
    assert(deletionProof.merklePathProofs.length == 2)
  }

  "validateDeletionProof" should "return true for valid deletion proof" in {
    val afterData = testData.filter($"user_id" =!= "user4")
    val deletedIds = Seq("user4")

    val deletionProof = MerkleTree.verifyDeletion(
      testData, afterData, deletedIds, columns, idColumn
    )

    val isValid = MerkleTree.validateDeletionProof(deletionProof, expectedDeletions = 1)
    assert(isValid)
  }

  "validateDeletionProof" should "return false for invalid deletion proof" in {
    val afterData = testData.filter($"user_id" =!= "user4")
    val deletedIds = Seq("user4")

    val deletionProof = MerkleTree.verifyDeletion(
      testData, afterData, deletedIds, columns, idColumn
    )

    // Test with wrong expected deletion count
    val isValid = MerkleTree.validateDeletionProof(deletionProof, expectedDeletions = 2)
    assert(!isValid)
  }

  "validateDeletionProof" should "return false when no actual deletion occurred" in {
    val deletedIds = Seq("user4")

    // Create "fake" deletion proof where before and after data are the same
    val deletionProof = MerkleTree.verifyDeletion(
      testData, testData, deletedIds, columns, idColumn
    )

    val isValid = MerkleTree.validateDeletionProof(deletionProof, expectedDeletions = 1)
    assert(!isValid) // Should fail because root hashes are the same
  }

  "combinedPrivacyAnalysis" should "return both uniqueness analysis and Merkle proof" in {
    val (uniquenessResult, merkleProof) = MerkleTree.combinedPrivacyAnalysis(
      testData,
      groupByColumns = Seq("age", "city"),
      userIdColumn = idColumn,
      retentionColumns = columns
    )

    // Check uniqueness analysis result has expected columns
    val expectedColumns = Set("hll", "cumulativeValueCount", "cumulativeValueRatio", "estimatedValueCount", "estimatedValueRatio")
    assert(expectedColumns.subsetOf(uniquenessResult.columns.toSet))

    // Check Merkle proof is valid
    assert(merkleProof.recordCount == 4)
    assert(merkleProof.rootHash.nonEmpty)
    assert(merkleProof.rootHash != "empty_tree")
  }

  "Merkle tree" should "be deterministic with ordered input" in {
    val orderedData1 = testData.orderBy($"user_id")
    val orderedData2 = testData.orderBy($"user_id")

    val proof1 = MerkleTree.createMerkleProof(orderedData1, columns, idColumn)
    val proof2 = MerkleTree.createMerkleProof(orderedData2, columns, idColumn)

    assert(proof1.rootHash == proof2.rootHash)
  }

  "Merkle tree" should "detect single character changes" in {
    val originalData = Seq(("user1", "alice@email.com", "25", "NYC")).toDF("user_id", "email", "age", "city")
    val modifiedData = Seq(("user1", "alice@email.com", "26", "NYC")).toDF("user_id", "email", "age", "city") // Changed age 25->26

    val originalProof = MerkleTree.createMerkleProof(originalData, columns, idColumn)
    val modifiedProof = MerkleTree.createMerkleProof(modifiedData, columns, idColumn)

    assert(originalProof.rootHash != modifiedProof.rootHash)
  }

  "createLeafHashes" should "produce consistent hashes for same input" in {
    val result1 = MerkleTree.createLeafHashes(testData, columns, idColumn)
    val result2 = MerkleTree.createLeafHashes(testData, columns, idColumn)

    val hashes1 = result1.orderBy($"user_id").select("leaf_hash").collect().map(_.getString(0))
    val hashes2 = result2.orderBy($"user_id").select("leaf_hash").collect().map(_.getString(0))

    assert(hashes1.sameElements(hashes2))
  }

  "timestamp" should "use proper Instant type and be recent" in {
    val beforeTime = Instant.now().minusSeconds(1)
    val proof = MerkleTree.createMerkleProof(testData, columns, idColumn)
    val afterTime = Instant.now().plusSeconds(1)

    assert(proof.timestamp.isInstanceOf[Instant])
    assert(proof.timestamp.isAfter(beforeTime))
    assert(proof.timestamp.isBefore(afterTime))
  }
}