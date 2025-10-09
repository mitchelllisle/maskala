# Privacy Engineering Enhancements - Summary

## Overview
This PR adds significant privacy engineering capabilities to Maskala, making it a more comprehensive toolkit for data engineers working with sensitive data.

## New Features

### 1. PrivacyRiskScorer
A comprehensive privacy risk assessment tool that helps identify individuals at risk of re-identification.

**Key capabilities:**
- **Risk Score Calculation**: Combines uniqueness and k-anonymity metrics into a single risk score (0-1 scale)
- **Risk Classification**: Automatically categorizes individuals as CRITICAL, HIGH, MEDIUM, or LOW risk
- **High-Risk Identification**: Quickly find individuals requiring additional privacy protection
- **Privacy Standards Validation**: Check if datasets meet organizational privacy thresholds
- **Detailed Risk Metrics**: Calculate prosecutor, journalist, and marketer re-identification risks

**Use cases:**
- Pre-publication privacy assessment
- Identifying which records need additional anonymization
- Compliance validation before data release
- Privacy-aware data sampling

### 2. RetentionAnalyser
Helps organizations comply with data retention requirements (GDPR, CCPA, etc.).

**Key capabilities:**
- **Retention Period Identification**: Find records exceeding configurable retention periods
- **Grace Period Support**: Provide warning periods before actual deletion
- **Multi-Date Policies**: Smart retention based on multiple date columns (keeps if ANY is recent)
- **Retention Summaries**: Generate detailed reports for audit compliance

**Use cases:**
- GDPR "Right to be Forgotten" compliance
- Automated data lifecycle management
- Compliance reporting and auditing
- Data minimization strategies

### 3. Enhanced Documentation
- Complete privacy engineering workflow guide showing how to use features together
- Updated README with new capabilities prominently featured
- Practical examples demonstrating real-world usage
- End-to-end workflow example code

## Implementation Details

### Files Added
- `src/main/scala/org/mitchelllisle/analysers/PrivacyRiskScorer.scala` (225 lines)
- `src/main/scala/org/mitchelllisle/analysers/RetentionAnalyser.scala` (187 lines)
- `src/test/scala/PrivacyRiskScorerTest.scala` (264 lines)
- `src/test/scala/RetentionAnalyserTest.scala` (154 lines)
- `examples/PrivacyWorkflowExample.scala` (103 lines)

### Files Modified
- `README.md` - Comprehensive documentation updates

### Testing
- **23 new tests added** (9 for RetentionAnalyser, 14 for PrivacyRiskScorer)
- **All 70 tests passing** (original 47 + new 23)
- Tests cover:
  - Correct risk score calculation
  - Retention policy logic
  - Edge cases (empty data, single records, uniform data)
  - Integration scenarios

## Technical Approach

### PrivacyRiskScorer Algorithm
```
Risk Score = (K-Anonymity Score * 0.6) + (Uniqueness Score * 0.4)

Where:
- K-Anonymity Score: Normalized based on group size (1=unique, 0=large group)
- Uniqueness Score: Inverse of group size
- Risk Level: CRITICAL (≥0.75), HIGH (≥0.5), MEDIUM (≥0.25), LOW (<0.25)
```

### RetentionAnalyser Logic
```
Retention Status = 
  - FOR_DELETION: date < (current_date - retention_days - grace_period_days)
  - GRACE_PERIOD: date < (current_date - retention_days) AND not FOR_DELETION
  - RETAINED: All other records
```

## Privacy Engineering Workflow

The new features enable a complete privacy engineering workflow:

1. **Assess** → Use PrivacyRiskScorer to evaluate re-identification risk
2. **Identify** → Use RetentionAnalyser to find records for deletion
3. **Delete** → Remove records based on retention policies
4. **Verify** → Use MerkleTree to cryptographically prove deletion
5. **Protect** → Apply anonymization to high-risk records

## Benefits for Data Engineers

1. **Comprehensive Privacy Assessment**: No need to manually calculate risk metrics
2. **Automated Compliance**: Built-in support for GDPR/CCPA retention requirements
3. **Audit Trail**: Cryptographic proofs for regulatory compliance
4. **Production Ready**: Tested, documented, and ready for production use
5. **Spark Native**: Leverages DataFrame API for scalable processing

## Example Usage

See `examples/PrivacyWorkflowExample.scala` for a complete end-to-end example demonstrating:
- Privacy risk assessment
- Retention policy application
- Cryptographic deletion verification
- Risk reporting

## Compatibility

- **Scala Version**: 2.12.20
- **Spark Version**: 3.5.6
- **Dependencies**: Uses only standard Spark SQL functions
- **Backwards Compatible**: No breaking changes to existing APIs
