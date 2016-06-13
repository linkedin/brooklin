package com.linkedin.datastream.connectors.mysql;

/**
 * The string format of the checkpoint is
 * "[sourceIdStr]:[transactionId]:[fileNum]:[offset]"
 *
 * "[sourceIdStr]:[transactionId]" is the GTID of MySql
 * fileNum is the binlog file number
 * offset is the binlog offset WITHIN the binlog file
 */
public class MysqlCheckpoint {
  private static final String DELIMITER = ":";

  private final String _sourceIdStr;
  private final long _transactionId;
  private final String _binlogFileName;
  private final long _binlogOffset;
  private final String _checkpointStr;

  public static String createCheckpointString(String sourceIdStr, long transactionId, String binlogFileName,
      long binlogOffset) {
    return String.join(DELIMITER, sourceIdStr, String.valueOf(transactionId), binlogFileName,
        String.valueOf(binlogOffset));
  }

  public MysqlCheckpoint(String checkpointStr) {
    String[] elements = checkpointStr.split(DELIMITER);
    _sourceIdStr = elements[0];
    _transactionId = Long.parseLong(elements[1]);
    _binlogFileName = elements[2];
    _binlogOffset = Long.parseLong(elements[3]);
    _checkpointStr = checkpointStr;
  }

  /**
   * get the checkpoint string
   */
  public String getCheckpointStr() {
    return _checkpointStr;
  }

  /**
   * get the source id in string
   */
  public String getSourceIdStr() {
    return _sourceIdStr;
  }

  /**
   * get the transaction id
   */
  public long getTransactionId() {
    return _transactionId;
  }

  public String getBinlogFileName() {
    return _binlogFileName;
  }

  /**
   * get the offset within the binlog file
   */
  public long getBinlogOffset() {
    return _binlogOffset;
  }

  @Override
  public String toString() {
    return createCheckpointString(_sourceIdStr, _transactionId, _binlogFileName, _binlogOffset);
  }
}
