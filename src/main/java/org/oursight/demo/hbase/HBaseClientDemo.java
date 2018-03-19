package org.oursight.demo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * see:
 * http://www.cnblogs.com/ggjucheng/p/3381328.html
 */
public class HBaseClientDemo {

  static HBaseAdmin admin = null;
  Configuration conf = null;

  public HBaseClientDemo() throws IOException {
    conf = HBaseConfiguration.create();
//    conf.set("hbase.zookeeper.property.clientPort", "2181");
//    conf.set("hbase.zookeeper.quorum", "10.101.170.133");
//    conf.set("zookeeper.znode.parent", "/hbase");

    System.out.println("about to connect to : " + conf);
    Connection conn = ConnectionFactory.createConnection(conf);
    admin = (HBaseAdmin) conn.getAdmin();
//    admin = new HBaseAdmin(conf);
  }



  private void createTable(String tableName) throws IOException {
    createTable(tableName, null, null);
  }

  private void createTable(String tableName, String family1,
                           String family2) throws IOException {
    if (admin.tableExists(tableName)) {
      System.out.println(tableName + "表已存在");
    } else {
      System.out.println("start to create table");
      HTableDescriptor hTableDescriptor = new HTableDescriptor(
              TableName.valueOf(tableName));
      if (family1 != null)
        hTableDescriptor.addFamily(new HColumnDescriptor(family1));
      if (family2 != null)
        hTableDescriptor.addFamily(new HColumnDescriptor(family2));
      admin.createTable(hTableDescriptor);
      System.out.println(tableName + "表创建成功！");
    }
  }

  /**
   * 往表中添加一条数据
   *
   * @param tableName
   * @param rowkey
   * @param family
   * @param qualifier
   * @param value~~~~~~~
   */
  private void addOneData(String tableName, String rowkey, String family,
                          String qualifier, String value) throws IOException {
//    HTablePool hTablePool = new HTablePool(conf, 1000);
//    HTableInterface table = hTablePool.getTable(tableName);
    Connection conn = ConnectionFactory.createConnection(conf);
    Table table = conn.getTable(TableName.valueOf(tableName));

    Put put = new Put(rowkey.getBytes());
    put.add(family.getBytes(), qualifier.getBytes(), value.getBytes());
    try {
      table.put(put);
      System.out.println("记录" + rowkey + "添加成功！");
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("记录" + rowkey + "添加失败！");
    }
  }


  /**
   * 查询所有记录
   *
   * @param tableName
   */
  private void getAllData(String tableName) {
    try {
//      HTable hTable = new HTable(conf, tableName);
      Connection conn = ConnectionFactory.createConnection(conf);
      Table table = conn.getTable(TableName.valueOf(tableName));

      Scan scan = new Scan();
      ResultScanner scanner = table.getScanner(scan);
      for (Result result : scanner) {
        if (result.raw().length > 0) {
          for (KeyValue kv : result.raw()) {

            System.out.println(new String(kv.getRow()) + "\t"
                    + new String(kv.getValue()));
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  /**
   * 查询所有记录
   *
   * @param tableName
   */
  private void getAllData1(String tableName) {
    try {
//      HTable hTable = new HTable(conf, tableName);
      Connection conn = ConnectionFactory.createConnection(conf);
      Table table = conn.getTable(TableName.valueOf(tableName));

      Scan scan = new Scan();
      ResultScanner scanner = table.getScanner(scan);
      for (Result result : scanner) {
        if (result.raw().length > 0) {
          /**
           * 这里用的都是keyValue里面旧的方法来获取行键，列族，列和值
           * 新的方法后面都有个Array，但是显示出来中间总有乱码，
           * 我猜测是时间戳在中间，但不知道怎么解析。
           * 以后再来解决
           */
          for (KeyValue keyValue : result.raw()) {

            System.out.println("row:" + new String(keyValue.getRow()) +
                    "\tcolumnfamily:" + new String(keyValue.getFamily()) +
                    "\tcolumn:" + new String(keyValue.getQualifier()) +
                    "\tvalue:" + new String(keyValue.getValue()));
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  /**
   * 获取所有的列表
   */
  private void getAllTables() {
    if (admin != null) {
      try {
        HTableDescriptor[] listTables = admin.listTables();
        if (listTables.length > 0) {
          for (HTableDescriptor hTableDescriptor : listTables) {
            System.out.println(hTableDescriptor.getNameAsString());
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void getOneRecord(String tableName, String rowkey) {
    HTablePool hTablePool = new HTablePool(conf, 1000);
    HTableInterface table = hTablePool.getTable(tableName);
    Get get = new Get(rowkey.getBytes());
    try {
      Result result = table.get(get);
      if (result.raw().length > 0) {
        for (KeyValue kv : result.raw()) {
          /*
					 * System.out.println(new String(kv.getRow()) + "\t" + new
					 * String(kv.getValue()));
					 */

          System.out.println(new Long(kv.getTimestamp()).toString());
          System.out.println(Bytes.toString(kv.getFamily()));
          System.out.println(Bytes.toString(kv.getQualifier()));
//          System.out.println(CellUtil.get);
//          System.out.println(Bytes.toString(kv.getKey()));
          System.out.println(Bytes.toString(kv.getValue()));

          System.out.println();
          System.out.println();

          System.out.println("family:" + Bytes.toString(kv.getFamily()));
          System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
          System.out.println("value:" + Bytes.toString(kv.getValue()));
          System.out.println("Timestamp:" + kv.getTimestamp());

//          System.out.print(new String(kv.get) + "\t");

//          System.out.println(new String(kv.ge.getRowArray()) + "\t");
          // + new String(kv.getValueArray()));
					/*
					 * System.out.println(new String(kv.getKey()) + "\t" + new
					 * String(kv.getValue()));
					 */
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public  void getOneRecordAllVersion(String tableName, String rowKey, String familyName, String columnName) throws IOException {
    HTable table = new HTable(conf, Bytes.toBytes(tableName));
    Get get = new Get(Bytes.toBytes(rowKey));
    get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
    get.setMaxVersions(10);
    Result result = table.get(get);
    for (KeyValue kv : result.list()) {
      System.out.println("family:" + Bytes.toString(kv.getFamily()));
      System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
      System.out.println("value:" + Bytes.toString(kv.getValue()));
      System.out.println("Timestamp:" + DateUtil.timeMillisToString(kv.getTimestamp()));
      System.out.println();
//      System.out.println("-------------------------------------------");
    }
        /*
         * List<?> results = table.get(get).list(); Iterator<?> it =
         * results.iterator(); while (it.hasNext()) {
         * System.out.println(it.next().toString()); }
         */
  }

  /**
   * 删除一条记录
   */
  private void deleteAllOnedata(String tableName, String rowkey) {
    HTablePool hTablePool = new HTablePool(conf, 1000);
    HTableInterface table = hTablePool.getTable(tableName);
    Delete delete = new Delete(rowkey.getBytes());
    try {
      table.delete(delete);
      System.out.println(rowkey + " 记录删除成功！");
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println(rowkey + " 记录删除失败！");
    }
  }

  /**
   * 删除一条记录的一个值
   *
   * @param tableName
   * @param rowkey
   * @param family
   * @param qualifier
   */
  private void deleteOneRcord(String tableName, String rowkey, String family,
                              String qualifier) {
    HTablePool hTablePool = new HTablePool(conf, 1000);
    HTableInterface table = hTablePool.getTable(tableName);
    Delete delete = new Delete(rowkey.getBytes());
    delete.deleteColumn(family.getBytes(), qualifier.getBytes());
    try {
      table.delete(delete);
      System.out.println(tableName + " " + rowkey + "," + family + ":"
              + qualifier + "值删除成功！");
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println(tableName + " " + rowkey + "," + family + ":"
              + qualifier + "值删除失败s！");
    }

  }

  /**
   * 删除一张表
   *
   * @param tableName
   */
  private void deleteTable(String tableName) {
    if (admin != null) {
      try {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println(tableName + "表删除成功！");
      } catch (IOException e) {
        e.printStackTrace();
        System.out.println(tableName + "表删除失败！");
      }
    }
  }

  public  void getOneRecordByColumn(String tableName, String rowKey,
                                       String familyName, String columnName) throws IOException {
    HTable table = new HTable(conf, Bytes.toBytes(tableName));
    Get get = new Get(Bytes.toBytes(rowKey));
    get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
    Result result = table.get(get);
    for (KeyValue kv : result.list()) {
      System.out.println("family:" + Bytes.toString(kv.getFamily()));
      System.out
              .println("qualifier:" + Bytes.toString(kv.getQualifier()));
      System.out.println("value:" + Bytes.toString(kv.getValue()));
      System.out.println("Timestamp:" + DateUtil.timeMillisToString(kv.getTimestamp()));
      System.out.println("-------------------------------------------");
    }
  }

  public static void main(String[] args) throws IOException {
    String testTableName = "test_tb";
    TableName tableName = TableName.valueOf(testTableName);

    HBaseClientDemo client = new HBaseClientDemo();
//    client.createTable(testTableName, "f1", "f2");
//    client.addOneData(testTableName, "row_url1", "f1", "html", "<html>111</html>");
//    client.addOneData(testTableName, "row_url2", "f1", "html", "<html>2222</html>");
//    client.getAllData(testTableName);

//    client.getOneRecord("test_tb", "row1");
    client.getOneRecordAllVersion("test_tb", "row1", "cf", "a");
//    client.getOneRecordByColumn("test_tb", "row1", "cf", "a");
//    client.getAllTables();
  }

}
