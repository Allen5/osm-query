package org.yunqiacademy.centralsystem.plugins.service;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
@Slf4j
public class OdpsServiceImpl implements OdpsService {


    //参数为城市名,经纬度信息
    @Override
    public List<Map<String, Object>> getOdpsData(String cityName, Double minLongitude, Double maxLongitude,
                                                 Double minLatitude, Double maxLatitude) {
        Odps odps = getOdps("eCWJKNSgOAwj0Vr7", "Jp3eplgHYOwKndLh4E9x6322QpzywW",
                "http://172.26.14.82:80/api", "nsodps_dev");

        List<Map<String, Object>> result = new ArrayList<>();
        try {

            List<String> dropSql = new ArrayList<>();
            //删除之前的临时表   临时表一天后会删除
            dropSql.add("drop table " + cityName + "_node_tmp;");
            dropSql.add("drop table " + cityName + "_edge_tmp1;");
            dropSql.add("drop table " + cityName + "_edge_tmp2;");

            executeSql(odps, dropSql);

            //重新创建临时表
            List<String> createSql = new ArrayList<>();
            createSql.add(" create table " + cityName + "_node_tmp(osmid BIGINT, x FLOAT, y FLOAT);");
            createSql.add(" create table " + cityName + "_edge_tmp1(osmid BIGINT, osmid_start BIGINT,osmid_end BIGINT);");
            createSql.add(" create table " + cityName + "_edge_tmp2(osmid BIGINT, highway STRING);");

            executeSql(odps, createSql);

            List<String> selectSql = new ArrayList<>();
            selectSql.add("insert into " + cityName + "_node_tmp select osmid,x,y from osm_node where x> " + minLongitude + " and x<" + maxLongitude + " and y>" + minLatitude + " and y<" + maxLatitude + ";");
            selectSql.add("insert into " + cityName + "_edge_tmp1 select osmid,osmid_start,osmid_end from osm_split_edge where osmid_start in (select osmid from " + cityName + "_node_tmp) and osmid_end in (select osmid from " + cityName + "_node_tmp);");
            selectSql.add("insert into " + cityName + "_edge_tmp2 select osmid,highway from osm_fulltag_edge where osmid in (select osmid from " + cityName + "_edge_tmp1);");

            //插入 搜索符合条件的数据
            executeSql(odps, selectSql);

            //下载数据
            TableTunnel tunnel = new TableTunnel(odps);

            //所有的表名
            List<String> tableName = new ArrayList<>();
            tableName.add(cityName + "_node_tmp");
            tableName.add(cityName + "_edge_tmp1");
            tableName.add(cityName + "_edge_tmp2");

            //导出数据
            for (String name : tableName) {
                TableTunnel.DownloadSession downloadSession = tunnel.createDownloadSession("nsodps_dev", name, false);
                long recordCount = downloadSession.getRecordCount();

                TunnelRecordReader reader = downloadSession.openRecordReader(0, recordCount);

                Record record;
                while ((record = reader.read()) != null) {
                    Map<String, Object> map = consumeRecord(record, downloadSession.getSchema());
                    result.add(map);
                }
                //TODO 关闭TunnelRecordReader
                reader.close();
            }
        } catch (Exception e) {
            //TODO 异常处理
            throw new RuntimeException(e);
        }
        return result;
    }


    public Odps getOdps(String accessId, String accessKey, String endpoint, String project) {
        Account account = new AliyunAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        odps.setEndpoint(endpoint);
        odps.setDefaultProject(project);
        return odps;
    }

    private void executeSql(Odps odps, List<String> sqlList) {
        for (String sql : sqlList) {
            try {
                Instance dropInstance = SQLTask.run(odps, sql);
                dropInstance.waitForSuccess();
            } catch (OdpsException e) {
                log.error("sql task execute filed. ");
                throw new RuntimeException(e);
            }
        }
    }

    private Map<String, Object> consumeRecord(Record record, TableSchema schema) {
        Map<String, Object> result = new HashMap<>();
        for (int i = 0; i < schema.getColumns().size(); i++) {
            Column column = schema.getColumn(i);
            String colValue = null;
            switch (column.getType()) {
                case BIGINT: {
                    Long v = record.getBigint(i);
                    colValue = v == null ? null : v.toString();
                    break;
                }
                case BOOLEAN: {
                    Boolean v = record.getBoolean(i);
                    colValue = v == null ? null : v.toString();
                    break;
                }
                case DATETIME: {
                    Date v = record.getDatetime(i);
                    colValue = v == null ? null : v.toString();
                    break;
                }
                case DOUBLE: {
                    Double v = record.getDouble(i);
                    colValue = v == null ? null : v.toString();
                    break;
                }
                case STRING: {
                    String v = record.getString(i);
                    colValue = v == null ? null : v.toString();
                    break;
                }
                case FLOAT: {
                    Float v = (Float) record.get(i);
                    colValue = v == null ? null : v.toString();
                    break;
                }
                default:
                    throw new RuntimeException("Unknown column type: "
                            + column.getType());
            }
            log.info("column: " + column.getName() + ", value: " + colValue);
            result.put(column.getName(), colValue);
        }
        return result;
    }
}
