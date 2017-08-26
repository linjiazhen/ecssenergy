package com.wintoo.monitor;

import com.wintoo.model.AmCalData;
import com.wintoo.model.Energy;
import com.wintoo.model.EnergyKey;
import com.wintoo.model.EquipRel;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcOperations;

import java.math.BigDecimal;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Jason on 15/10/28.
 */
public class RealCalEnergy implements Runnable{

    private JdbcOperations jdbcTemplateMysql;

    SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    private int sequence=0;

    private Map<String,String> buildRel=new HashMap<>();
    private Map<String,String> buildOrganRel=new HashMap<>();
    private Map<String,String> energyType=new HashMap<>();
    private Map<String,String> buildType=new HashMap<>();
    private Map<String,List<EquipRel>> equipRels=new HashMap<>();
    private Map<EnergyKey,Integer> buildEnergy=new HashMap<>();
    private List<Energy> buildEnergyList=new ArrayList<>();
    private Map<EnergyKey,Integer> organEnergy=new HashMap<>();
    private List<Energy> organEnergyList=new ArrayList<>();
    private Map<String,String> buildTime=new HashMap<>();
    private Map<String,String> organTime=new HashMap<>();
    private long start=System.currentTimeMillis();

    public RealCalEnergy(JdbcOperations jdbcTemplateMysql){
        try {

            this.jdbcTemplateMysql=jdbcTemplateMysql;
            getBaseRel();

        } catch (Exception e) {
            // TODO: handle exception
            System.out.println(e);
        }
    }

    private void getBaseRel(){
        try {
            jdbcTemplateMysql.queryForList("select F_UUID as buildId,'allofsumgroup' as upbuildId from T_BD_GROUP WHERE F_STATUS='00A'" +
                    "UNION select F_BUILDID as buildId,F_BUILDGROUPID as upbuildId from T_BD_GROUPBUILDRELA WHERE F_STATUS='00A'" +
                    "UNION SELECT F_UUID as buildId,F_BUILDID as upbuildId FROM T_BD_FLOOR WHERE F_STATUS='00A'" +
                    "UNION SELECT F_UUID as buildId,F_FLOORID as upbuildId FROM T_BD_ROOM WHERE F_STATUS='00A'").forEach(map-> buildRel.put((String) map.get("buildid"), (String) map.get("upbuildid")));
            buildRel.put("allofsumgroup","#");

            jdbcTemplateMysql.queryForList("SELECT F_BUILDID,F_ORGANID from T_RR_ORGANBUILDRELATION WHERE F_STATUS='00A' ORDER BY F_BUILDID").forEach(map->{
                String key=(String)map.get("F_BUILDID");
                if(buildOrganRel.containsKey(key)){
                    buildOrganRel.put(key,buildOrganRel.get(key)+","+map.get("F_ORGANID"));
                }
                else
                    buildOrganRel.put(key, (String )map.get("F_ORGANID"));
            });

            jdbcTemplateMysql.queryForList("select F_UUID,F_BUILDFUNC from T_BD_BUILD WHERE F_STATUS='00A'").forEach(map->
                    buildType.put((String) map.get("F_UUID"), (String) map.get("F_BUILDFUNC"))
            );

            jdbcTemplateMysql.queryForList("select F_ENERGYITEMCODE,F_PARENTITEMCODE from T_DT_ENERGYITEMDICT ").forEach(map->
                    energyType.put((String) map.get("F_ENERGYITEMCODE"), (String) map.get("F_PARENTITEMCODE"))
            );

            jdbcTemplateMysql.queryForList("select F_DEVICECODE,F_SUPERIOR_METER,F_ENERGYITEMCODE,F_LEVEL," +
                    "decode(F_LEVEL,0,F_ROOMID,1,F_FLOORID,2,F_BUILDCODE,3,F_BUILDGROUPID) as buildid,F_PERCENT from T_RR_DEVICERELATION WHERE F_STATUS='00A'").forEach(map->{
                EquipRel equipRel=new EquipRel();
                String equipid=(String) map.get("F_DEVICECODE");
                equipRel.setEquipid(equipid);
                equipRel.setUpequipid((String) map.get("F_SUPERIOR_METER"));
                equipRel.setEnergytype((String) map.get("F_ENERGYITEMCODE"));
                equipRel.setBuildid((String )map.get("buildid"));
                equipRel.setPercent((BigDecimal) map.get("F_PERCENT"));
                equipRel.setBuildlevel((BigDecimal) map.get("F_LEVEL"));
                if(!equipRels.containsKey(equipid))
                    equipRels.put(equipid, new ArrayList());
                equipRels.get(equipid).add(equipRel);
            });

            jdbcTemplateMysql.queryForList("select F_BUILDID,F_ENERGYITEMCODE,DATE_FORMAT(max(F_STARTTIME),'%Y/%m/%d %T') as datetime from T_EC_BUILD_15 group by F_BUILDID,F_ENERGYITEMCODE").forEach(map->
                    buildTime.put((String)map.get("F_BUILDID")+map.get("F_ENERGYITEMCODE"), (String)map.get("datetime"))
            );

            jdbcTemplateMysql.queryForList("select F_ORGANID,F_ENERGYITEMCODE,DATE_FORMAT(max(F_STARTTIME),'%Y/%m/%d %T') as datetime from T_EC_ORGAN_15 group by F_ORGANID,F_ENERGYITEMCODE").forEach(map->
                    organTime.put((String) map.get("F_ORGANID")+map.get("F_ENERGYITEMCODE"), (String)map.get("datetime"))
            );

            jdbcTemplateMysql.queryForList("SELECT F_SEQUENCE FROM T_CC_CALPRA").forEach(map-> sequence=(Integer) map.get("F_SEQUENCE"));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void getCalData(int num){
        jdbcTemplateMysql.queryForList("select F_ID,F_DEVICECODE,F_TIME_INTERVEL_ACTIVE,DATE_FORMAT(max(F_DATATIME),'%Y/%m/%d %T') as datetime from T_EA_15_ENERGY_BUFFER " +
                    "where F_TYPE=1 and F_ID > ? limit ?",new Object[]{sequence,num}).forEach(map->{
            AmCalData amCalData=new AmCalData();
            amCalData.setEquipid((String )map.get("F_DEVICECODE"));
            amCalData.setDatetime((String) map.get("datetime"));
            amCalData.setValue((BigDecimal) map.get("F_TIME_INTERVEL_ACTIVE"));
            sequence=(Integer) map.get("F_ID");
            calBuild15Data(amCalData);
        });
        calOrgan15Data();
    }

    private void calBuild15Data(AmCalData amCalData){
        try {
            Calendar calendar=Calendar.getInstance();
            calendar.setTime(sdf1.parse(amCalData.getDatetime()));
            calendar.add(Calendar.MINUTE, 15);
            String endtime=sdf1.format(calendar.getTime());
            String starttime=amCalData.getDatetime();
            List<EquipRel> equipRelList=equipRels.get(amCalData.getEquipid());
            for (int i=0;equipRelList!=null&&i<equipRelList.size();i++){
                BigDecimal buildlevel=equipRelList.get(i).getBuildlevel();
                BigDecimal value=equipRelList.get(i).getPercent().multiply(amCalData.getValue()).divide(new BigDecimal(100.0),2,BigDecimal.ROUND_HALF_UP);
                String buildid=equipRelList.get(i).getBuildid();
                while(buildid!=null&&!buildid.equals("#")){
                    String energytype=equipRelList.get(i).getEnergytype();
                    while(energytype!=null&&!energytype.equals("#")){
                        Energy bdEnergy;
                        String build;
                        if(buildlevel.equals(new BigDecimal(2))&&buildType.get(buildid)!=null){
                            build=buildType.get(buildid);
                        }
                        else build=buildid;
                        EnergyKey key=new EnergyKey();
                        key.setId(build);
                        key.setEnergytype(energytype);
                        key.setDatetime(starttime);
                        if(!buildEnergy.containsKey(key)) {
                            bdEnergy = new Energy();
                            bdEnergy.setId(build);
                            bdEnergy.setEnergytype(energytype);
                            bdEnergy.setStarttime(starttime);
                            bdEnergy.setEndtime(endtime);
                            bdEnergy.setValue(new BigDecimal(0));
                            bdEnergy.setBuildlevel(buildlevel);
                            buildEnergyList.add(bdEnergy);
                            buildEnergy.put(key, buildEnergyList.size() - 1);
                        }
                        bdEnergy=buildEnergyList.get(buildEnergy.get(key));
                        bdEnergy.setValue(bdEnergy.getValue().add(value));
                        energytype=energyType.get(energytype);
                    }
                    buildid=buildRel.get(buildid);
                    buildlevel=buildlevel.add(new BigDecimal(1));
                    if(buildlevel.equals(new BigDecimal(2)))
                        break;
                }
            }
        }catch (ParseException e){
            e.printStackTrace();
        }
    }
    private void calOrgan15Data(){
        for (Energy bdEnergy : buildEnergyList) {
            if(buildOrganRel.containsKey(bdEnergy.getId())) {
                String[] organs = buildOrganRel.get(bdEnergy.getId()).split(",");
                for (String organ : organs) {
                    EnergyKey key = new EnergyKey();
                    key.setId(organ);
                    key.setEnergytype(bdEnergy.getEnergytype());
                    key.setDatetime(bdEnergy.getStarttime());
                    if (!organEnergy.containsKey(key)) {
                        Energy onEnergy = new Energy();
                        onEnergy.setId(organ);
                        onEnergy.setEnergytype(bdEnergy.getEnergytype());
                        onEnergy.setStarttime(bdEnergy.getStarttime());
                        onEnergy.setEndtime(bdEnergy.getEndtime());
                        onEnergy.setValue(new BigDecimal(0));
                        organEnergyList.add(onEnergy);
                        organEnergy.put(key, organEnergyList.size() - 1);
                    }
                    Energy onEnergy = organEnergyList.get(organEnergy.get(key));
                    onEnergy.setValue(onEnergy.getValue().add(bdEnergy.getValue()));
                }
            }
        }
    }
    public void saveBuildEnergy() throws SQLException {
        for (Energy bdEnergy : buildEnergyList) {
            String time=buildTime.get(bdEnergy.getId()+bdEnergy.getEnergytype());
            if(time==null||bdEnergy.getStarttime().compareTo(time)>0){
                jdbcTemplateMysql.batchUpdate("insert into T_EC_BUILD_15(F_RESULTID,F_BUILDID,F_ENERGYITEMCODE,F_STARTTIME,F_ENDTIME,F_VALUE,F_EQUVALUE,F_BUILDLEVEL) " +
                        "VALUES(sys_guid(),?,?,?,?,?,?,?)", new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                        preparedStatement.setString(1,bdEnergy.getId());
                        preparedStatement.setString(2,bdEnergy.getEnergytype());
                        preparedStatement.setString(3,bdEnergy.getStarttime());
                        preparedStatement.setString(4,bdEnergy.getEndtime());
                        preparedStatement.setBigDecimal(5,bdEnergy.getValue());
                        preparedStatement.setBigDecimal(7,bdEnergy.getBuildlevel());
                    }
                    @Override
                    public int getBatchSize() {
                        return buildEnergyList.size();
                    }
                });
                buildTime.put(bdEnergy.getId()+bdEnergy.getEnergytype(),bdEnergy.getStarttime());
            }else{
                jdbcTemplateMysql.batchUpdate("update T_EC_BUILD_15 SET F_VALUE=+F_VALUE+?,F_EQUVALUE=F_EQUVALUE+? WHERE F_STARTTIME=? and F_BUILDID=? and F_ENERGYITEMCODE=?", new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                        preparedStatement.setBigDecimal(1,bdEnergy.getValue());
                        preparedStatement.setString(3,bdEnergy.getStarttime());
                        preparedStatement.setString(4,bdEnergy.getId());
                        preparedStatement.setString(5,bdEnergy.getEnergytype());
                    }
                    @Override
                    public int getBatchSize() {
                        return buildEnergyList.size();
                    }
                });
            }
        }
    }

    private void saveOrganEnergy() throws SQLException {
        for (Energy bdEnergy : buildEnergyList) {
            String time=buildTime.get(bdEnergy.getId()+bdEnergy.getEnergytype());
            if(time==null||bdEnergy.getStarttime().compareTo(time)>0){
                jdbcTemplateMysql.batchUpdate("insert into T_EC_ORGAN_15(F_RESULTID,F_ORGANID,F_ENERGYITEMCODE,F_STARTTIME,F_ENDTIME,F_VALUE,F_EQUVALUE) " +
                        "VALUES(?,?,?,?,?,?)", new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                        preparedStatement.setString(1,bdEnergy.getId());
                        preparedStatement.setString(2,bdEnergy.getEnergytype());
                        preparedStatement.setString(3,bdEnergy.getStarttime());
                        preparedStatement.setString(4,bdEnergy.getEndtime());
                        preparedStatement.setBigDecimal(5,bdEnergy.getValue());
                    }
                    @Override
                    public int getBatchSize() {
                        return buildEnergyList.size();
                    }
                });
                buildTime.put(bdEnergy.getId()+bdEnergy.getEnergytype(),bdEnergy.getStarttime());
            }else{
                jdbcTemplateMysql.batchUpdate("update T_EC_ORGAN_15 SET F_VALUE=F_VALUE+?,F_EQUVALUE=F_EQUVALUE+? WHERE F_STARTTIME=? and F_ORGANID=? and F_ENERGYITEMCODE=?", new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                        preparedStatement.setBigDecimal(1,bdEnergy.getValue());
                        preparedStatement.setString(3,bdEnergy.getStarttime());
                        preparedStatement.setString(4,bdEnergy.getId());
                        preparedStatement.setString(5,bdEnergy.getEnergytype());
                    }
                    @Override
                    public int getBatchSize() {
                        return buildEnergyList.size();
                    }
                });
            }
        }
    }
    public void run(){
        try {
            System.out.println("Start sequence:"+sequence);
            getCalData(5000);
            saveBuildEnergy();
            saveOrganEnergy();
            jdbcTemplateMysql.update("UPDATE T_CC_CALPRA SET F_SEQUENCE="+sequence);
            buildEnergy.clear();
            buildEnergyList.clear();
            organEnergy.clear();
            organEnergyList.clear();
            System.out.println("One time:"+(System.currentTimeMillis()-start)+"! sequence:"+sequence);
            start=System.currentTimeMillis();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
