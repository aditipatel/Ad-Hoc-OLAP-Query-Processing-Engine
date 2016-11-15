import java.util.*;
import java.lang.reflect.*;
import java.sql.*;

public class QueryProcessor1{

class MFStructure{
public String cust;
public int prod;
public int avg;
public int quant_NY;
public int avg_quant_NY;
public int avg_quant_CT;
public int avg_quant_NJ;

public String printHeader() throws Exception{
Field[] allFields = MFStructure.class.getDeclaredFields();
StringBuffer sbuf = new StringBuffer();
for (Field field : allFields) {
if(!field.getName().startsWith("this")){
sbuf.append(String.format("%-18s",field.getName()));
}//end of if
}//end of for
return sbuf.toString();
}

public String print() throws Exception{
Field[] allFields = MFStructure.class.getDeclaredFields();
StringBuffer sbuf = new StringBuffer();
for (Field field : allFields) {
if(!field.getName().startsWith("this")){
sbuf.append(String.format("%-18s",field.get(this)));
}//end of if
}//end of for
return sbuf.toString();
}
}

private Connection getDBConnection() throws Exception{
String usr = "postgres";
String pwd = "bhavin";
String url = "jdbc:postgresql://localhost:5432/postgres";
Class.forName("org.postgresql.Driver"); 
Connection con = DriverManager.getConnection(url, usr, pwd);
return con;
}

private class Sales{
private String cust;
private String prod;
private Integer day;
private Integer month;
private Integer year;
private String state;
private Integer quant;

public Object getValue(String name){
if("cust".equals(name))
return cust;
else if("prod".equals(name))
return prod;
else if("day".equals(name))
return day;
else if("month".equals(name))
return month;
else if("year".equals(name))
return year;
else if("state".equals(name))
return state;
else if("quant".equals(name))
return quant;
return null;
}
}

private List<Sales> getSalesData() throws Exception{
Connection con = getDBConnection();
Statement st = (Statement)con.createStatement();
String salesQuery = "select * from sales";
 ResultSet rs;
rs = st.executeQuery(salesQuery);
List<Sales> salesList = new ArrayList<Sales>();
Sales salesObj = null;
while (rs.next()){
salesObj = new Sales();
salesObj.cust = rs.getString("cust");
salesObj.prod = rs.getString("prod");
salesObj.day = rs.getInt("day");
salesObj.month = rs.getInt("month");
salesObj.year = rs.getInt("year");
salesObj.state = rs.getString("state");
salesObj.quant = rs.getInt("quant");
salesList.add(salesObj);
}
return salesList;
}


private int evaluate(String groupFunction,int currVal,int newVal, int count){
if(groupFunction.equals("avg") || groupFunction.equals("sum"))
 return currVal+newVal;
else if(groupFunction.equals("min"))
 return (newVal < currVal && currVal != 0) ? newVal : (currVal == 0) ? newVal : currVal;
else if(groupFunction.equals("max"))
 return (newVal > currVal) ? newVal : currVal;
else if(groupFunction.equals("count"))
 return count;
 else
return 0;
}


private List<MFStructure> populateMFStructure(List<Sales> salesList,boolean isGroupZeroPresent, String selectAttb,String groupingAttb,String whereClause)throws Exception{
String[] selectAttbArr = selectAttb.split(",");
String[] groupingAttbArr = groupingAttb.split(",");
Map<String,MFStructure> mfMap = new HashMap<String,MFStructure>();
Map<String,Integer> mfCountMap = new HashMap<String,Integer>();
String groupFunction = null, groupColumn = null, groupAttb = null;
if(isGroupZeroPresent){
groupAttb = selectAttbArr[groupingAttbArr.length];
groupFunction = groupAttb.substring(0,groupAttb.indexOf("_"));
groupColumn = groupAttb.substring(groupAttb.indexOf("_")+1);
}
MFStructure mf = null;
 Class<MFStructure> mfClass = MFStructure.class;
for(Sales s : salesList){
 if(!"null".equals(whereClause)){
String whereColumn = whereClause.substring(whereClause.indexOf('{')+1,whereClause.indexOf('}'));
 Object exp1 = s.getValue(whereColumn);
String operation = whereClause.substring(whereClause.indexOf('[')+1,whereClause.indexOf(']'));
String exp = whereClause.substring(whereClause.lastIndexOf('{')+1,whereClause.lastIndexOf('}'));
Object exp2 = "";
if(!"=".equals(operation))
exp2 = Integer.parseInt(exp);
else
exp2 = exp;
if(!compareExp(exp1,exp2,operation))
continue;
}//end of where clause if
String mapKey = "";
for(String colName : groupingAttbArr)
mapKey += s.getValue(colName);
if(!mfMap.containsKey(mapKey)){
mf = new MFStructure();
mfMap.put(mapKey,mf);
mfCountMap.put(mapKey,1);
for(String colName : groupingAttbArr){
Field field = mfClass.getField(colName);
field.set(mf,s.getValue(colName));
}//end of for
if(null != groupFunction && null != groupColumn){
Field field = mfClass.getField(groupAttb);
field.set(mf,evaluate(groupFunction,(int)field.get(mf),(int)s.getValue(groupColumn),mfCountMap.get(mapKey)));
}}else{
if(null != groupFunction && null != groupColumn){
mf = mfMap.get(mapKey);
mfCountMap.put(mapKey,(mfCountMap.get(mapKey).intValue())+1);
Field field = mfClass.getField(groupAttb);
field.set(mf,evaluate(groupFunction,(int)field.get(mf),(int)s.getValue(groupColumn),mfCountMap.get(mapKey)));
}//end of if
}//end of else
}//end of for
if(null != groupFunction && "avg".equals(groupFunction)){
Set<String> keys = mfMap.keySet();
for(String key : keys){
mf = mfMap.get(key);
Field field = mfClass.getField(groupAttb);
field.set(mf,(int)field.get(mf)/(mfCountMap.get(key).intValue()));
}//end of for
}//end of if
return new ArrayList<MFStructure>(mfMap.values());
}


 private boolean compareExp(Object exp1,Object exp2,String operation){
if("=".equals(operation))
return exp1.equals(exp2);
else if("==".equals(operation))
return (int)exp1 == (int)exp2;
else if("<".equals(operation))
return (int)exp1 < (int)exp2;
else if(">".equals(operation))
return (int)exp1 > (int)exp2;
else if("<>".equals(operation))
return (int)exp1 != (int)exp2;
else if("!=".equals(operation))
return !exp1.equals(exp2);
else if("<=".equals(operation))
return (int)exp1 <= (int)exp2;
else if(">=".equals(operation))
return (int)exp1 >= (int)exp2;
else if("&&".equals(operation))
return (boolean)exp1 && (boolean)exp2;
else if("||".equals(operation))
return (boolean)exp1 || (boolean)exp2;
else return false;
}


private void processPredicates(List<MFStructure> mfList,List<Sales> salesList,String predicateClause,String whereClause,String groupingAttr,String aggrFunc)throws Exception{
 Class<MFStructure> mfClass = MFStructure.class;
 String[] predicates = predicateClause.split(",");
 String[] aggrFuncArr = aggrFunc.split(",");
List<String> mfPredicates = null, tuplePredicates = null;
int count = 0;
for(String predicate : predicates){
String[] predicateArr = predicate.split(":");
mfPredicates = new ArrayList<String>();
tuplePredicates = new ArrayList<String>();
for(String p : predicateArr){
 if(p.contains("MF"))
mfPredicates.add(p);
else
tuplePredicates.add(p);
}//end of for loop of individual predicate list
if(mfPredicates.isEmpty()){//it's an MF query
String mfColumnName = aggrFuncArr[count];
 String[] groupingAttrArr = groupingAttr.split(",");
for(String groupingColName : groupingAttrArr){
 StringBuffer sbuf = new StringBuffer();
sbuf.append("{MF.").append(groupingColName).append(".").append(mfColumnName).append("}");
sbuf.append("[=]");
sbuf.append("{").append(groupingColName).append("}");
mfPredicates.add(sbuf.toString());
}//end of for
}//end of if
 Map<Integer,Integer> matchingMFcountMap = new HashMap<Integer,Integer>();
String mfColumnUpd = "", mfGroupFunc="", mfGroupColumn="";
for(Sales s : salesList){
 if(!"null".equals(whereClause)){
String whereColumn = whereClause.substring(whereClause.indexOf('{')+1,whereClause.indexOf('}'));
 Object exp1 = s.getValue(whereColumn);
String operation = whereClause.substring(whereClause.indexOf('[')+1,whereClause.indexOf(']'));
String exp = whereClause.substring(whereClause.lastIndexOf('{')+1,whereClause.lastIndexOf('}'));
Object exp2 = "";
if(!"=".equals(operation))
exp2 = Integer.parseInt(exp);
else
exp2 = exp;
if(!compareExp(exp1,exp2,operation))
continue;
}//end of where clause if
int mfcount = 0;
for(MFStructure mf : mfList){
boolean isMFCondPass = true;
for(String mfPred : mfPredicates){
String mfColumn = mfPred.substring(mfPred.indexOf('{')+1,mfPred.indexOf('}'));
Field field = mfClass.getField(mfColumn.substring(mfColumn.indexOf('.')+1,mfColumn.lastIndexOf('.')));
String mfCol = mfColumn.substring(mfColumn.indexOf('.')+1);
mfColumnUpd = mfCol.substring(mfCol.lastIndexOf(".")+1);
mfGroupFunc = mfColumnUpd.substring(0,mfColumnUpd.indexOf('_'));
mfGroupColumn = mfColumnUpd.substring(mfColumnUpd.indexOf('_')+1,mfColumnUpd.lastIndexOf('_'));
 Object exp2 = field.get(mf);
String operation = mfPred.substring(mfPred.indexOf('[')+1,mfPred.indexOf(']'));
String salesColumn = mfPred.substring(mfPred.lastIndexOf('{')+1,mfPred.lastIndexOf('}'));
Object exp1 = null;
if(!salesColumn.contains("MF."))
exp1 = s.getValue(salesColumn);
else{
Field field1 = mfClass.getField(salesColumn.substring(salesColumn.indexOf('.')+1,salesColumn.lastIndexOf('.')));
exp1 = field1.get(mf);
}//end of else
if(!compareExp(exp1,exp2,operation)){
isMFCondPass = false;
break;
}//end of if
}//end of mf predicate loop
if(isMFCondPass){
if(matchingMFcountMap.containsKey(mfcount))
matchingMFcountMap.put(mfcount,new Integer(matchingMFcountMap.get(mfcount).intValue()+1));
else
matchingMFcountMap.put(mfcount,1);
boolean isTupleCondPass = true;
for(String tuplePred : tuplePredicates){
String tupleColumn = tuplePred.substring(tuplePred.indexOf('{')+1,tuplePred.indexOf('}'));
 Object exp1 = s.getValue(tupleColumn);
String operation = tuplePred.substring(tuplePred.indexOf('[')+1,tuplePred.indexOf(']'));
 Object exp2 = tuplePred.substring(tuplePred.lastIndexOf('{')+1,tuplePred.lastIndexOf('}'));
if(!compareExp(exp1,exp2,operation)){
isTupleCondPass = false;
break;
}//end of if
}//end of tuple predicate list loop
if(isTupleCondPass){
Field field = mfClass.getField(mfColumnUpd);
field.set(mf,evaluate(mfGroupFunc,(int)field.get(mf),(int)s.getValue(mfGroupColumn),matchingMFcountMap.get(mfcount)));
}//end of if
}//end of if
mfcount++;
}//end of for loop of mflist
}//end of sales list for loop
if(null != mfGroupFunc && "avg".equals(mfGroupFunc)){
Set<Integer> keys = matchingMFcountMap.keySet();
for(Integer key : keys){
MFStructure mf = mfList.get(key.intValue());
Field field = mfClass.getField(mfColumnUpd);
field.set(mf,(int)field.get(mf)/(matchingMFcountMap.get(key).intValue()));
}//end of for
}//end of if
count++;
}//end of for loop of predicate keys
}


public static void main(String args[])throws Exception{
QueryProcessor1 q = new QueryProcessor1();
List<Sales> salesList = q.getSalesData();
List<MFStructure> mfList = q.populateMFStructure(salesList,true,"cust,prod,avg,quant_NY,avg_quant_NY,avg_quant_CT,avg_quant_NJ","cust","null");
q.processPredicates(mfList,salesList,"{MF.cust.avg_quant_NY}[=]{cust}:{state}[=]{NY},{MF.cust.avg_quant_CT}[=]{cust}:{state}[=]{CT},{MF.cust.avg_quant_NJ}[=]{cust}:{state}[=]{NJ}","null","cust","avg_quant_NY,avg_quant_CT,avg_quant_NJ");
MFStructure m = q.new MFStructure();
System.out.println(m.printHeader());
for(MFStructure mf : mfList)
System.out.println(mf.print());
System.out.println("\nTotal Records : "+mfList.size());
}
}