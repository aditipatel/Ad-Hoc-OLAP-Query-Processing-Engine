import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;

/**
 * 
 * @author Aditi Patel (Lone Ranger)
 * Ad-hoc OLAP query processing engine to execute MF/EMF queries
 * Algorithm:
 * 1. Create MF structure.
 * 2. Get all sales record from database and populate MF structure (scan 1) based on where clause and grouping variables.
 * 3. For each grouping variable predicate clause
 * 		a. Compare each sale records with all the MF structure records. 
 * 		b. For each MF structure record that satisfies the where clause and predicate clause conditions update the entry based on aggregate function.
 * 4. Apply having clause expression on MF structure list and return final MF structure list. 
 */
public class AdhocQueryProcessor {

	final static String QUERY_SEPERATOR = "~";
	
/**
 * This method reads the input query file (containing multiple queries separated by "~")
 * and returns the list of all the queries represented by list of strings.
 * @return Returns list of list of queries.
 */
	public List<List<String>> readInputFile() {

		JFileChooser filechooser = new JFileChooser();
		List<List<String>> queryList = new ArrayList<>();
		if (filechooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
			java.io.File file = filechooser.getSelectedFile();
			Scanner input = null;
			try {
				input= new Scanner(file);
				if (file.length() == 0) {
					JOptionPane.showMessageDialog(null, "The file is empty");
				}
				String line;
				List<String> query = null;
				while(input.hasNextLine()){
					line = input.nextLine();
					if(QUERY_SEPERATOR.equals(line) || query == null){
						query = new ArrayList<String>();
						queryList.add(query);
						if(QUERY_SEPERATOR.equals(line))
							continue;
					}
					query.add(line);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}finally{
				if(null != input)
					input.close();
			}
		}//end of if

		return queryList;

	}// end of readInputFile

	/**
	 * This method executes the queries.
	 * @param queryList List of queries
	 */
	public void processQueries(List<List<String>> queryList){
		int count = 1;
		for(List<String> query : queryList){
			FileWriter fw = null;
			String fileName = "QueryProcessor"+count;
			try {

				//parse input query
				String[] selectAttrArr = query.get(0).split(",");
				int selectAttrCount = selectAttrArr.length;

				int groupCount = Integer.parseInt(query.get(1));

				StringBuffer groupingAttrStrbuf = new StringBuffer(query.get(2));
				String[] groupingAtrrArr = query.get(2).split(",");
				List<String> columns = new ArrayList<String>();
				for(String groupingAttr : groupingAtrrArr)
					columns.add(groupingAttr);
				int noOfGrpAttr = groupingAtrrArr.length;

				boolean isGroupZeroPresent = selectAttrCount - (noOfGrpAttr+groupCount) > 0;

				//retrieve data type for grouping attributes from information schema
				Map<String,String> datatypeMap = retreiveDataTypes(columns);

				fw = new FileWriter(new File("src/"+fileName+".java"));
				fw.write("import java.util.*;");
				fw.write("\nimport java.lang.reflect.*;");
				fw.write("\nimport java.sql.*;");
				fw.write("\n\npublic class "+fileName+"{");

				//parse input and create MF structure
				createMFStructure(datatypeMap,selectAttrArr,fw);
				//create DB connection method
				createMethodToGetDBConnection(fw);
				//create sales class
				createSalesClass(fw);
				//create method to get all sales record satisfying where clause
				createMethodToGetSalesData(fw,query);
				//create method to evaluate group functions (sum,avg,min,max,count)
				createMethodToEvaluateGrpFunc(fw);
				//create method to populate MF structure
				createMethodToPopulateMFStructure(fw);
				//create method to evaluate expression
				createMethodToEvaluateExpression(fw);
				//create method to process predicates for grouping variables
				createMethodToProcessPredicates(fw);
				//create method to process having clause
				if(query.size() > 5)
				createMethodToProcessHavingClause(fw);
				String havingClause = null;
				if(query.size() > 5)
					havingClause = query.get(5);
				//create main method
				createMainMethod(fw,count,isGroupZeroPresent,query.get(0),groupingAttrStrbuf.toString(),query.get(4),groupCount,query.get(3),havingClause);

				fw.write("\n}");
				count++;
			}//end of try
			catch (IOException e) {
				e.printStackTrace();
			}finally{
				if(null != fw)
					try {
						fw.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
			}
		}//end of for
	}

	/**
	 * This method retrieves the data types of the the grouping attributes from the information schema
	 * @param coulmns Columns whose data type needs to be  retrieved.
	 * @return Returns Map containing column name and it's corresponding data type.
	 */
	private Map<String,String> retreiveDataTypes(List<String> coulmns) {
		Map<String,String> dataTypeMap = new HashMap<String,String>();

		try {
			String usr = "postgres";
			String pwd = "bhavin";
			String url = "jdbc:postgresql://localhost:5432/postgres";

			Class.forName("org.postgresql.Driver");     //Loads the required driver
			Connection con = DriverManager.getConnection(url, usr, pwd); // connect
			ResultSet rs;
			Statement st =  (Statement) con.createStatement(); // statement

			//prepare in clause for column names
			StringBuffer inColumnNamesStr = new StringBuffer();
			inColumnNamesStr.append("(");
			for(int i=0;i<coulmns.size();i++){
				inColumnNamesStr.append("'").append(coulmns.get(i)).append("'");
				if(i != coulmns.size()-1)
					inColumnNamesStr.append(",");
			}
			inColumnNamesStr.append(")");

			String query = "select column_name,data_type"
					+ " from information_schema.columns where table_name = 'sales' and column_name in "+inColumnNamesStr.toString()+";";
			rs =  st.executeQuery(query);

			while(rs.next()){ // checking if more rows available
				dataTypeMap.put(rs.getString(1), (rs.getString(2)).startsWith("character") ? "String": "int");
			}

		}
		catch (SQLException e) {
			System.out.println("Connection URL or username or password errors!");
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return dataTypeMap;
	}
/**
 * This method creates MF Structure object.
 * @param datatypeMap  Data type Map
 * @param selectAttrArr Select attributes
 * @param fw Filewriter object
 * @return Returns Map containing column name and it's corresponding data type.
 * @throws IOException throws IO exception
 */
	private Map<String,String> createMFStructure(Map<String,String> datatypeMap, String[] selectAttrArr, FileWriter fw) throws IOException{
		fw.write("\n\nclass MFStructure{");
		Set<String> groupingAttrSet = datatypeMap.keySet();
		for(String columnName : groupingAttrSet){
			fw.write("\npublic "+datatypeMap.get(columnName)+" "+columnName+";");
		}
		for(int i=datatypeMap.size();i<selectAttrArr.length;i++){
				fw.write("\npublic int "+selectAttrArr[i]+";");
		}

		fw.write("\n\npublic String printHeader() throws Exception{");
		fw.write("\nField[] allFields = MFStructure.class.getDeclaredFields();");
		fw.write("\nStringBuffer sbuf = new StringBuffer();");
		fw.write("\nfor (Field field : allFields) {");
		fw.write("\nif(!field.getName().startsWith(\"this\")){");
		fw.write("\nsbuf.append(String.format(\"%-18s\",field.getName()));");
		fw.write("\n}//end of if");
		fw.write("\n}//end of for");
		fw.write("\nreturn sbuf.toString();");
		fw.write("\n}");

		fw.write("\n\npublic String print() throws Exception{");
		fw.write("\nField[] allFields = MFStructure.class.getDeclaredFields();");
		fw.write("\nStringBuffer sbuf = new StringBuffer();");
		fw.write("\nfor (Field field : allFields) {");
		fw.write("\nif(!field.getName().startsWith(\"this\")){");
		fw.write("\nsbuf.append(String.format(\"%-18s\",field.get(this)));");
		fw.write("\n}//end of if");
		fw.write("\n}//end of for");
		fw.write("\nreturn sbuf.toString();");
		fw.write("\n}");

		fw.write("\n}");

		return datatypeMap;

	}
/**
 * This method returns the database connection
 * @param fw Filewriter object
 * @throws IOException throws IO exception
 */
	private void createMethodToGetDBConnection(FileWriter fw) throws IOException{
		fw.write("\n\n");
		fw.write("private Connection getDBConnection() throws Exception{");
		fw.write("\nString usr = \"postgres\";");
		fw.write("\nString pwd = \"bhavin\";");
		fw.write("\nString url = \"jdbc:postgresql://localhost:5432/postgres\";");
		fw.write("\nClass.forName(\"org.postgresql.Driver\"); ");
		fw.write("\nConnection con = DriverManager.getConnection(url, usr, pwd);");
		fw.write("\nreturn con;");
		fw.write("\n}");
	}
/**
 * This method creates the sales object to hold the sales table records.
 * @param fw Filewriter object
 * @throws IOException throws IO exception
 */
	private void createSalesClass(FileWriter fw) throws IOException{
		fw.write("\n\n");
		fw.write("private class Sales{");
		fw.write("\nprivate String cust;");
		fw.write("\nprivate String prod;");
		fw.write("\nprivate Integer day;");
		fw.write("\nprivate Integer month;");
		fw.write("\nprivate Integer year;");
		fw.write("\nprivate String state;");
		fw.write("\nprivate Integer quant;");

		fw.write("\n\npublic Object getValue(String name){");
		fw.write("\nif(\"cust\".equals(name))");
		fw.write("\nreturn cust;");
		fw.write("\nelse if(\"prod\".equals(name))");
		fw.write("\nreturn prod;");
		fw.write("\nelse if(\"day\".equals(name))");
		fw.write("\nreturn day;");
		fw.write("\nelse if(\"month\".equals(name))");
		fw.write("\nreturn month;");
		fw.write("\nelse if(\"year\".equals(name))");
		fw.write("\nreturn year;");
		fw.write("\nelse if(\"state\".equals(name))");
		fw.write("\nreturn state;");
		fw.write("\nelse if(\"quant\".equals(name))");
		fw.write("\nreturn quant;");
		fw.write("\nreturn null;");
		fw.write("\n}");
		fw.write("\n}");
	}
/**
 * This method is used to get all the data from sales table and store it in the List of Sales object in memory
 * @param fw Filewriter object
 * @param query Input query
 * @throws IOException throws IO exception
 */
	private void createMethodToGetSalesData(FileWriter fw,List<String> query) throws IOException{

		fw.write("\n\n");
		fw.write("private List<Sales> getSalesData() throws Exception{");
		fw.write("\nConnection con = getDBConnection();");
		fw.write("\nStatement st = (Statement)con.createStatement();");
		fw.write("\nString salesQuery = \"select * from sales\";");
		fw.write("\n ResultSet rs;");
		fw.write("\nrs = st.executeQuery(salesQuery);");
		fw.write("\nList<Sales> salesList = new ArrayList<Sales>();");
		fw.write("\nSales salesObj = null;");
		fw.write("\nwhile (rs.next()){");
		fw.write("\nsalesObj = new Sales();");
		fw.write("\nsalesObj.cust = rs.getString(\"cust\");");
		fw.write("\nsalesObj.prod = rs.getString(\"prod\");");
		fw.write("\nsalesObj.day = rs.getInt(\"day\");");
		fw.write("\nsalesObj.month = rs.getInt(\"month\");");
		fw.write("\nsalesObj.year = rs.getInt(\"year\");");
		fw.write("\nsalesObj.state = rs.getString(\"state\");");
		fw.write("\nsalesObj.quant = rs.getInt(\"quant\");");
		fw.write("\nsalesList.add(salesObj);");
		fw.write("\n}");
		fw.write("\nreturn salesList;");
		fw.write("\n}");
	}
/**
 * This is a generic method to evaluate all aggregate functions
 * Functions supported :-sum(), min(), max(), count() and average()
 * @param fw Filewriter object
 * @throws IOException throws IO exception
 */
	private void createMethodToEvaluateGrpFunc(FileWriter fw) throws IOException{
		fw.write("\n\n");
		fw.write("\nprivate int evaluate(String groupFunction,int currVal,int newVal, int count){");
		fw.write("\nif(groupFunction.equals(\"avg\") || groupFunction.equals(\"sum\"))");
		fw.write("\n return currVal+newVal;");
		fw.write("\nelse if(groupFunction.equals(\"min\"))");
		fw.write("\n return (newVal < currVal && currVal != 0) ? newVal : (currVal == 0) ? newVal : currVal;");
		fw.write("\nelse if(groupFunction.equals(\"max\"))");
		fw.write("\n return (newVal > currVal) ? newVal : currVal;");
		fw.write("\nelse if(groupFunction.equals(\"count\"))");
		fw.write("\n return count;");
		fw.write("\n else");
		fw.write("\nreturn 0;");
		fw.write("\n}");
	}
	/**
	 * This method populates the MF structure from the list of sales record based on the where clause condition and grouping variables.
	 * @param fw Filewriter object
	 * @throws IOException throws IO exception
	 */
	private void createMethodToPopulateMFStructure(FileWriter fw) throws IOException{
		fw.write("\n\n");
		fw.write("\nprivate List<MFStructure> populateMFStructure(List<Sales> salesList,boolean isGroupZeroPresent, String selectAttb,String groupingAttb,String whereClause)throws Exception{");
		fw.write("\nString[] selectAttbArr = selectAttb.split(\",\");");
		fw.write("\nString[] groupingAttbArr = groupingAttb.split(\",\");");
		fw.write("\nMap<String,MFStructure> mfMap = new HashMap<String,MFStructure>();");
		fw.write("\nMap<String,Integer> mfCountMap = new HashMap<String,Integer>();");
		fw.write("\nString groupFunction = null, groupColumn = null, groupAttb = null;");
		fw.write("\nif(isGroupZeroPresent){");
		fw.write("\ngroupAttb = selectAttbArr[groupingAttbArr.length];");
		fw.write("\ngroupFunction = groupAttb.substring(0,groupAttb.indexOf(\"_\"));");
		fw.write("\ngroupColumn = groupAttb.substring(groupAttb.indexOf(\"_\")+1);");
		fw.write("\n}");
		fw.write("\nMFStructure mf = null;");
		fw.write("\n Class<MFStructure> mfClass = MFStructure.class;");
		fw.write("\nfor(Sales s : salesList){");
		fw.write("\n if(!\"null\".equals(whereClause)){");
		fw.write("\nString whereColumn = whereClause.substring(whereClause.indexOf('{')+1,whereClause.indexOf('}'));");
		fw.write("\n Object exp1 = s.getValue(whereColumn);");
		fw.write("\nString operation = whereClause.substring(whereClause.indexOf('[')+1,whereClause.indexOf(']'));");
		fw.write("\nString exp = whereClause.substring(whereClause.lastIndexOf('{')+1,whereClause.lastIndexOf('}'));");
		fw.write("\nObject exp2 = \"\";");
		fw.write("\nif(!\"=\".equals(operation))");
		fw.write("\nexp2 = Integer.parseInt(exp);");
		fw.write("\nelse");
		fw.write("\nexp2 = exp;");
		fw.write("\nif(!compareExp(exp1,exp2,operation))");
		fw.write("\ncontinue;");
		fw.write("\n}//end of where clause if");
		fw.write("\nString mapKey = \"\";");
		fw.write("\nfor(String colName : groupingAttbArr)");
		fw.write("\nmapKey += s.getValue(colName);");
		fw.write("\nif(!mfMap.containsKey(mapKey)){");
		fw.write("\nmf = new MFStructure();");
		fw.write("\nmfMap.put(mapKey,mf);");
		fw.write("\nmfCountMap.put(mapKey,1);");
		fw.write("\nfor(String colName : groupingAttbArr){");
		fw.write("\nField field = mfClass.getField(colName);");
		fw.write("\nfield.set(mf,s.getValue(colName));");
		fw.write("\n}//end of for");
		fw.write("\nif(null != groupFunction && null != groupColumn){");
		fw.write("\nField field = mfClass.getField(groupAttb);");
		fw.write("\nfield.set(mf,evaluate(groupFunction,(int)field.get(mf),(int)s.getValue(groupColumn),mfCountMap.get(mapKey)));");
		fw.write("\n}");//end of if
		fw.write("}else{");
		fw.write("\nif(null != groupFunction && null != groupColumn){");
		fw.write("\nmf = mfMap.get(mapKey);");
		fw.write("\nmfCountMap.put(mapKey,(mfCountMap.get(mapKey).intValue())+1);");
		fw.write("\nField field = mfClass.getField(groupAttb);");
		fw.write("\nfield.set(mf,evaluate(groupFunction,(int)field.get(mf),(int)s.getValue(groupColumn),mfCountMap.get(mapKey)));");
		fw.write("\n}//end of if");
		fw.write("\n}//end of else");
		fw.write("\n}//end of for");
		fw.write("\nif(null != groupFunction && \"avg\".equals(groupFunction)){");
		fw.write("\nSet<String> keys = mfMap.keySet();");
		fw.write("\nfor(String key : keys){");
		fw.write("\nmf = mfMap.get(key);");
		fw.write("\nField field = mfClass.getField(groupAttb);");
		fw.write("\nfield.set(mf,(int)field.get(mf)/(mfCountMap.get(key).intValue()));");
		fw.write("\n}//end of for");
		fw.write("\n}//end of if");
		fw.write("\nreturn new ArrayList<MFStructure>(mfMap.values());");
		fw.write("\n}");
	}
/**
 * This method is used to evaluate a given relational expression
 * Operators supported :- For string comparison --> "=","!="
 * 						  For int comparison --> ">","<","<=",">=","==","<>"
 * @param fw Filewriter object
 * @throws IOException throws IO exception
 */
	private void createMethodToEvaluateExpression(FileWriter fw) throws IOException{
		fw.write("\n\n");
		fw.write("\n private boolean compareExp(Object exp1,Object exp2,String operation){");
		fw.write("\nif(\"=\".equals(operation))");
		fw.write("\nreturn exp1.equals(exp2);");
		fw.write("\nelse if(\"==\".equals(operation))");
		fw.write("\nreturn (int)exp1 == (int)exp2;");
		fw.write("\nelse if(\"<\".equals(operation))");
		fw.write("\nreturn (int)exp1 < (int)exp2;");
		fw.write("\nelse if(\">\".equals(operation))");
		fw.write("\nreturn (int)exp1 > (int)exp2;");
		fw.write("\nelse if(\"<>\".equals(operation))");
		fw.write("\nreturn (int)exp1 != (int)exp2;");
		fw.write("\nelse if(\"!=\".equals(operation))");
		fw.write("\nreturn !exp1.equals(exp2);");
		fw.write("\nelse if(\"<=\".equals(operation))");
		fw.write("\nreturn (int)exp1 <= (int)exp2;");
		fw.write("\nelse if(\">=\".equals(operation))");
		fw.write("\nreturn (int)exp1 >= (int)exp2;");
		fw.write("\nelse if(\"&&\".equals(operation))");
		fw.write("\nreturn (boolean)exp1 && (boolean)exp2;");
		fw.write("\nelse if(\"||\".equals(operation))");
		fw.write("\nreturn (boolean)exp1 || (boolean)exp2;");
		fw.write("\nelse return false;");
		fw.write("\n}");
	}
/**
 * This method parses the having clause expression and filters the records from MF Structure based in this expression
 * Format of having clause expression:
 * {left condition}[boolean operator][right condition]
 * Format of condition:
 * {left operand,operator,right operand}
 * Having clause example:
 * {avg_quant_NY,>,avg_quant_CT} [&&] {avg_quant_NY,>,avg_quant_NJ}
 * 
 * @param fw Filewriter object
 * @throws IOException throws IO exception
 */
	private void createMethodToProcessHavingClause(FileWriter fw) throws IOException{
		fw.write("\n\n");
		fw.write("\nprivate List<MFStructure> processHavingClause(List<MFStructure> mfList,String havingClause) throws Exception{");
		fw.write("\n String[] havingCondArr = havingClause.split(\":\");");
		fw.write("\n List<MFStructure> filteredList = new ArrayList<MFStructure>();");
		fw.write("\n Class<MFStructure> mfClass = MFStructure.class;");
		fw.write("\nfor(MFStructure mf : mfList){");
		fw.write("\n boolean isHavingCondMet = false;");
		fw.write("\nfor(String havingCond : havingCondArr){");
		fw.write("\nif(havingCond.contains(\"[\")){");
		fw.write("\nString leftExpr = havingCond.substring(havingCond.indexOf('{')+1,havingCond.indexOf('}'));");
		fw.write("\nString rightExpr = havingCond.substring(havingCond.lastIndexOf('{')+1,havingCond.lastIndexOf('}'));");
		fw.write("\nString operator = havingCond.substring(havingCond.indexOf('[')+1,havingCond.indexOf(']'));");
		fw.write("\nString[] leftExpArr = leftExpr.split(\",\");");
		fw.write("\nString[] rightExpArr = rightExpr.split(\",\");");
		fw.write("\nField field = mfClass.getField(leftExpArr[0]);");
		fw.write("\nObject leftleftVal = field.get(mf);");
		fw.write("\nfield = mfClass.getField(leftExpArr[2]);");
		fw.write("\nObject leftrighttVal = field.get(mf);");
		fw.write("\nfield = mfClass.getField(rightExpArr[0]);");
		fw.write("\nObject rightleftVal = field.get(mf);");
		fw.write("\nfield = mfClass.getField(rightExpArr[2]);");
		fw.write("\nObject rightrightVal = field.get(mf);");
		fw.write("\nString leftOp = leftExpArr[1];");
		fw.write("\nString rightOp = rightExpArr[1];");
		fw.write("\nboolean leftRes = compareExp(leftleftVal,leftrighttVal,leftOp);");
		fw.write("\nboolean rightRes = compareExp(rightleftVal,rightrightVal,rightOp);");
		fw.write("\n isHavingCondMet = compareExp(leftRes,rightRes,operator);");
		fw.write("\nif(!\"&&\".equals(operator))");
		fw.write("\n isHavingCondMet = leftRes || rightRes;");
		fw.write("\n}//end of if");
		fw.write("\nelse{");
		fw.write("\nString leftExpr = havingCond.substring(havingCond.indexOf('{')+1,havingCond.indexOf('}'));");
		fw.write("\nString[] leftExpArr = leftExpr.split(\",\");");
		fw.write("\nObject leftleftVal = null,leftrightVal = null;");
		fw.write("\nif(leftExpArr[0].contains(\"MF.\")){");
		fw.write("\nField field = mfClass.getField(leftExpArr[0].substring(leftExpArr[0].indexOf('.')+1));");
		fw.write("\nleftleftVal = field.get(mf);");
		fw.write("\n}");
		fw.write("\nelse");
		fw.write("\nleftleftVal = leftExpArr[0];");
		fw.write("\nif(leftExpArr[2].contains(\"MF.\")){");
		fw.write("\nField field = mfClass.getField(leftExpArr[2].substring(leftExpArr[2].indexOf('.')+1));");
		fw.write("\nleftrightVal = field.get(mf);");
		fw.write("\n}");
		fw.write("\nelse");
		fw.write("\nleftrightVal = leftExpArr[2];");
		fw.write("\nString leftOp = leftExpArr[1];");
		fw.write("\nisHavingCondMet = compareExp(leftleftVal,leftrightVal,leftOp);");
		fw.write("\n}//end of else");
		fw.write("\n}//end of for loop of having conditions");
		fw.write("\n if(isHavingCondMet)");
		fw.write("\nfilteredList.add(mf);");
		fw.write("\n}//end of for loop of mf list");
		fw.write("\n return filteredList;");
		fw.write("\n}");
	}
/**
 * This method does the following:
 * Step 1: Parses the predicate expression
 * Step 2: Evaluate each tuple record for the given predicate expression against the list of MF structure records
 * 
 * Format of the predicate clause expression:-
 * Predicate clause for each grouping variables are separated by ",".
 * Multiple conditions within each clause are separated by ":".
 * Where clause condition expression.
 * Format of condition expression:
 * {left operand}[operator]{right operand}
 * Expression format:
 * if expression represents MF structure column : {MF.<grouping_variable>.<mf_column_name>} e.g. {MF.cust.avg_quant_NY}
 * if expression represents tuple column : {<grouping_variable>} e.g. {cust}
 * Full predicate clause expression example:
 * {MF.cust.avg_quant_NY}[=]{cust}:{state}[=]{NY},{MF.cust.avg_quant_CT}[=]{cust}:{state}[=]{CT},{MF.cust.avg_quant_NJ}[=]{cust}:{state}[=]{NJ},{year}[==]{1997}
 * 
 * @param fw Filewriter object
 * @throws IOException throws IO exception
 */
	private void createMethodToProcessPredicates(FileWriter fw) throws IOException{
		fw.write("\n\n");
		fw.write("\nprivate void processPredicates(List<MFStructure> mfList,List<Sales> salesList,String predicateClause,String whereClause,String groupingAttr,String aggrFunc)throws Exception{");
		fw.write("\n Class<MFStructure> mfClass = MFStructure.class;");
		fw.write("\n String[] predicates = predicateClause.split(\",\");");
		fw.write("\n String[] aggrFuncArr = aggrFunc.split(\",\");");
		fw.write("\nList<String> mfPredicates = null, tuplePredicates = null;");
		fw.write("\nint count = 0;");
		fw.write("\nfor(String predicate : predicates){");
		fw.write("\nString[] predicateArr = predicate.split(\":\");");
		fw.write("\nmfPredicates = new ArrayList<String>();");
		fw.write("\ntuplePredicates = new ArrayList<String>();");
		fw.write("\nfor(String p : predicateArr){");
		fw.write("\n if(p.contains(\"MF\"))");
		fw.write("\nmfPredicates.add(p);");
		fw.write("\nelse");
		fw.write("\ntuplePredicates.add(p);");
		fw.write("\n}//end of for loop of individual predicate list");
		fw.write("\nif(mfPredicates.isEmpty()){//it's an MF query");
		fw.write("\nString mfColumnName = aggrFuncArr[count];");
		fw.write("\n String[] groupingAttrArr = groupingAttr.split(\",\");");
		fw.write("\nfor(String groupingColName : groupingAttrArr){");
		fw.write("\n StringBuffer sbuf = new StringBuffer();");
		fw.write("\nsbuf.append(\"{MF.\").append(groupingColName).append(\".\").append(mfColumnName).append(\"}\");");
		fw.write("\nsbuf.append(\"[=]\");");
		fw.write("\nsbuf.append(\"{\").append(groupingColName).append(\"}\");");
		fw.write("\nmfPredicates.add(sbuf.toString());");
		fw.write("\n}//end of for");
		fw.write("\n}//end of if");
		fw.write("\n Map<Integer,Integer> matchingMFcountMap = new HashMap<Integer,Integer>();");
		fw.write("\nString mfColumnUpd = \"\", mfGroupFunc=\"\", mfGroupColumn=\"\";");
		fw.write("\nfor(Sales s : salesList){");
		fw.write("\n if(!\"null\".equals(whereClause)){");
		fw.write("\nString whereColumn = whereClause.substring(whereClause.indexOf('{')+1,whereClause.indexOf('}'));");
		fw.write("\n Object exp1 = s.getValue(whereColumn);");
		fw.write("\nString operation = whereClause.substring(whereClause.indexOf('[')+1,whereClause.indexOf(']'));");
		fw.write("\nString exp = whereClause.substring(whereClause.lastIndexOf('{')+1,whereClause.lastIndexOf('}'));");
		fw.write("\nObject exp2 = \"\";");
		fw.write("\nif(!\"=\".equals(operation))");
		fw.write("\nexp2 = Integer.parseInt(exp);");
		fw.write("\nelse");
		fw.write("\nexp2 = exp;");
		fw.write("\nif(!compareExp(exp1,exp2,operation))");
		fw.write("\ncontinue;");
		fw.write("\n}//end of where clause if");
		fw.write("\nint mfcount = 0;");
		fw.write("\nfor(MFStructure mf : mfList){");
		fw.write("\nboolean isMFCondPass = true;");
		fw.write("\nfor(String mfPred : mfPredicates){");
		fw.write("\nString mfColumn = mfPred.substring(mfPred.indexOf('{')+1,mfPred.indexOf('}'));");
		fw.write("\nField field = mfClass.getField(mfColumn.substring(mfColumn.indexOf('.')+1,mfColumn.lastIndexOf('.')));");
		fw.write("\nString mfCol = mfColumn.substring(mfColumn.indexOf('.')+1);");
		fw.write("\nmfColumnUpd = mfCol.substring(mfCol.lastIndexOf(\".\")+1);");
		fw.write("\nmfGroupFunc = mfColumnUpd.substring(0,mfColumnUpd.indexOf('_'));");
		fw.write("\nmfGroupColumn = mfColumnUpd.substring(mfColumnUpd.indexOf('_')+1,mfColumnUpd.lastIndexOf('_'));");
		fw.write("\n Object exp2 = field.get(mf);");
		fw.write("\nString operation = mfPred.substring(mfPred.indexOf('[')+1,mfPred.indexOf(']'));");
		fw.write("\nString salesColumn = mfPred.substring(mfPred.lastIndexOf('{')+1,mfPred.lastIndexOf('}'));");
		fw.write("\nObject exp1 = null;");
		fw.write("\nif(!salesColumn.contains(\"MF.\"))");
		fw.write("\nexp1 = s.getValue(salesColumn);");
		fw.write("\nelse{");
		fw.write("\nField field1 = mfClass.getField(salesColumn.substring(salesColumn.indexOf('.')+1,salesColumn.lastIndexOf('.')));");
		fw.write("\nexp1 = field1.get(mf);");
		fw.write("\n}//end of else");
		fw.write("\nif(!compareExp(exp1,exp2,operation)){");
		fw.write("\nisMFCondPass = false;");
		fw.write("\nbreak;");
		fw.write("\n}//end of if");
		fw.write("\n}//end of mf predicate loop");
		fw.write("\nif(isMFCondPass){");
		fw.write("\nif(matchingMFcountMap.containsKey(mfcount))");
		fw.write("\nmatchingMFcountMap.put(mfcount,new Integer(matchingMFcountMap.get(mfcount).intValue()+1));");
		fw.write("\nelse");
		fw.write("\nmatchingMFcountMap.put(mfcount,1);");
		fw.write("\nboolean isTupleCondPass = true;");
		fw.write("\nfor(String tuplePred : tuplePredicates){");
		fw.write("\nString tupleColumn = tuplePred.substring(tuplePred.indexOf('{')+1,tuplePred.indexOf('}'));");
		fw.write("\n Object exp1 = s.getValue(tupleColumn);");
		fw.write("\nString operation = tuplePred.substring(tuplePred.indexOf('[')+1,tuplePred.indexOf(']'));");
		fw.write("\n Object exp2 = tuplePred.substring(tuplePred.lastIndexOf('{')+1,tuplePred.lastIndexOf('}'));");
		fw.write("\nif(!compareExp(exp1,exp2,operation)){");
		fw.write("\nisTupleCondPass = false;");
		fw.write("\nbreak;");
		fw.write("\n}//end of if");
		fw.write("\n}//end of tuple predicate list loop");
		fw.write("\nif(isTupleCondPass){");
		fw.write("\nField field = mfClass.getField(mfColumnUpd);");
		fw.write("\nfield.set(mf,evaluate(mfGroupFunc,(int)field.get(mf),(int)s.getValue(mfGroupColumn),matchingMFcountMap.get(mfcount)));");
		fw.write("\n}//end of if");
		fw.write("\n}//end of if");
		fw.write("\nmfcount++;");
		fw.write("\n}//end of for loop of mflist");
		fw.write("\n}//end of sales list for loop");
		fw.write("\nif(null != mfGroupFunc && \"avg\".equals(mfGroupFunc)){");
		fw.write("\nSet<Integer> keys = matchingMFcountMap.keySet();");
		fw.write("\nfor(Integer key : keys){");
		fw.write("\nMFStructure mf = mfList.get(key.intValue());");
		fw.write("\nField field = mfClass.getField(mfColumnUpd);");
		fw.write("\nfield.set(mf,(int)field.get(mf)/(matchingMFcountMap.get(key).intValue()));");
		fw.write("\n}//end of for");
		fw.write("\n}//end of if");
		fw.write("\ncount++;");
		fw.write("\n}//end of for loop of predicate keys");
		fw.write("\n}");
	}
	
	/**
	 * Creates the main method for the generated code which initiates the query engine execution. 
	 * @param fw Filewriter object
	 * @param count Input query count
	 * @param isGroupZeroPresent Boolean flag to indicate if grouping variable zero is present.
	 * @param selectAttb Select attributes.
	 * @param groupingAttb Grouping attributes.
	 * @param predicateClauseStr Predicate clause for all grouping variables.
	 * @param noOfGrpVariables No of grouping variables.
	 * @param aggregrateFunctions Aggregate functions. 
	 * @param havingClause Having clause expression.
	 * @throws IOException throws IO exception
	 */
	private void createMainMethod(FileWriter fw,int count,boolean isGroupZeroPresent, String selectAttb,String groupingAttb,String predicateClauseStr,int noOfGrpVariables,String aggregrateFunctions,String havingClause) throws IOException{
		fw.write("\n\n");
		fw.write("\npublic static void main(String args[])throws Exception{");
		fw.write("\nQueryProcessor"+count+" q = new QueryProcessor"+count+"();");
		fw.write("\nList<Sales> salesList = q.getSalesData();");
		String whereClause = null;
		String[] predicatesArr = predicateClauseStr.split(",");
		String predicateClause = predicateClauseStr;
		//check if query where clause is present
		if(predicatesArr.length > noOfGrpVariables){
			whereClause = predicatesArr[noOfGrpVariables];
			predicateClause = predicateClauseStr.substring(0,predicateClauseStr.lastIndexOf(","));
		}
		fw.write("\nList<MFStructure> mfList = q.populateMFStructure(salesList,"+isGroupZeroPresent+",\""+selectAttb+"\",\""+groupingAttb+"\",\""+whereClause+"\");");

		fw.write("\nq.processPredicates(mfList,salesList,\""+predicateClause+"\",\""+whereClause+"\",\""+groupingAttb+"\",\""+aggregrateFunctions+"\");");
		if(null != havingClause){
			fw.write("\nmfList = q.processHavingClause(mfList,\""+havingClause+"\");");
		}
		fw.write("\nMFStructure m = q.new MFStructure();");
		fw.write("\nSystem.out.println(m.printHeader());");
		fw.write("\nfor(MFStructure mf : mfList)");
		fw.write("\nSystem.out.println(mf.print());");
		fw.write("\nSystem.out.println(\"\\nTotal Records : \"+mfList.size());");
		fw.write("\n}");
	}

	/**
	 * Main method.
	 * @param args main method arguments
	 */
	public static void main(String[] args) {
		AdhocQueryProcessor adHocObj = new AdhocQueryProcessor();
		List<List<String>> queries = adHocObj.readInputFile();
		adHocObj.processQueries(queries);

	}

}


