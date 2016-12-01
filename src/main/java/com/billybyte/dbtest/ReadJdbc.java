package com.billybyte.dbtest;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;


public class ReadJdbc {
    private final Connection con;
    private final Statement st;
     /**
     * 
     * @param url - like test.cxpdwhygwwwh.us-east-1.rds.amazonaws.com
     * @param uid
     * @param password
     */
    public ReadJdbc(String url,String uid, String password, String databaseName){
        try {
        	String fullPostgresUrl =  "jdbc:postgresql://" + url + "/" + databaseName;
			con = DriverManager.getConnection(fullPostgresUrl,uid,password);
	        st = con.createStatement();
	        ResultSet rs = st.executeQuery("SELECT VERSION()");

	        if (rs.next()) {
	            System.out.println(rs.getString(1));
	        }
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
    }
    
    /**
     * 
     * @param readSql
     * @return Map<Integer,Map<String,String>> - a map where the key is row number,
     *           and the value is another map of column names and column values 
     */
    public Map<Integer,Map<String,String>> get(String readSql){
        try {
      	  ResultSet rs  = st.executeQuery(readSql);
      	Map<Integer,Map<String,String>> rowMap = new TreeMap<Integer,Map<String,String>>();
      	  int rowNum=0;
      	  while (rs.next())
      	  {
				ResultSetMetaData rsmd = rs.getMetaData();
				int numCols = rsmd.getColumnCount();
				Map<String,String> colMap = new HashMap<String,String>();
				for(int i=1;i<=numCols;i++){
					String colName = rsmd.getColumnName(i);
					String colValue = rs.getString(i);
					colMap.put(colName, colValue);
				}
				rowMap.put(rowNum++,colMap);
      	  } 
      	  rs.close();
      	  return rowMap;
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
    }
    
    Map<Integer,Map<String,String>> getFromSqlTextFile(boolean isResource,String readSqlPath){
		String rcvSql="";
		try {
			File file =null;
			if(isResource){
				ClassLoader classLoader = this.getClass().getClassLoader();
				file = new File(classLoader.getResource(readSqlPath).getFile());
			}else{
				file = new File(readSqlPath);
			}
			Scanner fio = new Scanner(file);
			while(fio.hasNextLine()){
				rcvSql += fio.nextLine()+ " ";
			}
			fio.close();
		} catch (Exception e1) {
			throw new IllegalStateException(e1);
		}
		return get(rcvSql);

    }
    
    public Map<Integer,Map<String,Object>> getObject(String readSql){
        try {
        	  ResultSet rs  = st.executeQuery(readSql);
        	Map<Integer,Map<String,Object>> rowMap = new TreeMap<Integer,Map<String,Object>>();
        	  int rowNum=0;
        	  while (rs.next())
        	  {
  				ResultSetMetaData rsmd = rs.getMetaData();
  				int numCols = rsmd.getColumnCount();
  				Map<String,Object> colMap = new HashMap<String,Object>();
  				for(int i=1;i<=numCols;i++){
  					String colName = rsmd.getColumnName(i);
  					Object colValue = rs.getObject(i);
  					colMap.put(colName, colValue);
  				}
  				rowMap.put(rowNum++,colMap);
        	  } 
        	  rs.close();
        	  return rowMap;
  		} catch (Exception e) {
			throw new IllegalStateException(e);
  		}
    }

    public void close(){
    	try {
			con.close();
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
    }
    
	public static void main(String[] args) {
		 Logger logger = Logger.getLogger(ReadJdbc.class); 

		Map<String,String> argMap = getArgPairsSeparatedByChar(args, "=");
        String url = argMap.get("url");
        String uid = argMap.get("uid");
        String pass = argMap.get("pass");
        String dbName = argMap.get("dbName");
        String selectStatement = argMap.get("initQuery");
        
        ReadJdbc rp2 = new ReadJdbc(url, uid, pass, dbName);
        boolean keepGoing = true;
        Scanner sc = new Scanner(System.in);
        while(keepGoing){
        	System.out.print("Enter select statment, 'exit' or return for default: "+selectStatement+" -> ");
        	
        	String temp = sc.nextLine();
        	if (temp.compareTo(" ")<=0){
        		temp = selectStatement;
        	}
		    if(temp.compareTo(" ")>0){
		    	if(temp.trim().toLowerCase().compareTo("exit")==0){
		    		break;
		    	}
		    	selectStatement = temp;
		    	Map<Integer,Map<String,String>> results = rp2.get(selectStatement);
		    	
		    	for(Entry<Integer,Map<String,String>> entry : results.entrySet()){
		    		for(Entry<String,String> innerEntry : entry.getValue().entrySet() ){
		    			logger.info(innerEntry.getKey()+ " : " + innerEntry.getValue());
		    		}
		    	}
		    }
        }
        sc.close();
        rp2.close();
        
        System.exit(0);
	}


	
	/**
	 * 
	 * @param resultsMap - Map<Integer,Map<String,String>>  results from ReadJdbc.get operation
	 * @param colsToPrint - String[]  columns to print.  If null, print all.
	 * @return
	 */
	public List<String[]> resultsToCsv(Map<Integer,Map<String, String>> resultsMap,
			String[] colsToPrint){
		if(resultsMap==null || resultsMap.size()<1){
			return null;
		}
		
		List<String[]> ret = new ArrayList<>();
		String[] keys = colsToPrint;
		if(keys==null){
			keys = new TreeSet<>(resultsMap.get(0).keySet()).toArray(new String[]{});
		}
		ret.add(keys);
		for(Map<String,String> innerMap : resultsMap.values()){
			String[] line = new String[keys.length];
			for(int i = 0;i<keys.length;i++){
				String key = keys[i];
				String value = innerMap.get(key);
				line[i] = value;
			}
			ret.add(line);
		}
		return ret;
	}
	
	public static final void logCsv(List<String[]> csvList,Logger logger){
		for(String[] arr : csvList){
			String line= "";
			for(int i = 0;i<arr.length;i++){
				line += arr[i]+",";
			}
			line = line.substring(0,line.length()-1);
			logger.info(line);
		}
	}
	
	/**
	 * Copy a csv file to a database table, where the column header
	 *   has the same exact names as the database table
	 *   
	 * @param file
	 * @param tableName
	 * @return
	 */
	public void copyCsvToDb(List<String[]> csvList,String tableName,String csvDelimiter){
        CopyManager copyManager=null;
		try {
			copyManager = new CopyManager((BaseConnection) con);
		} catch (SQLException e1) {
			throw new IllegalStateException(e1);
		}
        String[] header = csvList.get(0);
        String columnNameString = header[0];
        for(int i = 1;i<header.length;i++){
        	columnNameString +=  "," +  header[i];
        }
        StringBuilder builder = new StringBuilder();
		for(int i = 1;i<csvList.size();i++){
			String[] csv = csvList.get(i);
			String csvLine = csv[0] ;
			for(int j=1;j<csv.length;j++){
				String token = csv[j];
//				if(token.contains("\"")){
//					token = token.replace("\"", "\\\"");
//				}
//				if(token.contains("'")){
//					token = token.replace("'", "\\'");
//				}
				csvLine += csvDelimiter + token ;//csv[j];
			}
			builder.append(csvLine+"\n");
		}
		Reader srd = new StringReader(builder.toString());
		
		String copyString = "COPY " + tableName + 
				"(" + columnNameString + ") FROM STDIN  DELIMITERS '" + csvDelimiter + "'   CSV"; //QUOTE '\"'  ESCAPE E'\\' CSV 		
        try {
			copyManager.copyIn(copyString, srd );
		} catch (SQLException | IOException e) {
			throw new IllegalStateException(e);
		}


	}
	

	public static Map<String,String> getArgPairsSeparatedByChar(String[] args,String separator){
		Map<String, String> argPairs = new HashMap<String, String>();
		if(args!=null){
			// find pairs separated by the = sign
			for(String argPair : args){
				String[] pair = argPair.split("=");
				if(pair.length>1){
					argPairs.put(pair[0],pair[1]);
				}
			}
		}
		return argPairs;
	}

}
