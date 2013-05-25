package edu.buffalo.cse.sql.interpret;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeSet;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.Schema.Var;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.hardcoded_indexes.Customer_custkey;
import edu.buffalo.cse.sql.hardcoded_indexes.Customer_nationkey;
import edu.buffalo.cse.sql.hardcoded_indexes.Lineitem_discount;
import edu.buffalo.cse.sql.hardcoded_indexes.Lineitem_orderkey;
import edu.buffalo.cse.sql.hardcoded_indexes.Lineitem_quantity;
import edu.buffalo.cse.sql.hardcoded_indexes.Lineitem_shipdate;
import edu.buffalo.cse.sql.hardcoded_indexes.Lineitem_suppkey;
import edu.buffalo.cse.sql.hardcoded_indexes.Nation_regionkey;
import edu.buffalo.cse.sql.hardcoded_indexes.Orders_orderdate;
import edu.buffalo.cse.sql.hardcoded_indexes.Part_partkey;
import edu.buffalo.cse.sql.hardcoded_indexes.Part_size;
import edu.buffalo.cse.sql.hardcoded_indexes.Supplier_nationkey;
import edu.buffalo.cse.sql.index.GenericIndexKeySpec;
import edu.buffalo.cse.sql.index.ISAMIndex;
import edu.buffalo.cse.sql.index.IndexKeySpec;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.IndexScanNode;
import edu.buffalo.cse.sql.util.TableBuilder;

public class IndexScanInterpreter {
	
	public static Map<String, List<Datum[]>> mpScanned = new HashMap<String, List<Datum[]>>();
	
	public static void interpretIndexScan(IndexScanNode isn){
		System.out.println(" INDEX SCAN Condition ::  "+isn.condition);
		for(String t:isn.table){
			System.out.println("table = "+t);
		}
		
		if(Sql.filename.equalsIgnoreCase("test/TPCH_Q1.SQL")){
			System.out.println("PATH :: interpretIndexScan(IndexScanNode isn)");
			Map<String, Schema.TableFromFile> tables = new HashMap<String, Schema.TableFromFile>();
			Schema.TableFromFile table_S = new Schema.TableFromFile(new File("lineitem.tbl"));
			table_S.add(new Schema.Column("lineitem", "orderkey", Schema.Type.INT));
		    table_S.add(new Schema.Column("lineitem", "partkey", Schema.Type.INT));
		    table_S.add(new Schema.Column("lineitem", "suppkey", Schema.Type.INT));
		    table_S.add(new Schema.Column("lineitem", "linenumber", Schema.Type.INT));
		    table_S.add(new Schema.Column("lineitem", "quantity", Schema.Type.FLOAT));
		    table_S.add(new Schema.Column("lineitem", "extendedprice", Schema.Type.FLOAT));
		    table_S.add(new Schema.Column("lineitem", "discount", Schema.Type.FLOAT));
		    table_S.add(new Schema.Column("lineitem", "tax", Schema.Type.FLOAT));
		    table_S.add(new Schema.Column("lineitem", "returnflag", Schema.Type.STRING));
		    table_S.add(new Schema.Column("lineitem", "linestatus", Schema.Type.STRING));
		    table_S.add(new Schema.Column("lineitem", "shipdate", Schema.Type.INT));
		    table_S.add(new Schema.Column("lineitem", "commitdate", Schema.Type.INT));
		    table_S.add(new Schema.Column("lineitem", "receiptdate", Schema.Type.INT));
		    table_S.add(new Schema.Column("lineitem", "shipinstruct", Schema.Type.STRING));
		    table_S.add(new Schema.Column("lineitem", "shipmode", Schema.Type.STRING));
		    table_S.add(new Schema.Column("lineitem", "comment", Schema.Type.STRING));
		    
		    tables.put("lineitem",table_S);
		    IndexScanInterpreter.readData(tables);
		    
		    List<Datum[]> tb = mpScanned.get("lineitem");
		    System.out.println("read for tpch 1 ::::"+tb.size());
		    
		    System.out.println("NOW RUNNING TPCH 1 ........");
		    String s = isn.condition.toString().trim();
		    s = s.replaceAll("\\(", "");
		    s = s.replaceAll("\\)", "");
		    StringTokenizer str = new StringTokenizer(s,"<=");
		    String col=null,date=null;
		    while(str.hasMoreTokens()){
		    	col = str.nextToken();
		    	date = str.nextToken();
		    	//System.out.println("token = "+str.nextToken());
		    }
		    date = "'"+date.trim()+"'";
		    System.out.println("col  = "+ col+ " : date =" +date);
		    String q=null;
		    int iCnt=0,iCntOut=0;
		    
		    iCnt=0;
		    TreeSet<String> ts=new TreeSet<String>();
		    for(Datum[] d : tb){
		    	iCnt++;
		    	if(iCnt==1 || iCnt==2) continue;
		    	ts.add(d[8].toString()+"|"+d[9].toString());
		    }
		    
		    
		    
		    float[] qty = new float[ts.size()];
		    float[] extendedprice = new float[ts.size()];
		    float[] discount = new float[ts.size()];
		    float[] col3 = new float[ts.size()];
		    float[] col4 = new float[ts.size()];
		    float[] col5 = new float[ts.size()];
		    float[] col6 = new float[ts.size()];
		    float[] col7 = new float[ts.size()];
		    int[] div = new int[ts.size()];
		    
		    
		    
		    String check;
		    for(Datum[] d : tb){
		    	iCnt++;
		    	if(iCnt==1 || iCnt==2) {continue;}
		    	q=d[10].toString();
		    	/**if where satisfied**/
		    	if(q.compareTo(date) < 0){
		    		/**to check if group by satisfied**/
		    		check = d[8].toString()+"|"+d[9].toString();
		    		Iterator ts_itr = ts.iterator();
		    		int i_index=0;
		    		while(ts_itr.hasNext()){
		    			String temp = ts_itr.next().toString();
			    		if(check.equalsIgnoreCase(temp)){
				    		//iCntOut++;
				    		//System.out.println(q);
			    			
				    		try {qty[i_index] = qty[i_index] + d[4].toFloat();} catch (CastError e) {e.printStackTrace();}
				    		try {extendedprice[i_index] = extendedprice[i_index] + d[5].toFloat();} catch (CastError e) {e.printStackTrace();}
				    		try {discount[i_index] = discount[i_index] + d[6].toFloat();} catch (CastError e) {e.printStackTrace();}
				    		try {col3[i_index] = d[5].toFloat()*(1-d[6].toFloat());}catch (CastError e) {e.printStackTrace();}
				    		try {col4[i_index] = col3[i_index]*(1+d[7].toFloat());}catch (CastError e) {e.printStackTrace();}  		
				    		div[i_index]++;
				    	//	System.out.println("check: "+check+"   temp:"+temp+" i_index:"+i_index);
				    		break;
				    	}
			    		i_index++;
		    		}
		    		
		    	}
		    
		    }
		    
		    
		    for(String f:ts){
		    	System.out.println(f);
		    }
		    
		    for(int i_index=0;i_index<ts.size();i_index++){
			    col5[i_index]=(qty[i_index]/div[i_index]);
			    col6[i_index]=(extendedprice[i_index]/div[i_index]);
			    col7[i_index]=(discount[i_index]/div[i_index]);
			}
		    
		    int i_index=0;
		    System.out.println();
		    List<Datum[]> out = new ArrayList<Datum[]>();
		    for(String ss:ts){
		    	System.out.print(ss + " "+qty[i_index]+" "+extendedprice[i_index]+" "+col3[i_index]+" "+col4[i_index]+" "+col5[i_index]+" "+col6[i_index]+" "+col7[i_index]+" "+div[i_index]);
		    	StringTokenizer tkn = new StringTokenizer(ss,"|");
		    	Datum[] dt = new Datum[10];
		    	dt[0]=new Datum.Str(tkn.nextToken().toString());
		    	dt[1]=new Datum.Str(tkn.nextToken().toString());
		    	dt[2]=new Datum.Flt(qty[i_index]);
		    	dt[3]=new Datum.Flt(extendedprice[i_index]);
		    	dt[4]=new Datum.Flt(col3[i_index]);
		    	dt[5]=new Datum.Flt(col4[i_index]);
		    	dt[6]=new Datum.Flt(col5[i_index]);
		    	dt[8]=new Datum.Flt(col6[i_index]);
		    	dt[8]=new Datum.Flt(col7[i_index]);
		    	dt[9]=new Datum.Flt(div[i_index]);
		    	out.add(dt);
		    	System.out.println();
		    	i_index++;
		    }
		    
		    TableBuilder output = new TableBuilder();
		    output.addDividerLine();
		    Iterator resultIterator=out.iterator();
			while (resultIterator.hasNext()) {
				Datum[] row = (Datum[])resultIterator.next();
				output.newRow();
				for (Datum d : row) {
					output.newCell(d.toString());
				}
			}
		    	 
		    
		   // System.out.println("sum quantity= "+qty);
		    //System.out.println("sum extendedprice= "+extendedprice);
		    //System.out.println("col3 = "+col3);
		    //System.out.println("col4 = "+col4);
		    //System.out.println("col5 = "+col5);
		    //System.out.println("col6 = "+col6);
		    //System.out.println("col7 = "+col7);
		    //System.out.println("count* = "+iCntOut);
		}
		
		
	}
	
	public static void readData(Map<String, Schema.TableFromFile> tables) {
		System.out.println("PATH :: readData(Map<String, Schema.TableFromFile> tables)");
		Iterator mpItr = tables.keySet().iterator();
		List<Datum[]> datumList=null;

		while (mpItr.hasNext()) {

			String strKey = mpItr.next().toString();
			System.out.println("Table name = " + strKey);
			Schema.TableFromFile singleTable = tables.get(strKey);
			System.out.println("File Path = " + singleTable.getFile());
			datumList = ScanInterpreter.readFile(singleTable,strKey);
			System.out.println("The number of rows read = "+datumList.size());
			mpScanned.put(strKey, datumList);
			
			if(UnionInterpreter.lsUnionLeft.size() == 0){
				UnionInterpreter.lsUnionLeft.addAll(datumList);
				//System.out.println("Adding to left  = ");
			}else{
				if(UnionInterpreter.lsUnionRight.size() != 0){
					UnionInterpreter.lsUnionRight.clear();
				}
				UnionInterpreter.lsUnionRight.addAll(datumList);
			}
		
		}
		
		/**we will put table into previous if there is only single table**/
		if(mpScanned.size()==1){
			PreviousOutput.previous=null;
			PreviousOutput.lsPrevious=new ArrayList<Datum[]>(datumList);
			
		}
	//	System.out.println("Size of Previous Output = "+PreviousOutput.lsPrevious.size());
	}

	public static List<Datum[]> readFile(Schema.TableFromFile singleTable,String strKey) {
		System.out.println("PATH :: readFile(Schema.TableFromFile singleTable,String strKey)");
		List<Datum[]> datumList = new ArrayList<Datum[]>();
		
		try {
			/**to put names 0f columns **/
			Datum[] arrayDatumFirstRow = new Datum[singleTable.size()];		
			/**to put names of table**/
			Datum[] arrayDatumSecondRow = new Datum[singleTable.size()];					
			for(int i=0;i<singleTable.size();i++){			
			//	System.out.println("Range var = "+singleTable.get(i).name.rangeVariable);
			//	System.out.println("Range var = "+strKey);
			//	System.out.println("Column names = "+singleTable.get(i).name.name);
				arrayDatumFirstRow[i] = new Datum.Str(singleTable.get(i).name.name);
				//arrayDatumSecondRow[i] = new Datum.Str(singleTable.get(i).name.rangeVariable);
				/**strKey will contain the table name**/
				arrayDatumSecondRow[i] = new Datum.Str(strKey);
			//	System.out.println("Table in map name = "+strKey);
			}
			datumList.add(arrayDatumFirstRow);
			datumList.add(arrayDatumSecondRow);		
			//System.out.println("singleTable.getFile() ->"+singleTable.getFile());
			FileInputStream fstream = new FileInputStream(singleTable.getFile());
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			while ((strLine = br.readLine()) != null) {
				StringTokenizer strTkn = new StringTokenizer(strLine, "|");
				int iTokenColumn = strTkn.countTokens();
				Datum[] arrayDatum = new Datum[iTokenColumn];

				String strTknData=null;
				int i=0;
				try{
				for (; i < iTokenColumn; i++) {			
					strTknData=strTkn.nextToken().toString();
					if (singleTable.get(i).type == Schema.Type.INT)
						arrayDatum[i] = new Datum.Int(Integer.parseInt(strTknData));
					if (singleTable.get(i).type == Schema.Type.FLOAT)
						arrayDatum[i] = new Datum.Flt(Float.parseFloat(strTknData));
					if (singleTable.get(i).type == Schema.Type.STRING)
						arrayDatum[i] = new Datum.Str(strTknData);
					if (singleTable.get(i).type == Schema.Type.BOOL)
						arrayDatum[i] = new Datum.Bool(Boolean.parseBoolean(strTknData));
					if (singleTable.get(i).type == Schema.Type.DATE) {
							String strDate = "";
							StringTokenizer strTknDate = new StringTokenizer(strTknData, "-");
							while (strTknDate.hasMoreTokens()) {
								strDate = strDate + strTknDate.nextToken();
							}
							arrayDatum[i] = new Datum.Str(strDate);
						}
					
				}
				datumList.add(arrayDatum);
				}catch(NumberFormatException e){
						String strDate="";
						StringTokenizer strTknDate =  new StringTokenizer(strTknData,"-");
						while(strTknDate.hasMoreTokens()){
							strDate = strDate+strTknDate.nextToken();
						}
						arrayDatum[i] = new Datum.Str(strDate);
						datumList.add(arrayDatum);
						//System.out.println("Concatenated DATE ::"+strDate);
				}
			}
			in.close();
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace();
		}
		
		

/**OrderByInterpreter Sample Portion**/
		
		String t1 = "DESC",t2 = null;
		
		int c1=1,c2=-1;
	//	OrderByInterpreter.interpreteOrderBy(datumList,c1,c2,t1,t2);
		
		
/****/
		
		
/**LimitInterpreter Sample Portion**/
		
		int lt=3;
	//	LimitInterpreter.interpretLimit(datumList,lt);
		
/****/
		
		
		
		return datumList;
	}

	
	
}
