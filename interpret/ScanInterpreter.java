package edu.buffalo.cse.sql.interpret;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;


import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.data.Datum;

public class ScanInterpreter {

	public static Map<String, List<Datum[]>> mpScanned = new HashMap<String, List<Datum[]>>();

	public static void interpreteScan() {

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
