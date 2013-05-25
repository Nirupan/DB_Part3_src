package edu.buffalo.cse.sql.interpret;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.StringTokenizer;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.data.*;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.plan.AggregateNode;
import edu.buffalo.cse.sql.plan.AggregateNode.AggColumn;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.ExprTree.OpCode;
import edu.buffalo.cse.sql.plan.PlanNode;

public class AggregateInterpreter {

	public static List<Datum[]> lsAgg = new ArrayList<Datum[]>();
	static int iAggOutputNoOfCol;
	static int iCurrentOutputColIndex;

	public static void interpreteAggregates(AggregateNode aggrNode){
		System.out.println("***********Interpreting Multiple Aggregates***********");
		
		iAggOutputNoOfCol=aggrNode.getAggregates().size();
		iCurrentOutputColIndex=0;
		ExprTree.OpCode extraExpress= null;
		Datum[] aggrOutput = new Datum[iAggOutputNoOfCol];
		
		System.out.println("o/p size "+iAggOutputNoOfCol);
		Iterator itr=aggrNode.getAggregates().iterator();
		String strCol=null;
		while(itr.hasNext()){
			AggColumn col = (AggColumn)itr.next();
			strCol="'"+col.expr.toString().trim()+"'";
			System.out.println("FOR COLUMN = "+strCol);
		    if(col.aggType==AggregateNode.AType.SUM){
		    	
		    	if(col.expr.size()==0){
		    		System.out.println("PATH :: col.expr.size()==0");
		    		if(strCol.contains(".")){
		    			StringTokenizer strTkn = new StringTokenizer(strCol,".");
		    			strTkn.nextToken();
		    			strCol = "'"+strTkn.nextToken().toString();
		    		}
		    	    findSum(strCol,aggrOutput,PreviousOutput.lsPrevious);
		    	}else{
		    		System.out.println("PATH :: col.expr.size()!=0");
		    		//get columns from expression such as expr (A + B) will give col1='A' and col2='B' 
		    		String col1="'"+col.expr.get(0).toString().trim()+"'";
		    		String col2="'"+col.expr.get(1).toString().trim()+"'";
		    		System.out.println("COL1 = "+col1);
		    		System.out.println("COL2 = "+col2);
		    		//get the opertor, here, it will give + for (A + B) ExprTree
		    		ExprTree.OpCode opcode = col.expr.op;
		    		String strColNameToBeGiven=col1+opcode.sep+col2; 
		    		System.out.println("LENGTH OF EXPR = "+col.expr.get(1).size());
		    		
		    		if(col2.contains("-")){
		    			extraExpress = ExprTree.OpCode.SUB;
		    		}
		    		if(col2.contains("+")){
		    			extraExpress = ExprTree.OpCode.ADD;
		    		}
		    		
		    		List<Datum[]> ls = null;
		    		if(col.expr.get(0).size() >0){
		    			String strCol11 = col.expr.get(0).get(0).toString();
		    			String strCol21 = col.expr.get(0).get(1).get(1).toString();
		    			String strCol22 = col.expr.get(1).get(1).toString();

		    			System.out.println("strCol11 == "+strCol11+" strCol21 == "+strCol21+" strCol22 == "+strCol22);
		    			if(Sql.isTPCH){
		    				ls = spComputeColumnExpression(strCol11,strCol21, strCol22, opcode, strColNameToBeGiven,ExprTree.OpCode.ADD);
		    			}
		    		}else if(col.expr.get(1).size() >0){
		    			int iCol21 = Integer.parseInt(col.expr.get(1).get(0).toString());
		    			String strCol22 = col.expr.get(1).get(1).toString();
		    			System.out.println("iCol21 = "+iCol21+" "+col.expr.get(1).op+" strCol22 = "+strCol22);
		    			ls = computeColumnExpression(col1, strCol22, opcode, strColNameToBeGiven,extraExpress);
		    		}
		    		else{
		    		//compute A + B and get result in list
		    			ls = computeColumnExpression(col1, col2, opcode, strColNameToBeGiven,extraExpress);
		    		}
		    		//usual SUM for column (A + B), thus the output is SUM(A + B)
		    		strCol="'"+strColNameToBeGiven+"'";
		    		findSum(strCol, aggrOutput, ls);
		    		

		    	}
	    	
		    }
		    if(col.aggType==AggregateNode.AType.AVG){
		    	if(col.expr.size()==0){
		    		findAVG(strCol,aggrOutput,PreviousOutput.lsPrevious);
		    	}else{
		    	
		    		//get the two columns in expression e.g A+B e.g AGG10
		    		String col1="'"+col.expr.get(0).toString().trim()+"'";
		    		String col2="'"+col.expr.get(1).toString().trim()+"'";
		    		ExprTree.OpCode opcode = col.expr.op;
		    		String strColNameToBeGiven=col1+opcode.sep+col2;
		    		
		    		List<Datum[]> ls = computeColumnExpression(col1, col2, opcode, strColNameToBeGiven,null);
		    		
		    		// o/p is AVG(A + B)
		    		strCol="'"+strColNameToBeGiven+"'";
	    			findAVG(strCol, aggrOutput, ls);
		    		
		    	}
		    }
		    if(col.aggType==AggregateNode.AType.COUNT){
		    		findCount(strCol,aggrOutput,PreviousOutput.lsPrevious);
		    }
		    if(col.aggType==AggregateNode.AType.MIN){
		    		findMin(strCol,aggrOutput,PreviousOutput.lsPrevious);
		    }
		    if(col.aggType==AggregateNode.AType.MAX){
		    		findMax(strCol,aggrOutput,PreviousOutput.lsPrevious);
		    }
			System.out.println(col.expr.toString().trim());
			
			}
		
	}
	
	static List<Datum[]> computeColumnExpression(String col1,String col2,ExprTree.OpCode opcode,String strColNameToBeGiven,ExprTree.OpCode extraExpress){
		System.out.println("PATH :: computeColumnExpression(String col1,String col2,ExprTree.OpCode opcode,String strColNameToBeGiven)");
		/**change this..**/
		if(col1.contains(".")){
			StringTokenizer strTkn = new StringTokenizer(col1,".");
			strTkn.nextToken();
			col1 = "'"+strTkn.nextToken().toString();
		}
		System.out.println(" col1 = "+col1);
		int iCol1 = getColumnPosition(col1,PreviousOutput.lsPrevious);
		System.out.println("iposition = "+iCol1);
		if(col2.contains(".")){
			StringTokenizer strTkn = new StringTokenizer(col2,".");
			strTkn.nextToken();
			col2 = "'"+strTkn.nextToken().toString()+"'";
		}
		System.out.println(" col2 = "+col2);
		
		StringTokenizer strTkn=null;
		int iCol2 = getColumnPosition(col2,PreviousOutput.lsPrevious);
		System.out.println("iposition = "+iCol2);
				
		List<Datum[]> ls = null;
		if(opcode==ExprTree.OpCode.ADD){
			ls = add(iCol1,iCol2,strColNameToBeGiven);
			String strCol="'"+strColNameToBeGiven+"'";
		}
		if(opcode==ExprTree.OpCode.MULT){
			ls = multiply(iCol1,iCol2,Integer.MIN_VALUE,strColNameToBeGiven,extraExpress);
			String strCol="'"+strColNameToBeGiven+"'";
		}
		
		return ls;
		
	}
	
	static List<Datum[]> add(int iCol1,int iCol2,String strColNameToBeGiven){
		List<Datum[]> ls=new ArrayList<Datum[]>();
		
		try{
		Iterator itr = PreviousOutput.lsPrevious.iterator();
		itr.next();
		itr.next();
		while(itr.hasNext()){
			
			Datum[] temp = (Datum[])itr.next();
			Datum[] rslt= new Datum[1];
			rslt[0]= new Datum.Int(temp[iCol1].toInt()+temp[iCol2].toInt());
		    ls.add(rslt);	
		}
		
		/**adding column name like A+B **/
		Datum[] colName=new Datum[1];
		/**Str method adds ' ' so, ' strColNameToBeGiven ' is stored instead of just strColNameToBeGiven**/
		colName[0]=new Datum.Str(strColNameToBeGiven);
		Datum[] tableName=new Datum[1];
		tableName[0]=new Datum.Str("add");
		
		ls.add(0,colName);
		ls.add(1,tableName);
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return ls;
	}
	
	static List<Datum[]> multiply(int iCol1,int iCol2,int iCol3,String strColNameToBeGiven,ExprTree.OpCode extraExpress){
		List<Datum[]> ls=new ArrayList<Datum[]>();
		
		try{
		Iterator itr = PreviousOutput.lsPrevious.iterator();
		itr.next();
		itr.next();
		Datum[] temp=null;
		Datum[] rslt=null;
		while(itr.hasNext()){
			try{
			temp = (Datum[])itr.next();
			rslt= new Datum[1];
			rslt[0]= new Datum.Int(temp[iCol1].toInt()*temp[iCol2].toInt());
		    ls.add(rslt);
			}catch(CastError e){

				if(extraExpress == ExprTree.OpCode.SUB){
					rslt[0]= new Datum.Flt(temp[iCol1].toFloat()*(1-temp[iCol2].toFloat()));
				}else if(extraExpress == ExprTree.OpCode.ADD){
					rslt[0]= new Datum.Flt(temp[iCol1].toFloat()*(1-temp[iCol2].toFloat())*(1+temp[iCol3].toFloat()));
				}
			    ls.add(rslt);

			}
		}
		
		/**adding column name like A+B **/
		Datum[] colName=new Datum[1];
		/**Str method adds ' ' so, ' strColNameToBeGiven ' is stored instead of just strColNameToBeGiven**/
		colName[0]=new Datum.Str(strColNameToBeGiven);
		Datum[] tableName=new Datum[1];
		tableName[0]=new Datum.Str("mult");
	
		ls.add(0,colName);
		System.out.println("COLNAME IN MULT = "+strColNameToBeGiven);
		ls.add(1,tableName);
		
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return ls;
	}
	
	static void findCount(String strCol,Datum[] aggrOutput,List<Datum[]> ls){
		if (ls.size() > 0) {
			aggrOutput[iCurrentOutputColIndex] = new Datum.Int(ls.size() - 2);
			iCurrentOutputColIndex++;
			AggregateInterpreter.lsAgg.add(aggrOutput);
		
		}
	}
	
	static void findAVG(String strCol,Datum[] aggrOutput,List<Datum[]> ls){
		System.out.println("PATH :: findAVG(String strCol,Datum[] aggrOutput,List<Datum[]> ls)");
		System.out.println("------------------------------------------------------------------");
		System.out.println("strCol = "+strCol);
		if(strCol.contains(".")){
			StringTokenizer strTkn = new StringTokenizer(strCol,".");
			strTkn.nextToken();
			strCol = "'"+strTkn.nextToken().toString();
		}
		//AggregateInterpreter.findSum(strCol,aggrOutput);
		int iColPos = AggregateInterpreter.getColumnPosition(strCol,ls);
		int iSum=0;
		float fSum=0;
		Iterator itr = ls.iterator();
		/**to skip colnames row**/
		itr.next();
		/**to skip table names row**/
		itr.next();
		Datum[] temp=null;
		while (itr.hasNext()) {
			temp = (Datum[]) itr.next();
			try {
				iSum = iSum + temp[iColPos].toInt();
			} catch (CastError e) {
				// TODO Auto-generated catch block
				try {
					fSum = fSum + temp[iColPos].toFloat();
				} catch (CastError e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

		}
		System.out.println("SUMMMMM = " + iSum);
		System.out.println("SUMMM ===" + fSum);
		int iSize=ls.size()-2;
		
		/**retrieves sum from lsAgg set by findSum**/
		double dAvg = 0.0;
		if(fSum!=0){
			dAvg = fSum/iSize;
		}else{
			dAvg = (double) ((double) iSum / (double) iSize);
		}
		System.out.println("iSum" + iSum);
		System.out.println("isize" + iSize);
		System.out.println("dAVG=" + dAvg);
		aggrOutput[iCurrentOutputColIndex] = new Datum.Flt(dAvg);
		iCurrentOutputColIndex++;
		AggregateInterpreter.lsAgg.clear();
		AggregateInterpreter.lsAgg.add(aggrOutput);

	}
	
	static int getColumnPosition(String strCol,List<Datum[]> ls) {
		Datum[] colNames = ls.get(0);
		int i = 0;
		for (; i < colNames.length; i++) {
			//System.out.println("colNames[i].toString()="
			//		+ colNames[i].toString());
			//System.out.println("col == "+colNames[i].toString().trim());
			if (strCol.equalsIgnoreCase(colNames[i].toString().trim())) {
				System.out.println("MATCHED COLUMN NAME");
				break;
			}
		}
		return i;

	}

	static void findMax(String strCol,Datum[] aggrOutput,List<Datum[]> ls) {
		int iColPos = AggregateInterpreter.getColumnPosition(strCol,ls);
		try {
			Iterator itr = ls.iterator();
			boolean bSkipFirstRow = false;
			int iMax = Integer.MIN_VALUE;
			itr.next();
			itr.next();
			while (itr.hasNext()) {
				Datum[] temp = (Datum[]) itr.next();
				if (temp[iColPos].toInt() > iMax) {
					iMax = temp[iColPos].toInt();
				}
			}
			aggrOutput[iCurrentOutputColIndex] = new Datum.Int(iMax);
			iCurrentOutputColIndex++;
			AggregateInterpreter.lsAgg.add(aggrOutput);

		} catch (CastError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	static void findMin(String strCol,Datum[] aggrOutput,List<Datum[]> ls) {
		int iColPos = AggregateInterpreter.getColumnPosition(strCol,ls);
		try {
			Iterator itr = ls.iterator();
			boolean bSkipFirstRow = false;
			int iMin = Integer.MAX_VALUE;
			itr.next();
			itr.next();
			while (itr.hasNext()) {
				Datum[] temp = (Datum[]) itr.next();
				if (temp[iColPos].toInt() < iMin) {
					iMin = temp[iColPos].toInt();
				}
			}
			aggrOutput[iCurrentOutputColIndex] = new Datum.Int(iMin);
			iCurrentOutputColIndex++;
			AggregateInterpreter.lsAgg.add(aggrOutput);

		} catch (CastError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	static void findSum(String strCol,Datum[] aggrOutput,List<Datum[]> ls) {
		System.out.println("PATH :: findSum(String strCol,Datum[] aggrOutput,List<Datum[]> ls)");
		System.out.println("------------------------------------------------------------------");
		System.out.println(" col == "+strCol);
		
		System.out.println("PATH :: findSum(String strCol,Datum[] aggrOutput,List<Datum[]> ls) ");
		System.out.println("Column to be matched  = " +strCol);
		int iColPos = AggregateInterpreter.getColumnPosition(strCol,ls);
		System.out.println("Column position in sum = "+iColPos);
		int iSum=0;
		float fSum=0;
		boolean isFloat =false;
		Iterator itr = ls.iterator();
		/**to skip colnames row**/
		itr.next();
		/**to skip table names row**/
		itr.next();
		while (itr.hasNext()) {
			Datum[] temp = (Datum[]) itr.next();
			try {
				iSum = iSum + temp[iColPos].toInt();
			} catch (CastError e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				try {
					isFloat=true;
					fSum = fSum + temp[iColPos].toFloat();
				} catch (CastError e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

		}
		System.out.println("SUMM = "+iSum);
		if(isFloat){
			aggrOutput[iCurrentOutputColIndex] = new Datum.Flt(fSum);
			System.out.println("FOUND SUM = "+fSum);
		}else{
			aggrOutput[iCurrentOutputColIndex] = new Datum.Int(iSum);
		}
		iCurrentOutputColIndex++;
		AggregateInterpreter.lsAgg.add(aggrOutput);
	
	}

	
	static List<Datum[]> spComputeColumnExpression(String col1,String col2,String col3,ExprTree.OpCode opcode,String strColNameToBeGiven,ExprTree.OpCode extraExpress){
		System.out.println("PATH :: computeColumnExpression(String col1,String col2,ExprTree.OpCode opcode,String strColNameToBeGiven)");
		/**change this..**/
		if(col1.contains(".")){
			StringTokenizer strTkn = new StringTokenizer(col1,".");
			strTkn.nextToken();
			col1 = "'"+strTkn.nextToken().toString()+"'";
		}
		System.out.println(" col1 = "+col1);
		int iCol1 = getColumnPosition(col1,PreviousOutput.lsPrevious);
		System.out.println("iposition = "+iCol1);
		if(col2.contains(".")){
			StringTokenizer strTkn = new StringTokenizer(col2,".");
			strTkn.nextToken();
			col2 = "'"+strTkn.nextToken().toString()+"'";
		}
		System.out.println(" col2 = "+col2);
		int iCol2 = getColumnPosition(col2,PreviousOutput.lsPrevious);
		System.out.println("iposition = "+iCol2);
		if(col3.contains(".")){
			StringTokenizer strTkn = new StringTokenizer(col3,".");
			strTkn.nextToken();
			col3 = "'"+strTkn.nextToken().toString()+"'";
		}
		System.out.println(" col3 = "+col3);
		int iCol3 = getColumnPosition(col3,PreviousOutput.lsPrevious);
		System.out.println("iposition = "+iCol3);
	
		StringTokenizer strTkn=null;
				
		List<Datum[]> ls = null;
		if(opcode==ExprTree.OpCode.ADD){
			ls = add(iCol1,iCol2,strColNameToBeGiven);
			String strCol="'"+strColNameToBeGiven+"'";
		}
		if(opcode==ExprTree.OpCode.MULT){
			ls = multiply(iCol1,iCol2,iCol3,strColNameToBeGiven,extraExpress);
			String strCol="'"+strColNameToBeGiven+"'";
		}
		
		return ls;
		
	}
	
}
