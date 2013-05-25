package edu.buffalo.cse.sql.interpret;

import java.beans.Expression;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.plan.*;

public class SelectionInterpreter {

	public static List<Datum[]> lsSelect = new ArrayList<Datum[]>();
	static Stack<ExprTree> stack = new Stack<ExprTree>();
	static boolean flag = false;
	static boolean isOR  = false;
	static boolean isColNamesAdded = false;
	
	public static void interpreteSelectionMultiple(SelectionNode selectNode){
		System.out.println("PATH :: interpreting SELECTION  ---"+selectNode.getCondition().op);
		System.out.println("Size of Previous : "+PreviousOutput.lsPrevious.size());
		stack.push(selectNode.getCondition());
		
		/**need to discriminate between different conditions such as (1) A=B (2) A=B AND C=D (3) A=B OR C=D**/		
		if(selectNode.getCondition().allVars().size()>2){
			System.out.println("PATH :: selectNode.getCondition().allVars().size()>2");
				Collection<ExprTree> coll = selectNode.conjunctiveClauses();
				Iterator itr = coll.iterator();
				while (itr.hasNext()) {
					ExprTree exprTree = (ExprTree) itr.next();
					System.out.println("next expr tree= " + exprTree.op);
					/**dont push if its and internediate operator**/
					if(exprTree.op!=ExprTree.OpCode.EQ)
					 pushOnStack(exprTree);
			}
		if (stack != null) {
			System.out.println("PATH :: stack != null");
				while (!stack.isEmpty()) {
					popAndexecute();
				}
		}
			System.out.println("MULTIPLE ---------"+selectNode.conjunctiveClauses());
			PreviousOutput.previous=selectNode;
			//RelationReader.readRelation(PreviousOutput.lsPrevious);
			
		}else{
			System.out.println("PATH :: selectNode.getCondition().allVars().size()<2");
			System.out.println("single ------"+selectNode.getCondition());
			Collection<ExprTree> coll = selectNode.conjunctiveClauses();
			Iterator itr=coll.iterator();
			ExprTree exprTree = (ExprTree)itr.next();
			interpreteSelectionAND(exprTree,PreviousOutput.lsPrevious);
			PreviousOutput.previous=selectNode;
		}
	}
	
	static void pushOnStack(ExprTree exprTree) {
		System.out.println("pushing on stack :: " + exprTree.op);
		stack.push(exprTree);
	}
	
	static void popAndexecute() {
		System.out.println("Selection interpreter: popping ");
		ExprTree exprTree = (ExprTree)stack.pop();
		System.out.println("pop ->" + exprTree.op);
		
		if (exprTree.op == ExprTree.OpCode.AND) {
			isOR=false;
			Iterator itr = exprTree.iterator();
			while(itr.hasNext()){
				ExprTree eTree=(ExprTree)itr.next();		
				if(eTree.op != ExprTree.OpCode.OR)
				interpreteSelectionAND(eTree, PreviousOutput.lsPrevious);
			}
		}
		if (exprTree.op == ExprTree.OpCode.OR) {
			isOR=true;
			List<Datum[]> ls = new ArrayList<Datum[]>(PreviousOutput.lsPrevious);
			PreviousOutput.lsPrevious.clear();
			System.out.println("==================================================================OR="+exprTree.get(1).op);
			/**condition 0**/
			flag = true;
			Iterator itr=exprTree.iterator();
			while(itr.hasNext()){
				ExprTree eTree = (ExprTree)itr.next();
				interpreteSelectionOR(eTree, ls);
			//	RelationReader.readRelation(PreviousOutput.lsPrevious);
			}
		}
	}
	
	public static void interpreteSelectionOR(ExprTree tree,List<Datum[]> ls) {
		ExprTree.OpCode operator = null;
		List lsColumnsToBeUsed = new ArrayList();
		List lstableNamesToBeUsed = new ArrayList();
		System.out.println("tree operator ="+tree.op);
		operator = tree.op;
		Iterator itrr = tree.iterator();
		while (itrr.hasNext()) {
				ExprTree.VarLeaf leaf = (ExprTree.VarLeaf) itrr.next();
				// range variable gives table name
				/**Adityaimp, shows how to get tablename and column name in selectionNode**/
				String strTableName = leaf.name.rangeVariable;
				String strColName = leaf.name.name;
				lsColumnsToBeUsed.add(strColName);
				lstableNamesToBeUsed.add(strTableName);
				System.out.println("Table name :: OR "+strTableName);
				System.out.println("Col name :: OR "+strColName);
			}
			if (operator == ExprTree.OpCode.EQ) {
				SelectionInterpreter.implementEQ(lsColumnsToBeUsed,ls);
			}
			if (operator == ExprTree.OpCode.LT) {
				SelectionInterpreter.implementLT(lsColumnsToBeUsed,ls,lstableNamesToBeUsed);
			}
			if (operator == ExprTree.OpCode.GT) {
				SelectionInterpreter.implementGT(lsColumnsToBeUsed,ls,lstableNamesToBeUsed);
			}
	}
	
	
	public static void interpreteSelectionAND(ExprTree tree,List<Datum[]> ls) {
		System.out.println("PATH :: interpreteSelectionAND(ExprTree tree,List<Datum[]> ls)");
		boolean isOneValueConst =false;
		ExprTree.OpCode operator = null;
		List lsColumnsToBeUsed = new ArrayList();
		List lstableNamesToBeUsed = new ArrayList();
		//System.out.println("treee="+tree.op);
		operator = tree.op;
		Iterator itrr = tree.iterator();
		while (itrr.hasNext()) {
			
			Object obj = itrr.next();
			try{
				System.out.println("PATH :: VAR LEAF PATH");
				ExprTree.VarLeaf leaf=null;
				leaf = (ExprTree.VarLeaf) obj;
				// range variable gives table name
				/**Adityaimp, shows how to get tablename and column name in selectionNode**/
				String strTableName = leaf.name.rangeVariable;
				String strColName = leaf.name.name;
				
				/**adding <strTableName.strColName> to the list**/
				
				lsColumnsToBeUsed.add(strColName);
				lstableNamesToBeUsed.add(strTableName);
				System.out.println("Table name AND :: "+strTableName);
				System.out.println("Col name in OR :: "+strColName);
				
				
			}catch(ClassCastException e){
				System.out.println("PATH :: CONST LEAF PATH");
				ExprTree.ConstLeaf leaf = null;
				isOneValueConst = true;
				leaf = (ExprTree.ConstLeaf)obj;
				lsColumnsToBeUsed.add(leaf.v);
				System.out.println("Constant value to be compared = "+leaf.v);
				
			}
				
			}
			if (operator == ExprTree.OpCode.EQ) {
				SelectionInterpreter.implementEQ(lsColumnsToBeUsed,ls);
			}
			if (operator == ExprTree.OpCode.LT) {
				SelectionInterpreter.implementLT(lsColumnsToBeUsed,ls,lstableNamesToBeUsed);
			}
			if (operator == ExprTree.OpCode.LTE) {
				SelectionInterpreter.implementLTE(lsColumnsToBeUsed,ls, lstableNamesToBeUsed,isOneValueConst);
			}
	}

	static void implementLTE(List lsColumnsToBeUsed,List<Datum[]> ls,List lsTableNamesToBeUsed,boolean isOneValueConst){
		System.out.println("PATH :: implementLTE(List lsColumnsToBeUsed,List<Datum[]> ls)");
		int iCol1 = Integer.MIN_VALUE;
		int iCol2 = Integer.MIN_VALUE;
		String strCol2Name = null;
		String strTable2Name = null;
		String strDate=null;
		try {
			String strCol1Name = "'" + lsColumnsToBeUsed.get(0).toString().trim() + "'";
			String strTable1Name="'" + lsTableNamesToBeUsed.get(0).toString().trim()+"'";
			System.out.println("strCol1Name =" +strCol1Name+ ":: strTable1Name = "+strTable1Name);
			if(isOneValueConst){
				iCol2 = Integer.parseInt(lsColumnsToBeUsed.get(1).toString());
				strDate = lsColumnsToBeUsed.get(1).toString();
				System.out.println("Got const value = "+iCol2);
			} else {
				strCol2Name = "'"+ lsColumnsToBeUsed.get(1).toString().trim() + "'";
				strTable2Name = "'"	+ lsTableNamesToBeUsed.get(1).toString().trim() + "'";
			}
			Iterator itr = ls.iterator();
			Datum[] colNames = (Datum[]) itr.next();
			Datum[] tableNames = (Datum[]) itr.next();
		//	String strUnAliasedTableName1 = Sql.mpTableAlias.get(strTable1Name).toString();
		//	System.out.println("strUnAliasedTableName = "+strUnAliasedTableName1);
			if(Sql.mpAliasTable.size()>0){
				String strUnAliasedTableName=Sql.mpAliasTable.get(strTable1Name).toString();
				System.out.println("PATH :: Got unaliased table name : "+strTable1Name+ " :: "+strUnAliasedTableName);
				strTable1Name = strUnAliasedTableName;
			
			}
			for (int i = 0; i < colNames.length; i++) {
			//	System.out.println("table name = "+tableNames[i]);
			//	System.out.println("strTable1Name = "+strTable1Name);
				if (strCol1Name.equalsIgnoreCase(colNames[i].toString()) && strTable1Name.equalsIgnoreCase(tableNames[i].toString())){				//	if (iCol1 == Integer.MIN_VALUE) {
						iCol1 = i;
				}
				if (!isOneValueConst) {
					if (strCol2Name.equalsIgnoreCase(colNames[i].toString())
							&& strTable2Name.equalsIgnoreCase(tableNames[i]
									.toString())) {
						iCol2 = i;
					}
				}
			}

			/**to implement less than e.g like S.X <= T.C**/
			//System.out.println("Size of prev= "+ls.size());
			strDate="'"+strDate+"'";
			while (itr.hasNext()) {
				//System.out.println("PATH :: Iterating to compare");
				Datum[] temp = (Datum[]) itr.next();
				//System.out.println("date="+temp[iCol1].toString());
				if (isOneValueConst) {
					
					if (temp[iCol1].toString().compareTo(strDate) <= 0) {
						//System.out.println("temp[iCol1] =  "+temp[iCol1].toString()+ " : strDate = "+strDate);
						Datum[] selected = new Datum[temp.length];
						selected = temp;
						SelectionInterpreter.lsSelect.add(selected);
					}
				} else {
					if (temp[iCol1].toInt() <= temp[iCol2].toInt()) {
						Datum[] selected = new Datum[temp.length];
						selected = temp;
						SelectionInterpreter.lsSelect.add(selected);
					}
				}
			}
						
			/**Adding column names to final list**/
			Datum[] addColNames=new Datum[colNames.length];
			for(int k=0;k<colNames.length;k++){
				addColNames[k]=colNames[k];
			}
			/**Adding table names to final list**/
			Datum[] addTableNames=new Datum[tableNames.length];
			for(int k=0;k<tableNames.length;k++){
				addTableNames[k]=tableNames[k];
			}
			/**Adding colnames at start of datum list**/
			//SelectionInterpreter.lsSelect.add(0,addColNames);
			//SelectionInterpreter.lsSelect.add(1,addTableNames);
			
			if(!isColNamesAdded){
				PreviousOutput.lsPrevious.add(0,addColNames);
				PreviousOutput.lsPrevious.add(1,addTableNames);
				isColNamesAdded = true;
			}
			
			/**Setting previous list**/
			if(isOR){
				PreviousOutput.lsPrevious.addAll(SelectionInterpreter.lsSelect);
			}
			else{
				SelectionInterpreter.lsSelect.add(0,addColNames);
				SelectionInterpreter.lsSelect.add(1,addTableNames);
				PreviousOutput.lsPrevious=new ArrayList<Datum[]>(SelectionInterpreter.lsSelect);
			}
			SelectionInterpreter.lsSelect.clear();
			//System.out.println("Reading relation Previous..");
			//RelationReader.readRelation(PreviousOutput.lsPrevious);
		
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	
	
	}
	
	static void implementEQ(List lsColumnsToBeUsed,List<Datum[]> ls) {
		System.out.println("PATH :: implementEQ(List lsColumnsToBeUsed,List<Datum[]> ls)");
		try {
			String strCol1Name = "'" + lsColumnsToBeUsed.get(0).toString().trim() + "'";
			String strCol2Name = "'" + lsColumnsToBeUsed.get(1).toString().trim() + "'";
			int iCol1 = Integer.MIN_VALUE;
			int iCol2 = Integer.MIN_VALUE;
			Iterator itr = ls.iterator();
			Datum[] colNames = (Datum[]) itr.next();
			/**for col names**/
			for (int i = 0; i < colNames.length; i++) {
				if (strCol1Name.equalsIgnoreCase(colNames[i].toString())) {
					if (iCol1 == Integer.MIN_VALUE) {
						iCol1 = i;
					} else {
						iCol2 = i;
					}
				}
			}
			/**for table names**/
			Datum[] tableNames = (Datum[])itr.next();
			while (itr.hasNext()) {
				Datum[] temp = (Datum[]) itr.next();
				if (temp[iCol1].toInt() == temp[iCol2].toInt()) {
					Datum[] selected = new Datum[temp.length];
					selected = temp;
					SelectionInterpreter.lsSelect.add(selected);
				}
			}			
			/**Adding column names to final list**/
			Datum[] addColNames=new Datum[colNames.length];
			for(int k=0;k<colNames.length;k++){
				addColNames[k]=colNames[k];
			}
			/**Adding table names to final list**/
			Datum[] addTableNames=new Datum[colNames.length];
			for(int k=0;k<colNames.length;k++){
				addTableNames[k]=tableNames[k];
			}
			/**Adding colnames at start of datum list**/
			SelectionInterpreter.lsSelect.add(0,addColNames);
			SelectionInterpreter.lsSelect.add(1,addTableNames);
			
			/**Setting previous list**/
			//PreviousOutput.previous=selectNode;
			PreviousOutput.lsPrevious=new ArrayList<Datum[]>(SelectionInterpreter.lsSelect);
			SelectionInterpreter.lsSelect.clear();
			//System.out.println("Reading relation Previous..");
			//RelationReader.readRelation(PreviousOutput.lsPrevious);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	
	static void implementLT(List lsColumnsToBeUsed,List<Datum[]> ls,List lsTableNamesToBeUsed){
		System.out.println("PATH :: implementLT(List lsColumnsToBeUsed,List<Datum[]> ls,List lsTableNamesToBeUsed)");
		try {
			String strCol1Name = "'" + lsColumnsToBeUsed.get(0).toString().trim() + "'";
			String strTable1Name="'" + lsTableNamesToBeUsed.get(0).toString().trim()+"'";
			String strCol2Name = "'" + lsColumnsToBeUsed.get(1).toString().trim() + "'";
			String strTable2Name="'" + lsTableNamesToBeUsed.get(1).toString().trim()+"'";
			int iCol1 = Integer.MIN_VALUE;
			int iCol2 = Integer.MIN_VALUE;
			Iterator itr = ls.iterator();
			Datum[] colNames = (Datum[]) itr.next();
			Datum[] tableNames = (Datum[]) itr.next();
			for (int i = 0; i < colNames.length; i++) {
				if (strCol1Name.equalsIgnoreCase(colNames[i].toString()) && strTable1Name.equalsIgnoreCase(tableNames[i].toString())){				//	if (iCol1 == Integer.MIN_VALUE) {
						iCol1 = i;
				}
				if (strCol2Name.equalsIgnoreCase(colNames[i].toString()) && strTable2Name.equalsIgnoreCase(tableNames[i].toString())){
						iCol2 = i;
				}
			}

			/**to implement less than e.g like S.T < T.C**/
			while (itr.hasNext()) {
				Datum[] temp = (Datum[]) itr.next();
				if (temp[iCol1].toInt() < temp[iCol2].toInt()) {
					Datum[] selected = new Datum[temp.length];
					selected = temp;
					SelectionInterpreter.lsSelect.add(selected);
				}
			}
						
			/**Adding column names to final list**/
			Datum[] addColNames=new Datum[colNames.length];
			for(int k=0;k<colNames.length;k++){
				addColNames[k]=colNames[k];
			}
			/**Adding table names to final list**/
			Datum[] addTableNames=new Datum[tableNames.length];
			for(int k=0;k<tableNames.length;k++){
				addTableNames[k]=tableNames[k];
			}
			/**Adding colnames at start of datum list**/
			//SelectionInterpreter.lsSelect.add(0,addColNames);
			//SelectionInterpreter.lsSelect.add(1,addTableNames);
			
			if(!isColNamesAdded){
				PreviousOutput.lsPrevious.add(0,addColNames);
				PreviousOutput.lsPrevious.add(1,addTableNames);
				isColNamesAdded = true;
			}
			
			/**Setting previous list**/
			if(isOR){
				PreviousOutput.lsPrevious.addAll(SelectionInterpreter.lsSelect);
			}
			else{
				SelectionInterpreter.lsSelect.add(0,addColNames);
				SelectionInterpreter.lsSelect.add(1,addTableNames);
				PreviousOutput.lsPrevious=new ArrayList<Datum[]>(SelectionInterpreter.lsSelect);
			}
			SelectionInterpreter.lsSelect.clear();
			//System.out.println("Reading relation Previous..");
			//RelationReader.readRelation(PreviousOutput.lsPrevious);
		
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	
	}
	
	static void implementGT(List lsColumnsToBeUsed,List<Datum[]> ls,List lsTableNamesToBeUsed){

		try {
			String strCol1Name = "'" + lsColumnsToBeUsed.get(0).toString().trim() + "'";
			String strTable1Name="'" + lsTableNamesToBeUsed.get(0).toString().trim()+"'";
			String strCol2Name = "'" + lsColumnsToBeUsed.get(1).toString().trim() + "'";
			String strTable2Name="'" + lsTableNamesToBeUsed.get(1).toString().trim()+"'";
			int iCol1 = Integer.MIN_VALUE;
			int iCol2 = Integer.MIN_VALUE;
			Iterator itr = ls.iterator();
			Datum[] colNames = (Datum[]) itr.next();
			Datum[] tableNames = (Datum[]) itr.next();
			for (int i = 0; i < colNames.length; i++) {
				if (strCol1Name.equalsIgnoreCase(colNames[i].toString()) && strTable1Name.equalsIgnoreCase(tableNames[i].toString())) {
						iCol1 = i;
				}
				if (strCol2Name.equalsIgnoreCase(colNames[i].toString()) && strTable2Name.equalsIgnoreCase(tableNames[i].toString())){ 
						iCol2 = i;
				}
			}

			/**to implement less than e.g like S.T < T.C**/
			while (itr.hasNext()) {
				Datum[] temp = (Datum[]) itr.next();
				if (temp[iCol1].toInt() > temp[iCol2].toInt()) {
					Datum[] selected = new Datum[temp.length];
					selected = temp;
					SelectionInterpreter.lsSelect.add(selected);
				}
			}
			/**Adding column names to final list**/
			Datum[] addColNames=new Datum[colNames.length];
			for(int k=0;k<colNames.length;k++){
				addColNames[k]=colNames[k];
			}
			/**Adding table names to final list**/
			Datum[] addTableNames=new Datum[tableNames.length];
			for(int k=0;k<tableNames.length;k++){
				addTableNames[k]=tableNames[k];
			}
			/**Adding colnames at start of datum list**/
		//	SelectionInterpreter.lsSelect.add(0,addColNames);
		//	SelectionInterpreter.lsSelect.add(1,addTableNames);
			
			if(!isColNamesAdded){
				PreviousOutput.lsPrevious.add(0,addColNames);
				PreviousOutput.lsPrevious.add(1,addTableNames);
				isColNamesAdded = true;
			}
			
			/**Setting previous list**/
			//PreviousOutput.previous=selectNode;
			if(isOR){
				PreviousOutput.lsPrevious.addAll(SelectionInterpreter.lsSelect);
			}
			else{
				SelectionInterpreter.lsSelect.add(0,addColNames);
				SelectionInterpreter.lsSelect.add(1,addTableNames);
				PreviousOutput.lsPrevious=new ArrayList<Datum[]>(SelectionInterpreter.lsSelect);
			}
			SelectionInterpreter.lsSelect.clear();
			//System.out.println("Reading relation Previous..");
			//RelationReader.readRelation(PreviousOutput.lsPrevious);
			
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	
	}
}