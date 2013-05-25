package edu.buffalo.cse.sql.interpret;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Schema.Var;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ProjectionNode;

public class ProjectionInterpreter {

	public static List<Datum[]> lsProject = new ArrayList<Datum[]>();
	static int iCurrentOutputColIndex;
	static Stack<ExprTree> stack = new Stack<ExprTree>();
	static Schema.Type previousType;
	static Datum previousValue;

	public static void interpreteProjection(ProjectionNode projectNode) {

		/** for all const nodes **/
		if (projectNode.getChild().type == PlanNode.Type.NULLSOURCE) {
			
			System.out.println("in NULL SRC");
			iCurrentOutputColIndex = 0;
			Map<Var, ExprTree> mp = projectNode.getMapping();
			Iterator itr = mp.keySet().iterator();

			Datum[] rslt = new Datum[1];
			Datum[] colNames = null;
			
			if(UnionInterpreter.isUnion){
				rslt = new Datum[projectNode.getColumns().size()];
			}

			while (itr.hasNext()) {
				Var key = (Var) itr.next();
				ExprTree exprTree = mp.get(key);
				System.out.println("COLUMNNNN SIZEEEE= " + projectNode.getColumns().size());
				if (exprTree.op == ExprTree.OpCode.CONST && exprTree.size() == 0) {
					System.out.println("constant :: " + key);
					constant(exprTree, rslt,key,colNames);			
				} else if (exprTree.op == ExprTree.OpCode.VAR) {
				}
				else {
					pushOnStack(exprTree);
					System.out.println("inside else");
					Iterator itrr = exprTree.iterator();
					while (itrr.hasNext()) {
						ExprTree e = (ExprTree) itrr.next();
						System.out.println(e.op);
						if (e.op != ExprTree.OpCode.CONST)
							pushOnStack(e);
					}
				}
			}
			
			if(UnionInterpreter.isUnion ){
				/**adding colname **/				
				if(UnionInterpreter.lsUnionLeft.size() == 0){
					UnionInterpreter.isNullSourceLeft=true;
					UnionInterpreter.lsUnionLeft.add(rslt);
					System.out.println("Adding to left  = "+ rslt);
				}else{
					if(UnionInterpreter.lsUnionRight.size() != 0){
						UnionInterpreter.lsUnionRight.clear();
					}
					UnionInterpreter.isNullSourceRight=true;
					UnionInterpreter.lsUnionRight.add(rslt);
					System.out.println("Adding to right  = "+ rslt);

				}
			}

			if (stack != null) {
				while (!stack.isEmpty()) {
					popAndexecute(rslt);
				}
			}

			ProjectionInterpreter.lsProject.add(rslt);
			System.out.println(projectNode.getMapping());
		//	System.out.println("map size = " + projectNode.getMapping().size());
		}

		/** when its normal select query for columns **/
		else {	
			if(UnionInterpreter.isUnion){
			//	System.out.println("88888888888888888888888888888888:: "+projectNode.getColumns().size());
			//	System.out.println("88888888888888888888888888888888:: "+projectNode.getColumns().get(0).name);
				UnionInterpreter.iSetOutputSize=projectNode.getColumns().size();
				int iColNum=0;
				List lsColNames = new ArrayList();
				Iterator itr = projectNode.getColumns().iterator();
				while (itr.hasNext()) {
					System.out.println("FOR UNION");
					String strColName = itr.next().toString();
					String strTableName = null;
					/**table name is to determine exact match in case column names are same**/
					strTableName = "'"+strColName.substring(strColName.indexOf(":")+1,strColName.length()).trim()+"'";
					System.out.println("strColName = "+strColName);
					System.out.println("strTableName= " +strTableName);		
					String colName = strColName.substring(0,strColName.indexOf(":"));
					String colNameToBeCompared = "'" + colName + "'";

					System.out.println("COL NUM = "+iColNum);
					lsColNames.add(colNameToBeCompared);
				}

				UnionInterpreter.lsCol=lsColNames;
				UnionInterpreter.iSetOutputSize=lsColNames.size();
				return;
			}
			
			int iSize = projectNode.getColumns().size();
			int iColNum = 0;

			/** code to find matched columns and out into list of colnames **/
			List lsColNames = new ArrayList();
			Iterator itr = projectNode.getColumns().iterator();
			while (itr.hasNext()) {
				String strColName = itr.next().toString();
				String strTableName = null;
				/**table name is to determine exact match in case column names are same**/
				if(strColName.contains(".")){
					strTableName = "'"+strColName.substring(strColName.indexOf(":")+1,strColName.indexOf(".")).trim()+"'";
				}
				System.out.println("strColName = "+strColName);
				System.out.println("strTableName= " +strTableName);		
				String colName = strColName.substring(0,strColName.indexOf(":"));
				String colNameToBeCompared = "'" + colName + "'";
				System.out.println("colNameToBeCompared = "			+ colNameToBeCompared);
				iColNum = getColumnPosition(colNameToBeCompared,PreviousOutput.lsPrevious,strTableName);
				System.out.println("COL NUM = "+iColNum);
				lsColNames.add(iColNum);
			}

			Iterator itrr = PreviousOutput.lsPrevious.iterator();
			itrr.next();
			itrr.next();
			int iCounter = 0;
			while (itrr.hasNext()) {
				Datum[] temp = (Datum[]) itrr.next();
				// System.out.println(temp[0]);
				Datum[] rslt = new Datum[iSize];
				/** code to write multiple columns to output **/
				Iterator itrCol = lsColNames.iterator();
				while (itrCol.hasNext()) {
					int iColNumber = Integer.parseInt(itrCol.next().toString());
					rslt[iCounter] = temp[iColNumber];
					iCounter++;
				}
				iCounter=0;
				ProjectionInterpreter.lsProject.add(rslt);
			}
			iColNum++;
			
		}
	}

	static void pushOnStack(ExprTree exprTree) {
		System.out.println("pushing on stack :: " + exprTree.op);
		stack.push(exprTree);
	}

	static void popAndexecute(Datum[] rslt) {

		System.out.println("popping");
		ExprTree exprTree = stack.pop();
		System.out.println("pop ->" + exprTree.op);

		if (exprTree.op == ExprTree.OpCode.ADD) {
			if (exprTree.get(0).toString().contains(".")
					&& exprTree.get(1).toString().contains(".")) {
			} else {
				int a = Integer.parseInt(exprTree.get(0).toString());
				int b = Integer.parseInt(exprTree.get(1).toString());
				int c = a + b;
				rslt[iCurrentOutputColIndex] = new Datum.Int(c);
				previousType = Schema.Type.INT;
				previousValue = new Datum.Int(c);
			}
		}

		if (exprTree.op == ExprTree.OpCode.MULT) {
			System.out.println("sending to mult = " + exprTree);
			mult(exprTree, rslt);
		}

		if (exprTree.op == ExprTree.OpCode.NOT) {
			if (exprTree.get(0).toString().equalsIgnoreCase("true")) {
				rslt[iCurrentOutputColIndex] = new Datum.Bool(false);
			}
			if (exprTree.get(0).toString().equalsIgnoreCase("false")) {
				rslt[iCurrentOutputColIndex] = new Datum.Bool(true);
			}

		}
		if (exprTree.op == ExprTree.OpCode.AND) {

			Iterator itr = exprTree.iterator();
			boolean expr1 = Boolean.parseBoolean(itr.next().toString());
			System.out.println("expr1 " + expr1);
			boolean expr2 = Boolean.parseBoolean(itr.next().toString());
			System.out.println("expr2 " + expr2);
			boolean finalBool = expr1 && expr2;
			System.out.println("final bool = " + finalBool);
			rslt[iCurrentOutputColIndex] = new Datum.Bool(finalBool);

		}
		if (exprTree.op == ExprTree.OpCode.OR) {

			Iterator itr = exprTree.iterator();
			boolean expr1 = Boolean.parseBoolean(itr.next().toString());
			boolean expr2 = Boolean.parseBoolean(itr.next().toString());
			boolean finalBool = expr1 || expr2;
			rslt[iCurrentOutputColIndex] = new Datum.Bool(finalBool);

		}

		if (exprTree.op == ExprTree.OpCode.VAR) {
			Iterator itr = exprTree.allVars().iterator();
			while (itr.hasNext()) {
				Var var = (Var) itr.next();
				System.out.println("name=" + var.name);
				System.out.println("range=" + var.rangeVariable);

			}

		}
	}

	static void mult(ExprTree exprTree, Datum[] rslt) {

		boolean isFloat = false;
		int inum1 = 0, inum2 = 0;
		float fnum1 = 0, fnum2 = 0;
		if (exprTree.get(0).toString().contains(".")
				|| exprTree.get(1).toString().contains(".")) {
			isFloat = true;
		}
		try {
			if (isFloat)
				fnum1 = Float.parseFloat(exprTree.get(0).toString());
			else
				inum1 = Integer.parseInt(exprTree.get(0).toString());
		} catch (NumberFormatException ex) {
			try {
				if (previousType == Schema.Type.FLOAT)
					fnum1 = previousValue.toFloat();
				else
					inum1 = previousValue.toInt();
			} catch (CastError e) {
				e.printStackTrace();
			}
		}
		try {
			if (isFloat)
				fnum2 = Float.parseFloat(exprTree.get(1).toString());
			else
				inum2 = Integer.parseInt(exprTree.get(1).toString());
		} catch (NumberFormatException ex) {
			try {
				if (previousType == Schema.Type.FLOAT)
					fnum2 = previousValue.toFloat();
				else
					inum2 = previousValue.toInt();
			} catch (CastError e) {
				e.printStackTrace();
			}
		}

		if (isFloat || previousType == Schema.Type.FLOAT) {
			float fsrlt = fnum1 * fnum2;
			rslt[iCurrentOutputColIndex] = new Datum.Flt(fsrlt);
			previousType = Schema.Type.FLOAT;
			previousValue = new Datum.Flt(fsrlt);

		} else {
			int irslt = inum1 * inum2;
			rslt[iCurrentOutputColIndex] = new Datum.Int(irslt);
			previousType = Schema.Type.INT;
			previousValue = new Datum.Int(irslt);

		}

	}

	static void constant(ExprTree exprTree, Datum[] rslt,Var key,Datum[] colNames) {

		String strConst = exprTree.toString();
		if (strConst.startsWith("'") && strConst.endsWith("'")) {
			String strOut = strConst.substring(1, strConst.length() - 1);
			rslt[iCurrentOutputColIndex] = new Datum.Str(strOut);
		} else if (strConst.equalsIgnoreCase("true")) {
			rslt[iCurrentOutputColIndex] = new Datum.Bool(true);
		} else if (strConst.equalsIgnoreCase("false")) {
			rslt[iCurrentOutputColIndex] = new Datum.Bool(false);
		} else if (strConst.contains(".")) {
			rslt[iCurrentOutputColIndex] = new Datum.Flt(
					Float.parseFloat(exprTree.toString()));
		} else {
			rslt[iCurrentOutputColIndex] = new Datum.Int(
					Integer.parseInt(exprTree.toString()));
		}
		iCurrentOutputColIndex++;		
	}

	static int getColumnPosition(String strCol, List<Datum[]> ls,String strTableName) {
		
		Datum[] colNames=null;
		Datum[] tableNames = null;
		int i = 0;
		
		if(ls!=null){
		 colNames = ls.get(0);
		 tableNames = ls.get(1);
		
		i = 0;
		for (; i < colNames.length; i++) {
			//System.out.println("colNames[i].toString()="+ colNames[i].toString());
			if (strTableName==null) {
				if (strCol.equalsIgnoreCase(colNames[i].toString().trim())) {
					System.out.println("Matched Column");
					break;
				
				} 
			}else {
					if (strCol.equalsIgnoreCase(colNames[i].toString().trim()) && strTableName.equalsIgnoreCase(tableNames[i].toString().trim())) {
							System.out.println("Matched Column");
							break;
					}
				}
			}
		}
		return i;
	}
}