package edu.buffalo.cse.sql.interpret;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Schema.Var;
import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.PlanNode.Structure;

public class JoinInterpreter {

	static List<Datum[]> lsJoinedDatum = new ArrayList<Datum[]>();
	public static List colSequence;

	public static void interpretejoin(JoinNode joinNode) {
		System.out.println("PATH :: interpretejoin(JoinNode joinNode)");
		if (joinNode.getJoinType() == JoinNode.JType.NLJ) {
			System.out.println("PATH :: NLJ");
			if (joinNode.struct == Structure.BINARY) {

				String strLHSTable = joinNode.getLHS().getSchemaVars().get(0).rangeVariable.trim();
				String strRHSTable = joinNode.getRHS().getSchemaVars().get(0).rangeVariable.trim();
				
				System.out.println("LHS Table = " + strLHSTable);
				System.out.println("RHS Table = " + strRHSTable);

				if (colSequence == null) {
					colSequence = new ArrayList();
				}

				boolean isLeftPresent = false;
				boolean isRightPresent = false;
				Iterator itrColSeq = colSequence.iterator();
				while (itrColSeq.hasNext()) {
					String strName = itrColSeq.next().toString();
					if (strLHSTable.equalsIgnoreCase(strName)) {
						isLeftPresent = true;
					}
					if (strRHSTable.equalsIgnoreCase(strName)) {
						isRightPresent = true;
					}
				}
				if(!isLeftPresent)	colSequence.add(strLHSTable);
				if(!isRightPresent) colSequence.add(strRHSTable);
				
				Iterator itr = ScanInterpreter.mpScanned.keySet().iterator();
				List<Datum[]> datumLeft = new ArrayList<Datum[]>();
				List<Datum[]> datumRight = new ArrayList<Datum[]>();
				

				while (itr.hasNext()) {
					
					String strTableName = itr.next().toString().trim();
					String strOrigTableName=strTableName;
					System.out.println("strTableName = "+strTableName);				
					if(Sql.mpTableAlias.size()>0){
						String strTableNameForMap="'"+strTableName.trim()+"'";
						String strUnAliasedTableName=Sql.mpTableAlias.get(strTableNameForMap).toString();
						System.out.println("PATH :: Got unaliased table name : "+strTableName+ " :: "+strUnAliasedTableName);
						strTableName = strUnAliasedTableName;
						if(!(strLHSTable.contains("'"))){
						strLHSTable="'"+strLHSTable+"'";
						strRHSTable="'"+strRHSTable+"'";
						}
					
					}
					System.out.println("strTableName = "+strTableName);
					
					if (strTableName.equalsIgnoreCase(strLHSTable)) {
						if (PreviousOutput.previous != null
								&& PreviousOutput.previous.type == PlanNode.Type.JOIN) {
							datumLeft = PreviousOutput.lsPrevious;
						} else {
							System.out.println("PATH :: Getting datum left from mpScanned");
							datumLeft = ScanInterpreter.mpScanned
									.get(strOrigTableName);
						}
						continue;
					}
					if (strTableName.equalsIgnoreCase(strRHSTable)) {
						datumRight = ScanInterpreter.mpScanned
								.get(strOrigTableName);
						continue;
					}
				}

				System.out.println("MAP SIZE = "+ScanInterpreter.mpScanned.size());
			//	System.out.println(datumLeft);
			//	System.out.println(datumRight);
				
				int iNumLHSColumn = datumLeft.get(0).length;
				int iNumRHSColumn = datumRight.get(0).length;
				System.out.println("iNumLHSColumn" + iNumLHSColumn);
				System.out.println("iNumRHSColumn" + iNumRHSColumn);

				Iterator itrLeft = datumLeft.iterator();
				// skip iteration one to remove 1st record which is column names
				itrLeft.next();
				itrLeft.next();
				while (itrLeft.hasNext()) {

					Datum[] joinDatum = new Datum[iNumLHSColumn + iNumRHSColumn];
					Datum[] leftTemp = (Datum[]) itrLeft.next();
					int i = 0;
					// put table 1 data
					for (; i < leftTemp.length; i++) {
						joinDatum[i] = leftTemp[i];
					}

					Iterator itrRight = datumRight.iterator();
					// skip iteration one to remove 1st record which is column
					// names
					itrRight.next();
					// joine with table 2 data
					itrRight.next();
					while (itrRight.hasNext()) {
						Datum[] rightTemp = (Datum[]) itrRight.next();
						for (int j = 0; j < rightTemp.length; j++) {
							joinDatum[i + j] = rightTemp[j];
						}
						// put all data once in one datum array and add it to
						// list
						Datum[] joinedRecord = new Datum[iNumLHSColumn
								+ iNumRHSColumn];
						for (int k = 0; k < joinDatum.length; k++) {
							joinedRecord[k] = joinDatum[k];
						}
						JoinInterpreter.lsJoinedDatum.add(joinedRecord);
					}
				}

				Datum[] joinedRecordColumnNames = new Datum[iNumLHSColumn+ iNumRHSColumn];
				Datum[] joinedRecordTableNames = new Datum[iNumLHSColumn+ iNumRHSColumn];

				/** Adding column names to final list **/
				Iterator itrL = datumLeft.iterator();
				Datum[] tempCol = (Datum[]) itrL.next();
				Datum[] tempTable = (Datum[]) itrL.next();

				int k = 0;
				for (k = 0; k < iNumLHSColumn; k++) {
					joinedRecordColumnNames[k] = tempCol[k];
					joinedRecordTableNames[k]= tempTable[k];
				}
				Iterator itrR = datumRight.iterator();
				Datum[] tempCol1 = (Datum[]) itrR.next();
				Datum[] tempTable1 = (Datum[]) itrR.next();
				for (int l = 0; l < iNumRHSColumn; l++) {
					joinedRecordColumnNames[k + l] = tempCol1[l];
					joinedRecordTableNames[k + l] = tempTable1[l];
				}

				/** Adding colnames at start of datum list **/
				JoinInterpreter.lsJoinedDatum.add(0, joinedRecordColumnNames);
				/**Adding table names**/
				JoinInterpreter.lsJoinedDatum.add(1, joinedRecordTableNames);

				/** Setting previous list **/
				PreviousOutput.previous = joinNode;
				PreviousOutput.lsPrevious = new ArrayList<Datum[]>(
						JoinInterpreter.lsJoinedDatum);
				JoinInterpreter.lsJoinedDatum.clear();

			}

		}
		
		if(joinNode.getJoinType()==JoinNode.JType.MERGE){
			
			String strLHSTable = joinNode.getLHS().getSchemaVars().get(0).rangeVariable.trim();
			String strRHSTable = joinNode.getRHS().getSchemaVars().get(0).rangeVariable.trim();
			
		}
	}

}