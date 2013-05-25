package edu.buffalo.cse.sql.interpret;

import java.util.Iterator;

import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.Schema.Table;
import edu.buffalo.cse.sql.Schema.Var;
import edu.buffalo.cse.sql.plan.AggregateNode;
import edu.buffalo.cse.sql.plan.IndexScanNode;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.PlanNode.Type;
import edu.buffalo.cse.sql.plan.ProjectionNode;
import edu.buffalo.cse.sql.plan.ScanNode;
import edu.buffalo.cse.sql.plan.SelectionNode;
import edu.buffalo.cse.sql.plan.UnionNode;
import edu.buffalo.cse.sql.interpret.*;

public class RAInterpreterStack {
	public static PlanNode popStack() {
		
		//System.out.println("***************************POP***************************");
		PlanNode planNode;

		try{
		while (!RAInterpreter.RAStack.isEmpty()) {

			planNode = (PlanNode) RAInterpreter.RAStack.pop();

			if(planNode.type == Type.INDEXSCAN){
				//System.out.println("here index scannning");
				IndexScanNode isn = (IndexScanNode)planNode;
				
				
				return null;
			}
			if (planNode.type == Type.AGGREGATE) {				
				AggregateNode aggrNode = (AggregateNode)planNode;
				
				if(aggrNode.getChild().type==Type.INDEXSCAN){
					IndexScanNode isn = (IndexScanNode)aggrNode.getChild();
					IndexScanInterpreter.interpretIndexScan(isn);
					//System.out.println("here i am");
					return null;
				}
				
				//System.out.println("pop ->" + planNode.type);
				//System.out.println("  ->"+aggrNode.getAggregates().size());
				
				/**change temp**/
				AggregateInterpreter.interpreteAggregates(aggrNode);
				if(RAInterpreter.RAStack.isEmpty()){
					Sql.outputList=AggregateInterpreter.lsAgg;
				//	return;
				}
			}
			if (planNode.type == Type.JOIN) {
				//System.out.println("pop ->" + planNode.type);
				JoinNode joinNode= (JoinNode)planNode;
				JoinInterpreter.interpretejoin(joinNode);
			}
			if (planNode.type == Type.PROJECT) {
				//System.out.println("pop ->" + planNode.type);
				ProjectionNode projectNode= (ProjectionNode)planNode;
				ProjectionInterpreter.interpreteProjection(projectNode);
				if(RAInterpreter.RAStack.isEmpty()){
					Sql.outputList=ProjectionInterpreter.lsProject;
					//return;
				}
			}
			if (planNode.type == Type.SCAN) {
				ScanNode scanNode = (ScanNode)planNode;
			}
			
			if (planNode.type == Type.SELECT) {
				SelectionNode selectNode=(SelectionNode)planNode;
				SelectionInterpreter.interpreteSelectionMultiple(selectNode);
				//System.out.println("pop ->" + planNode.type);
			}
			if (planNode.type == Type.UNION) {

				//System.out.println("pop ->" + planNode.type);
				UnionNode unionNode=(UnionNode) planNode;
				UnionInterpreter.interpreteUnion();
				if(RAInterpreter.RAStack.isEmpty()){
					Sql.outputList=UnionInterpreter.lsUnion;
					//return;
				}
			}
		}
		return null;
		}catch(Exception ex){
			
			ex.printStackTrace();
		}
		return null;
	}

}