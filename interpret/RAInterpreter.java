package edu.buffalo.cse.sql.interpret;

import java.util.Stack;

import edu.buffalo.cse.sql.plan.AggregateNode;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.NullSourceNode;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ProjectionNode;
import edu.buffalo.cse.sql.plan.ScanNode;
import edu.buffalo.cse.sql.plan.SelectionNode;
import edu.buffalo.cse.sql.plan.UnionNode;
import edu.buffalo.cse.sql.plan.PlanNode.Type;

public class RAInterpreter {

	static int iCount;
	public static Stack RAStack = new Stack();
	
	public static Object interpreteRA(PlanNode planNode) {

		if (planNode.type == Type.AGGREGATE) {
			//System.out.println("Type  = " + planNode.type);
			AggregateNode aggregate = (AggregateNode) planNode;
			RAInterpreter.RAStack.push(aggregate);		
			//System.out.println("  -> " + aggregate.getAggregates());
			PlanNode aggrChild = aggregate.getChild();
			RAInterpreter.interpreteRA(aggrChild);

		}
		if (planNode.type == Type.SELECT) {
			//System.out.println("Type =  " + planNode.type);
			SelectionNode selectChild = (SelectionNode) planNode;
			RAInterpreter.RAStack.push(selectChild);
			//System.out.println("  -> " + selectChild.getCondition());
			PlanNode sltChild = selectChild.getChild();
			RAInterpreter.interpreteRA(sltChild);
		}
		if (planNode.type == Type.JOIN) {
			//System.out.println("Type =  " + planNode.type);
			JoinNode joinNode = (JoinNode) planNode;
			RAInterpreter.RAStack.push(joinNode);		
			//System.out.println("join type = " + joinNode.getJoinType());
			//System.out.println("   ->" + joinNode.getLHS());
			//System.out.println("   ->" + joinNode.getRHS());
			PlanNode rightChild = joinNode.getRHS();
			PlanNode leftChild = joinNode.getLHS();
			RAInterpreter.interpreteRA(leftChild);
			RAInterpreter.interpreteRA(rightChild);
		}
		if (planNode.type == Type.NULLSOURCE) {
			//System.out.println("Type =  " + planNode.type);
			NullSourceNode nullSrcNode = (NullSourceNode) planNode;
			RAInterpreter.RAStack.push(nullSrcNode);
		}
		if (planNode.type == Type.PROJECT) {
			//System.out.println("Type =  " + planNode.type);
			ProjectionNode projectionNode = (ProjectionNode) planNode;
			RAInterpreter.RAStack.push(projectionNode);
			PlanNode node = projectionNode.getChild();
			//System.out.println("Type =  " + node.type);
			RAInterpreter.interpreteRA(node);
		}
		if (planNode.type == Type.SCAN) {
			//System.out.println("Type =  " + planNode.type);
			ScanNode scanNode = (ScanNode) planNode;
			RAInterpreter.RAStack.push(scanNode);
			//System.out.println("   ->" + scanNode.table);
		}
		if(planNode.type == Type.INDEXSCAN){
			//System.out.println("Type =  " + planNode.type);
			
			
		}
		if (planNode.type == Type.UNION) {
			//System.out.println("child =   " + planNode.type);
			UnionNode unionNode = (UnionNode) planNode;
			RAInterpreter.RAStack.push(unionNode);
			UnionInterpreter.isUnion=true;
			PlanNode leftChild = unionNode.getLHS();
			PlanNode rightChild = unionNode.getRHS();
			RAInterpreter.interpreteRA(leftChild);
			RAInterpreter.interpreteRA(rightChild);
		}
		return null;
	}
}
