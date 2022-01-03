package com.starrocks.connector.flink.table.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;


public class StarRocksExpressionExtractor implements ExpressionVisitor<String> {

    private static final Map<FunctionDefinition, String> FUNC_TO_STR = new HashMap<>();

        static {
            FUNC_TO_STR.put(BuiltInFunctionDefinitions.EQUALS, "=");
            FUNC_TO_STR.put(BuiltInFunctionDefinitions.NOT_EQUALS, "<>");
            FUNC_TO_STR.put(BuiltInFunctionDefinitions.GREATER_THAN, ">");
            FUNC_TO_STR.put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, ">=");
            FUNC_TO_STR.put(BuiltInFunctionDefinitions.LESS_THAN, "<");
            FUNC_TO_STR.put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, "<=");
            FUNC_TO_STR.put(BuiltInFunctionDefinitions.AND, "and");
            FUNC_TO_STR.put(BuiltInFunctionDefinitions.OR, "or");
            FUNC_TO_STR.put(BuiltInFunctionDefinitions.IS_NULL, "is");
            FUNC_TO_STR.put(BuiltInFunctionDefinitions.NOT, "");
        }

    @Override
    public String visit(CallExpression call) {

        FunctionDefinition funcDef = call.getFunctionDefinition();
        if (funcDef.equals(BuiltInFunctionDefinitions.CAST)) {
            return call.getChildren().get(0).accept(this);
        }
    
        if (funcDef.equals(BuiltInFunctionDefinitions.NOT) && call.getOutputDataType().equals(DataTypes.BOOLEAN())) {
            return call.getChildren().get(0).toString() + " = false";
        }
        if (funcDef.equals(BuiltInFunctionDefinitions.IS_NULL)) {
            return call.getChildren().get(0).toString() + " is null";
        }
        if (FUNC_TO_STR.containsKey(funcDef)) {
            
            List<String> operands = new ArrayList<>();
            for (Expression child : call.getChildren()) {
                String operand = child.accept(this);
                if (operand == null) {
                    continue;
                }
                operands.add(operand);
            }
            return "(" + String.join(" " + FUNC_TO_STR.get(funcDef) + " ", operands) + ")";
        }
        return null;
    }

    @Override
    public String visit(ValueLiteralExpression valueLiteral) {
        if (valueLiteral.getOutputDataType() == DataTypes.DATE() || valueLiteral.getOutputDataType().toString().equals("DATE NOT NULL")) {
            return "'" + valueLiteral.toString() + "'";
        }
        return valueLiteral.toString();
    }

    @Override
    public String visit(FieldReferenceExpression fieldReference) {
        return fieldReference.getName();
    }

    @Override
    public String visit(TypeLiteralExpression typeLiteral) {
        return typeLiteral.getOutputDataType().toString();
    }

    @Override
    public String visit(Expression other) {
        return null;
    }

}