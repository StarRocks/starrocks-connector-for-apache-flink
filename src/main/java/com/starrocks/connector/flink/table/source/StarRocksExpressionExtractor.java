/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.apache.flink.table.types.logical.LogicalTypeRoot;


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
            // FUNC_TO_STR.put(BuiltInFunctionDefinitions.IS_NULL, "is");
            // FUNC_TO_STR.put(BuiltInFunctionDefinitions.IS_NOT_NULL, "is not");
            // FUNC_TO_STR.put(BuiltInFunctionDefinitions.NOT, "");
        }

    @Override
    public String visit(CallExpression call) {
        FunctionDefinition funcDef = call.getFunctionDefinition();
        if (funcDef.equals(BuiltInFunctionDefinitions.LIKE)) {
            throw new RuntimeException("Not support filter -> [like]");
        }
        if (funcDef.equals(BuiltInFunctionDefinitions.IN)) {
            throw new RuntimeException("Not support filter -> [in]");
        }
        if (funcDef.equals(BuiltInFunctionDefinitions.BETWEEN)) {
            throw new RuntimeException("Not support filter -> [between]");
        }
        if (funcDef.equals(BuiltInFunctionDefinitions.CAST)) {
            return call.getChildren().get(0).accept(this);
        }
        if (funcDef.equals(BuiltInFunctionDefinitions.NOT) && call.getOutputDataType().equals(DataTypes.BOOLEAN())) {
            return call.getChildren().get(0).toString() + " = false";
        }
        if (funcDef.equals(BuiltInFunctionDefinitions.IS_NULL)) {
            return call.getChildren().get(0).toString() + " is null";
        }
        if (funcDef.equals(BuiltInFunctionDefinitions.IS_NOT_NULL)) {
            return call.getChildren().get(0).toString() + " is not null";
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
        LogicalTypeRoot typeRoot = valueLiteral.getOutputDataType().getLogicalType().getTypeRoot();
        if (typeRoot.equals(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) || 
            typeRoot.equals(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) || 
            typeRoot.equals(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE) ||
            typeRoot.equals(LogicalTypeRoot.DATE)) {
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