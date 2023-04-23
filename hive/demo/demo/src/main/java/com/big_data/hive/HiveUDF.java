package com.big_data.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


public class HiveUDF extends GenericUDF {

    // 用于处理具体的逻辑
    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        Object o = arg0[0].get();

        if (o == null){
            return 0;
        }
        return o.toString().length();
        
    }

    @Override
    public String getDisplayString(String[] arg0) {
        // TODO Auto-generated method stub
        return "";
    }

    // 判断传参的类型 和长度
    // 约定返回的数据类型
    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
        // TODO Auto-generated method stub
        if (arg0.length != 1){
            throw new UDFArgumentLengthException("please give me only one arg");
        }

        if(!arg0[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentTypeException(1, "i need primitive type  args");
        }
        
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }
    
}
