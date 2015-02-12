package com.dataiku.hive.udf.maps;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaIntObjectInspector;

import java.util.*;

public class UDFCountToMap extends GenericUDF {
    ListObjectInspector loi;
    JavaStringObjectInspector elementOI;

    public String getDisplayString(String[] args){
	return "UDFCountToMap()";
    }

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException{
	if (arguments.length != 1){
	    throw new UDFArgumentException("UDFCountToMap takes exactly 1 argument");
	}

	ObjectInspector a = arguments[0];
	this.loi = (ListObjectInspector) a;

	JavaStringObjectInspector soi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	JavaIntObjectInspector ioi = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
	return ObjectInspectorFactory.getStandardMapObjectInspector(soi, ioi);
    }

    public Map<String, Integer> evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException{
	List<String> a = (List<String>) this.loi.getList(arguments[0].get());
	
        HashMap<String, Integer> map= new HashMap<String, Integer>();
        if (a == null) {
            return null;
        }
        for(String s : a) {
            if (s == null) {
                continue;
            }
            if (map.containsKey(s)) {
                map.put(s, map.get(s) + 1);
            } else {
                map.put(s, 1);
            }
        }
        return map;
    }

}
