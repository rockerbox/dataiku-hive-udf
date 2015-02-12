package com.dataiku.hive.udf.maps;

import org.apache.hadoop.hive.ql.exec.UDF;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;

import java.util.*;

/**
 * Filter topN Elements from a map
 */
public class UDFMapValueFilterTopN extends GenericUDF {
    MapObjectInspector moi;
    JavaStringObjectInspector soi;
    WritableConstantIntObjectInspector noi;
    
    public String getDisplayString(String[] args){
        return "UDFMapFilterTopN()";
    }

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException{
        if (arguments.length != 2){
            throw new UDFArgumentException("UDFMapFilterTopN takes exactly 2 arguments");
        }

        ObjectInspector a = arguments[0];
        this.moi = (MapObjectInspector) a;

	ObjectInspector b = arguments[1];
	this.noi = (WritableConstantIntObjectInspector) b;

        JavaStringObjectInspector soi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        JavaIntObjectInspector ioi = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        return ObjectInspectorFactory.getStandardMapObjectInspector(soi, ioi);
    }

    public Map<String, Integer> evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
	Map<String, Integer> map = (Map<String, Integer>) this.moi.getMap(arguments[0].get());
	Integer n = ((IntWritable)arguments[1].get()).get();

        if (map.size() < n) {
            return map;
        }
        List<Integer> list = new ArrayList(map.values());
        Collections.sort(list);
        int limit = list.get(list.size() - n);
        int count = 0;
        HashMap<String, Integer> nm = new HashMap<String, Integer>();

        for(Map.Entry<String, Integer> entry : map.entrySet()) {
            if (entry.getValue() > limit) {
                nm.put(entry.getKey(), entry.getValue());
            }
        }
        for(Map.Entry<String, Integer> entry : map.entrySet()) {
             if (nm.size() == n) {
                break;
             }
            if (entry.getValue() == limit) {
                nm.put(entry.getKey(), entry.getValue());
            }
        }
        return nm;
    }
}

