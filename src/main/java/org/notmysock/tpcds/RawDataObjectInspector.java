package org.notmysock.tpcds;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public final class RawDataObjectInspector extends SettableStructObjectInspector {
	
	public static class RawColumn implements StructField {
		private final int col;
		private final String type;
		private final ObjectInspector fieldObjectInspector;

		public RawColumn(int column, String type,  ObjectInspector fieldObjectInspector) {
			this.fieldObjectInspector = fieldObjectInspector;
			this.col = column;
			this.type = type;
		}

		public String getFieldName() {
			return "col"+this.col;
		}

		public ObjectInspector getFieldObjectInspector() {
			return fieldObjectInspector;
		}

		public String getFieldComment() {
			return null;
		}

		@Override
		public String toString() {
			return getFieldName()+":"+type;
		}
	}

	List<RawColumn> fields = new ArrayList<RawColumn>();

	public RawDataObjectInspector(String[] types) {
		for(int i = 0; i < types.length; i++) {
			Type t = Integer.class;
			if("int".equals(types[i])) {
				t = Integer.class;
			} else if("float".equals(types[i])) {
				t = Double.class;
			} else if("string".equals(types[i])) {
				t = String.class;
			} else {
				throw new NotImplementedException();
			}
			fields.add(new RawColumn(i, types[i], ObjectInspectorFactory
					.getReflectionObjectInspector(t,
							ObjectInspectorOptions.JAVA)));
		}
	}

	@Override
	public Object create() {
		throw new NotImplementedException();
	}

	@Override
	public Object setStructFieldData(Object arg0, StructField arg1, Object arg2) {
		throw new NotImplementedException();
	}

	@Override
	public List<? extends StructField> getAllStructFieldRefs() {
		return fields;
	}

	@Override
	public Object getStructFieldData(Object data, StructField field) {
		if (data == null) {
			return null;
		}
		Object[] columns = (Object[]) data;
		for (int i = 0; i < fields.size(); i++) {
			if(fields.get(i) == field) {
				return columns[i];
			}
		}
		
		throw new NotImplementedException();
		
	}

	@Override
	public StructField getStructFieldRef(String name) {
		// TODO Auto-generated method stub
		throw new NotImplementedException();
	}

	@Override
	public List<Object> getStructFieldsDataAsList(Object data) {
		if (data == null) {
			return null;
		}
		Object[] columns = (Object[]) data;
		try {
			ArrayList<Object> result = new ArrayList<Object>(fields.size());			
			for (int i = 0; i < fields.size(); i++) {
				if(i < columns.length) {
					result.add(columns[i]);
				} else {
					result.add(null);
				}
			}
			return result;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Category getCategory() {
		return Category.STRUCT;
	}

	@Override
	public String getTypeName() {
		return "RawData<>";
	}
}
