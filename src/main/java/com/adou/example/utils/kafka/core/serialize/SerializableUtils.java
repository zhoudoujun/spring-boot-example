package com.adou.example.utils.kafka.core.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SerializableUtils {
	public static Object getObjectFromBytes(byte[] objBytes) throws IOException, ClassNotFoundException {
		if ((objBytes == null) || (objBytes.length == 0)) {
			return null;
		}
		ByteArrayInputStream bi = new ByteArrayInputStream(objBytes);
		ObjectInputStream oi = new ObjectInputStream(bi);
		bi.close();
		oi.close();
		return oi.readObject();
	}

	public static byte[] getBytesFromObject(Object obj) throws IOException {
		if (obj == null) {
			return null;
		}
		ByteArrayOutputStream bo = new ByteArrayOutputStream();
		ObjectOutputStream oo = new ObjectOutputStream(bo);
		oo.writeObject(obj);
		bo.close();
		oo.close();
		return bo.toByteArray();
	}
}
