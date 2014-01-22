package org.notmysock.tpcds;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.net.*;
import java.math.*;
import java.security.*;
import java.util.concurrent.*;

public abstract class Utilities {
	public static File findContainingJar(Class my_class) {
		ClassLoader loader = my_class.getClassLoader();
		String class_file = my_class.getName().replaceAll("\\.", "/")
				+ ".class";
		try {
			for (Enumeration itr = loader.getResources(class_file); itr
					.hasMoreElements();) {
				URL url = (URL) itr.nextElement();
				if ("jar".equals(url.getProtocol())) {
					String toReturn = url.getPath();
					if (toReturn.startsWith("file:")) {
						toReturn = toReturn.substring("file:".length());
					}
					// URLDecoder is a misnamed class, since it actually decodes
					// x-www-form-urlencoded MIME type rather than actual
					// URL encoding (which the file path has). Therefore it
					// would
					// decode +s to ' 's which is incorrect (spaces are actually
					// either unencoded or encoded as "%20"). Replace +s first,
					// so
					// that they are kept sacred during the decoding process.
					toReturn = toReturn.replaceAll("\\+", "%2B");
					toReturn = URLDecoder.decode(toReturn, "UTF-8");
					return new File(toReturn.replaceAll("!.*$", ""));
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return null;
	}

}
