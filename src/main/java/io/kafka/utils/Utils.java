package io.kafka.utils;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Properties;
import java.util.zip.CRC32;

/**
 * @author tf
 * @version 创建时间：2018年12月30日 下午10:12:35
 * @ClassName 工具类
 */
public class Utils {

	public static int getInt(Properties props, String name, int defaultValue) {
        return getIntInRange(props, name, defaultValue, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }
	
	public static int getIntInRange(Properties props, String name, int defaultValue, int min, int max) {
        int v = defaultValue;
        if (props.containsKey(name)) {
            v = Integer.valueOf(props.getProperty(name));
        }
        if (v >= min && v <= max) {
            return v;
        }
        throw new IllegalArgumentException(name + " has value " + v + " which is not in the range");
    }
	
	
	public static File getCanonicalFile(File f) {
        try {
            return f.getCanonicalFile();
        } catch (IOException e) {
            return f.getAbsoluteFile();
        }
    }
	
	public static String getString(Properties props, String name, String defaultValue) {
        return props.containsKey(name) ? props.getProperty(name) : defaultValue;
    }
	
	public static String getString(Properties props, String name) {
        if (props.containsKey(name)) {
            return props.getProperty(name);
        }
        throw new IllegalArgumentException("Missing required property '" + name + "'");
    }
	
	//********Channel************/
	@SuppressWarnings("resource")
	public static FileChannel openChannel(File file, boolean mutable) throws IOException {
        if (mutable) {
            return new RandomAccessFile(file, "rw").getChannel();
        }
        return new FileInputStream(file).getChannel();
    }

    /**
     * Get a property of type java.util.Properties or return the default if
     * no such property is defined
     * @param props properties
     * @param name the key
     * @param defaultProperties default property if empty
     * @return value from the property
     */
    public static Properties getProps(Properties props, String name, Properties defaultProperties) {
        final String propString = props.getProperty(name);
        if (propString == null) return defaultProperties;
        String[] propValues = propString.split(",");
        if (propValues.length < 1) {
            throw new IllegalArgumentException("Illegal format of specifying properties '" + propString + "'");
        }
        Properties properties = new Properties();
        for (int i = 0; i < propValues.length; i++) {
            String[] prop = propValues[i].split("=");
            if (prop.length != 2) throw new IllegalArgumentException("Illegal format of specifying properties '" + propValues[i] + "'");
            properties.put(prop[0], prop[1]);
        }
        return properties;
    }

	public static String fromBytes(byte[] b) {
        return fromBytes(b, "UTF-8");
    }

    public static String fromBytes(byte[] b, String encoding) {
        if (b == null) return null;
        try {
            return new String(b, encoding);
        } catch (UnsupportedEncodingException e) {
            return new String(b);
        }
    }


	public static byte[] getBytes(String s, String encoding) {
		if (s == null) return null;
        try {
            return s.getBytes(encoding);
        } catch (UnsupportedEncodingException e) {
            return s.getBytes();
        }
	}


	public static long crc32(byte[] bytes) {
		return crc32(bytes, 0, bytes.length);
	}
	
	/**
     * CRC的全称是循环冗余校验。
     * 文件校验
     * @param bytes  
     * @param offset 
     * @param size   
     * @return CRC32
     */
    public static long crc32(byte[] bytes, int offset, int size) {
        CRC32 crc = new CRC32();
        crc.update(bytes, offset, size);
        return crc.getValue();
    }


	public static void putUnsignedInt(ByteBuffer buffer, long value) {
		 buffer.putInt((int) (value & 0xffffffffL));
	}


	public static long getUnsignedInt(ByteBuffer buffer, int index) {
		return buffer.getInt(index) & 0xffffffffL;
	}


	public static String toString(ByteBuffer buffer, String encoding) {
		byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return fromBytes(bytes, encoding);
	}


	public static int read(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {
		int count = channel.read(buffer);
        if (count == -1) throw new EOFException("Received -1 when reading from channel, socket has likely been closed.");
        return count;
	}

	public static Thread newThread(String name, Runnable runnable, boolean daemon) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(daemon);
        return thread;
	}

	public static String readShortString(ByteBuffer buffer) {
		short size = buffer.getShort();
        if (size < 0) {
            return null;
        }
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return fromBytes(bytes);
	}

	public static int caculateShortString(String topic) {
		 return 2 + getBytes(topic).length;
	}
	
	public static byte[] getBytes(String s) {
        return getBytes(s, "UTF-8");
    }

	/**
	 * Len(topic) + topic
	 * @param buffer
	 * @param s
	 */
	public static void writeShortString(ByteBuffer buffer, String s) {
		if (s == null) {
            buffer.putShort((short) -1);
        } else if (s.length() > Short.MAX_VALUE) {
            throw new IllegalArgumentException("String max mum size Short.MAX_VALUE  " + Short.MAX_VALUE + ".");
        } else {
            byte[] data = getBytes(s); 
            buffer.putShort((short) data.length);
            buffer.put(data);
        }
	}

	@SuppressWarnings("unchecked")
    public static <E> E getObject(String className) {
        if (className == null) {
            return (E) null;
        }
        try {
            return (E) Class.forName(className).newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

	public static int getInt(Properties props, String name) {
		if (props.containsKey(name)) {
            return getInt(props, name, -1);
        }
        throw new IllegalArgumentException("Missing required property '" + name + "'");
	}

	public static boolean getBoolean(Properties props, String name, boolean defaultValue) {
		 if (!props.containsKey(name)) return defaultValue;
	        return "true".equalsIgnoreCase(props.getProperty(name));
	}
	

}
