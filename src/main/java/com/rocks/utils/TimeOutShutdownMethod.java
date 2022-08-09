package com.rocks.utils;

import org.apache.log4j.Logger;

import java.lang.reflect.Method;
import java.util.concurrent.*;

/**
 * 设置一个方法运行超时时间，超时结束该方法
 * 缺陷：方法参数不能是基础类型。shutdwon關閉不了死循環。
 * @author zhuang
 * @date 2022-01-07
 */
public class TimeOutShutdownMethod {
    private static final Logger logger = Logger.getLogger("TimeOutShutdownMethod.class");
    /** 超时时间 **/
    private static final int TIMEOUT = 3;
    /** 超时时间单位 **/
    private static final TimeUnit TIMEOUT_UNIT = TimeUnit.MINUTES;

    /**
     * 设置方法的超时时间
     * @param clazz 调用的方法所属的类
     * @param methodName 调用的方法
     * @param args 调用的方法参数
     * @return 调用方法的return值
     */
    public static Object setMethodShutdownTimeOut(Class clazz, String methodName, Object... args) {
        return setMethodTimeOut(clazz, methodName, TIMEOUT, TIMEOUT_UNIT, args);
    }

    /**
     * 设置方法的超时时间
     * @param clazz 调用的方法所属的类
     * @param methodName 调用的方法
     * @param timeout 超时时间
     * @param timeUnit 超时时间单位
     * @param args 调用的方法参数
     * @return 调用方法的return值
     */
    public static Object setMethodTimeOut(Class clazz, String methodName, int timeout, TimeUnit timeUnit, Object... args) {
        final ExecutorService exec = Executors.newFixedThreadPool(1);

        Callable call = () -> {
            Method method = clazz.getDeclaredMethod(methodName, toClass(args));
            method.setAccessible(true);
            Object invoke = method.invoke(clazz.newInstance(), args);
            return invoke;
        };

        Future<String> future = exec.submit(call);
        Object object = null;
        try {
            object = future.get(timeout, timeUnit);
            // System.out.println("方法执行完成");
        } catch (Exception e) {
            // 方法超时
            logger.debug("method exec timeout!!!", e);
        }
        exec.shutdownNow();

        return object;
    }

    private static Class<?>[] toClass(Object... args) {
        Class<?>[] parameterTypes = new Class<?>[args.length];
        for (int i=0; i < args.length; i++) {
            parameterTypes[i] =  args[i].getClass();
        }
        return parameterTypes;
    }

}

