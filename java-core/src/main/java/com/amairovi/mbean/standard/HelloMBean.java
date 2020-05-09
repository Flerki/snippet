package com.amairovi.mbean.standard;

public interface HelloMBean {
    void sayHello();
    int add(int x, int y);

    String getName();

    int getCacheSize();
    void setCacheSize(int size);

}
