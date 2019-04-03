package com.atguigu.gmall1018.dw.publisher.service;

import java.util.Map;

public interface PublisherService {

    //查询总数
    public int getDauTotal(String date);

    //查询分时
    public Map getDauHourCount(String date);

}
