package com.atguigu.gmall1018.dw.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall1018.dw.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


import java.beans.SimpleBeanInfo;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublistherController {


    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date){
        List totalList = new ArrayList();
        int dauTotal = publisherService.getDauTotal(date);
        Map dauMap = new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);

        totalList.add(dauMap);

        Map newMidMap = new HashMap();
        newMidMap.put("id","mew_mid");
        newMidMap.put("name","新增用户");
        newMidMap.put("value",1000);

        totalList.add(newMidMap);

        return JSON.toJSONString(totalList);

    }

    private String getYesterday(String date){
        Date today = null;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            today =simpleDateFormat.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Date yesterday = DateUtils.addDays(today, -1);
        String yesterdayString = simpleDateFormat.format(yesterday);

        return yesterdayString;

    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id,@RequestParam("date") String date) {
        if ("dau".equals(id)) {
            Map dauHourCountTDMap = publisherService.getDauHourCount(date);

            String yesterday = getYesterday(date);

            Map dauHourCountYDMap = publisherService.getDauHourCount(yesterday);
            Map dauMap = new HashMap<>();
            dauMap.put("today",dauHourCountTDMap);
            dauMap.put("yesterday",dauHourCountYDMap);
            return JSON.toJSONString(dauMap);
        }
        return null;

    }


}
