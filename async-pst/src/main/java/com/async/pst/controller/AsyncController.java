package com.async.pst.controller;

import com.async.pst.service.AsyncService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/async")
public class AsyncController {


    @Autowired
    AsyncService asyncService;


    @RequestMapping("/test")
    public String test() throws ExecutionException, InterruptedException {
        return asyncService.result();
    }


}
