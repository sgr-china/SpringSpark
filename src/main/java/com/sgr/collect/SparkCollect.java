package com.sgr.collect;

import com.sgr.serviceScala.ScalaExportService;
import com.sgr.serviceScala.TestScalaService;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author sunguorui
 * @date 2021年11月21日 9:46 下午
 */
@RestController
public class SparkCollect {

    @Resource
    private TestScalaService testScalaService;

    @RequestMapping("/test")
    public void test(@RequestParam("sql") @NotBlank String sql) {
        testScalaService.getData(sql);

    }

    @Resource
    private ScalaExportService scalaExportService;
    @RequestMapping("/exportT")
    public void testSpark(@RequestParam("sql") @NotBlank String sql,
                     @RequestParam("code") @NotBlank String code,
                     @RequestParam("path") @NotBlank String path) {
       scalaExportService.exportData(sql, code, path, "1000");
    }

}
