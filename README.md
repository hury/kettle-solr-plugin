2017-11-17 17:16:56 @hury

kettle solr plugin
==================
项目概述：
基于kettle 4.1,solr 7.1 实现 将数据写入到solr core的功能；

项目部署：
1、使用maven build->package ,生成 solr_plugin-0.0.1-SNAPSHOT.jar

2、在kettle目录 plugins\steps 下，新建 solr_plugin 目录

3、将src/main/resources下的plugin.xml,solr.png,和必须的jar包一同拷贝到 solr_plugin 目录下：
E:\dev\data-integration_4.1.0\plugins\steps>tree /f
E:.
└─solr_plugin
        httpclient-4.5.3.jar
        httpcore-4.4.6.jar
        httpmime-4.5.3.jar
        noggit-0.8.jar
        plugin.xml
        solr-solrj-7.1.0.jar
        solr.png
        solr_plugin-0.0.1-SNAPSHOT.jar
        zookeeper-3.4.10.jar

4、启动kettle测试；

小结：
当前仅实现基于solr core的单实例模式，需要研究基于zookeeper模式的数据写入功能；

本项目在如下源码的基础上进行开发调试而成：
https://github.com/m-khl/kettle-solr-plugin.git