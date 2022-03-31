#!/usr/bin/env python3
#-*- coding:utf-8 _*-  
""" 
@author: Dennis zhang
@site:  1、先通过配置文件读取账号信息
        2、通过账号信息获取下属所有的实例信息
        3、通过实例信息挨个获取监控信息
        4、整合监控信息，生成metrics

        todo_list：未监控 metrics：MongoDB_Opcounters、
                                   MongoDB_Cursors、
                                   MongoDB_Network、
                                   MongoDB_Global_Lock_Current_Queue、
                                   MongoDB_Wt_Cache
@software: PyCharm 
"""
import requests
import logging
import yaml
import json
import time

from flask import Flask, Response
from flask_cache import Cache

from enum import Enum, unique
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.acs_exception.exceptions import ClientException
from aliyunsdkcore.acs_exception.exceptions import ServerException
from aliyunsdkdds.request.v20151201.DescribeDBInstancePerformanceRequest import DescribeDBInstancePerformanceRequest
from aliyunsdkdds.request.v20151201.DescribeDBInstancesRequest import DescribeDBInstancesRequest
from prometheus_client import Gauge, generate_latest
from prometheus_client.core import CollectorRegistry

@unique
class account_uid(Enum):
    LTAI4F**= "******"
  
class aliyun_mongodb:

    def __init__(self):

        # 日志设置段
        file_name = "../log/aliyun_mongodb_api.log"
        fh = logging.FileHandler(filename=file_name, encoding="utf-8")
        logging.basicConfig(handlers=[fh], level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

        # 全局定义时间
        base_time = int(time.time()) - 60 * 5
        self.start_time = time.strftime("%Y-%m-%dT%H:%MZ", time.gmtime(base_time - 300))
        self.end_time = time.strftime("%Y-%m-%dT%H:%MZ", time.gmtime(base_time))
        print(self.start_time)

        # 配置文件设置
        self.config_file = "./aliyun_mongodb_config.yaml"

        # 初始化监控项metrics
        self.regensiter = CollectorRegistry(auto_describe=False)
        self.mongodb_metrics_up = Gauge(
            "aliyun_mongodb_metrics_up", "aliyun mongodb  metrics info", (), registry=self.regensiter
        )
        self.mongodb_metrics_cpu_usage = Gauge(
            "aliyun_mongodb_metrics_cpu_usage", "aliyun mongodb metrics info",
            ("account", "db_instance_id", "db_instance_desc", "metrics_name"),
            registry=self.regensiter
        )
        self.mongodb_metrics_memory_usage = Gauge(
            "aliyun_mongodb_metrics_memory_usage", "aliyun mongodb metrics info",
            ("account", "db_instance_id", "db_instance_desc", "metrics_name"),
            registry=self.regensiter
        )
        self.mongodb_metrics_iops_usage = Gauge(
            "aliyun_mongodb_metrics_iops_usage", "aliyun mongodb metrics info",
            ("account", "db_instance_id", "db_instance_desc", "metrics_name"),
            registry=self.regensiter
        )
        self.mongodb_metrics_disk_usage = Gauge(
            "aliyun_mongodb_metrics_disk_usage", "aliyun mongodb metrics info",
            ("account", "db_instance_id", "db_instance_desc", "metrics_name"),
            registry=self.regensiter
        )
        self.mongodb_metrics_connections_usage = Gauge(
            "aliyun_mongodb_metrics_connections_usage", "aliyun mongodb metrics info",
            ("account", "db_instance_id", "db_instance_desc", "metrics_name"),
            registry=self.regensiter
        )


    def init_account_info(self, key_id:str, secret:str, region:str):
        """
        初始化阿里云授权账号
        :param key_id: accessKeyId 字段，授权账户
        :param secret: accessSecret 字段，授权密码
        :param region: 区域信息
        :return: client 实例
        """
        client = AcsClient(key_id, secret, region)
        return client

    def get_account_instance_info(self, client: object):
        """
        :param client: 账号实例
        通过账号查询下属所有的实例信息
        :return: 返回实例列表
        """
        # 初始化最终返回的实例列表
        instance_list = []

        request = DescribeDBInstancesRequest()
        request.set_accept_format('json')

        response = client.do_action_with_exception(request)
        # 返回值为字节，转字典
        res = json.loads(str(response, encoding="utf-8"))

        # 将多个返回值中实例 ID 筛选出，返回列表
        try:
            db_instance_list = res["DBInstances"]["DBInstance"]
        except (KeyError, Exception) as e:
            logging.error(f"返回值异常，请检查！ {e}")
            logging.error(f"请求原始返回数据为： {res}")
            raise RuntimeError("监控账号获取实例ID失败！")

        # 改动：单个实例信息以实例id为键，实例描述为值{“实例id”：“实例描述”},实例描述为空则使用实例id
        try:
            for one_instance in db_instance_list:
                instance_desc = one_instance.get("DBInstanceDescription", one_instance["DBInstanceId"])
                instance_list.append({one_instance["DBInstanceId"]: instance_desc})

        except KeyError as e:
            logging.error(f"推测账号信息返回值为空，请检查 {e}")
            logging.error(f"阿里云返回 instance list 信息为 {db_instance_list}")
            raise RuntimeError("获取数据疑似为空！")
        except Exception as exceptions:
            logging.error(f"获取实例 ID 过程发生异常，请检查！ {exceptions}")
            raise RuntimeError("其他类型错误！")

        return instance_list


    def deal_with_metrcis_info(self, requests: dict):
        """
        阿里云 DescribeDBInstancePerformance 参数返回模板抽象，都通过该函数处理返回字段，最终返回value
        :param request: 阿里云返回的所有数据信息，最终只返回单个 value 值
        :return: value str
        简单格式如下
        {
        "PerformanceKeys": {
            "PerformanceKey": [
                {
                    "ValueFormat": "mem_usage",
                    "PerformanceValues": {
                        "PerformanceValue": [
                            {
                                "Value": "27.62",
                                "Date": "2020-12-03T08:10:00Z"
                            },]
                    },
                    "Unit": "%",
                    "Key": "MemoryUsage"
                    }
                ]
            },
        "RequestId": "E83B82DC-64D1-4843-BA34-7753D7EC08AF",
        "EndTime": "2020-12-03T08:16Z",
        "StartTime": "2020-12-03T08:11Z"
        }
        """
        # 从阿里返回值中获取当前的 value 值
        try:
            performance_key = requests["PerformanceKeys"]["PerformanceKey"]
            performance_value = performance_key[0]["PerformanceValues"]["PerformanceValue"]
            value = performance_value[0]["Value"]
        except (KeyError, Exception) as e:
            logging.error(f"返回值异常，请检查！ {e}")
            logging.error(f"阿里云原始返回数据为： {requests}")
            raise RuntimeError("阿里云返回全部字段处理失败！")

        return value

    def get_metrcics_info(self, client: object, db_instance_id: str, db_instance_desc: str, account: str, key: str):
        """
        获取实例的各项监控指标使用情况
        :key 值不固定详情见 https://www.alibabacloud.com/help/zh/doc-detail/64048.htm
        :param key: 指标类型
        :param db_instance_id: 实例id
        :param db_instance_desc: 实例描述信息
        :param account: 账号信息，用域生成metrics
        :param client: client 实例
        :return: none
        """
        request = DescribeDBInstancePerformanceRequest()
        request.set_accept_format('json')
        request.set_Key(key)
        request.set_StartTime(self.start_time)
        request.set_EndTime(self.end_time)
        request.set_DBInstanceId(db_instance_id)

        response = client.do_action_with_exception(request)
        res = json.loads(str(response, encoding="utf-8"))

        value = self.deal_with_metrcis_info(res)

        if key == "CpuUsage":
            self.mongodb_metrics_cpu_usage.labels(account, db_instance_id, db_instance_desc, key).set(value)
        elif key == "MemoryUsage":
            self.mongodb_metrics_memory_usage.labels(account, db_instance_id, db_instance_desc, key).set(value)
        elif key == "IOPSUsage":
            self.mongodb_metrics_iops_usage.labels(account, db_instance_id, db_instance_desc, key).set(value)
        elif key == "DiskUsage":
            self.mongodb_metrics_disk_usage.labels(account, db_instance_id, db_instance_desc, key).set(value)
        elif key == "MongoDB_Connections":
            self.mongodb_metrics_connections_usage.labels(account, db_instance_id, db_instance_desc, key).set(value)


    def main(self):
        # 读取配置文件获取账号信息
        with open(self.config_file) as file:
            content = yaml.safe_load(file)
            config = content.get("config_file")

        # 遍历账号，获取账号下实例信息
        for one_account in config:

            client = self.init_account_info(one_account["accessKeyId"],
                                            one_account["accessSecret"],
                                            one_account["RegionId"])
            instance_list = self.get_account_instance_info(client)
            print(instance_list)

            account_number = account_uid[one_account["accessKeyId"]].value

            # 为各个实例查询相应的监控项信息
            for one_instance in instance_list:
                (instance_id, instance_desc), = one_instance.items()
                print(one_instance.items())
                self.get_metrcics_info(client, instance_id, instance_desc, account_number, "CpuUsage")
                self.get_metrcics_info(client, instance_id, instance_desc, account_number, "MemoryUsage")
                self.get_metrcics_info(client, instance_id, instance_desc, account_number, "IOPSUsage")
                self.get_metrcics_info(client, instance_id, instance_desc, account_number, "DiskUsage")
                self.get_metrcics_info(client, instance_id, instance_desc, account_number, "MongoDB_Connections")


app = Flask(__name__)
cache = Cache(app, config={"CACHE_TYPE": "simple"})
cache.init_app(app)


@app.route("/metrics")
@cache.cached(timeout=60)
def web():
    am = aliyun_mongodb()
    am.main()
    return Response(generate_latest(am.regensiter), mimetype="text/plain")

if __name__ == '__main__':
    am = aliyun_mongodb()
    app.run(host="0.0.0.0", port="5555", debug=True)
