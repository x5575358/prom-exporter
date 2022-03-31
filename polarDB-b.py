#!/usr/bin/env python
# encoding: utf-8
"""
@author: Dennis
@desc: 通过阿里云 api 获取对应状态值
1、通过账号获取账号下的集群、节点信息
  1.1 aciton = DescribeDBClustersRequest
  1.2 新字典格式
    {cluster_id: <集群id>
     DB_class: <产品规格>
     DB_nodes:
       [{node_roles: <节点身份>
         node_id: <节点id>
       },]
    }

2、通过信息取获取指定的参数值，action=DescribeDBNodePerformance
  2.1 时间默认为 60s
  2.2 cpu 和 disk 涉及到 集群信息，连接数会通过集群规模判断
  2.3 最大连接数、最大存储与集群配置成正相关
    2核4G：5T  1200连接数？？？
    2核8G：5T  1200连接数
    4核16G：10T  5000连接数
    8核32G：10T  10000连接数
    8核64G：30T  10000连接数
    32核256G：50T  64000连接数
  2.4 通过返回值生成新字典，新建函数处理各个字典，生成metrics

3、搜集返回值，拼接成web
"""


import time, json, logging
from prometheus_client import Gauge, generate_latest
from prometheus_client.core import CollectorRegistry
from flask import Flask, Response
from flask_cache import Cache
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.acs_exception.exceptions import ClientException
from aliyunsdkcore.acs_exception.exceptions import ServerException
from aliyunsdkpolardb.request.v20170801.DescribeDBNodePerformanceRequest import (
    DescribeDBNodePerformanceRequest,
)
from aliyunsdkpolardb.request.v20170801.DescribeDBClustersRequest import (
    DescribeDBClustersRequest,
)


class aliyun_polarDB_api:
    def init_client(self, access_key_id: str, access_key_secret: str, region_id: str):
        """
        :param access_key_id:
        :param access_key_secret:
        :param region_id:
        """
        client = AcsClient(
            ak=access_key_id, secret=access_key_secret, region_id=region_id
        )
        logging.basicConfig(
            filename="../log/aliyun_polarDB_api.log",
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
        )

        #  存储本次查询中所有的集群信息，为了metrics_info 准备
        self.cluster_info = []
        self.performance = []
        self.max_connects = {
            "polar.mysql.x2.medium": {"max_connect": 1200, "max_date": 5120},
            "polar.mysql.x4.medium": {"max_connect": 1200, "max_date": 5120},
            "polar.mysql.x4.large": {"max_connect": 5000, "max_date": 10240},
            "polar.mysql.x4.xlarge": {"max_connect": 10000, "max_date": 10240},
            "polar.mysql.x8.xlarge": {"max_connect": 1200, "max_date": 30720},
            "polar.mysql.x8.2xlarge": {"max_connect": 1200, "max_date": 51200},
            "polar.mysql.x8.4xlarge": {"max_connect": 1200, "max_date": 51200},
        }
        return client

    # 执行子节点查询 连接数、复制延迟、cpu使用率、磁盘使用率
    def get_polardb_performance(
        self, client, node_id: str, metrics_name, time_interval: int = 30
    ):
        """
        :param client: 初始化后得aliyun api client 实例
        :param time_interval: 获取数据的时间间隔，默认为 60秒 （单位：秒）
        :param metrics_name: 请求监控项的名称，返回对应状态值
        :param node_id 节点id，必填项
        时间格式：格式：yyyy-MM-ddTHH:mmZ（UTC时间）

        :return:<response>
        """
        try:
            request = DescribeDBNodePerformanceRequest()
            request.set_accept_format("json")

            localtime = int(time.time())
            request.set_DBNodeId(node_id)
            request.set_StartTime(
                time.strftime(
                    "%Y-%m-%dT%H:%MZ", time.gmtime(localtime - int(time_interval))
                )
            )
            request.set_EndTime(time.strftime("%Y-%m-%dT%H:%MZ", time.localtime()))
            request.set_Key(metrics_name)

            response = client.do_action_with_exception(request)
            logging.debug(str(response, encoding="utf-8"))
        except ServerException as se:
            logging.error(se.error_code + " : " + se.message)
        except ClientException as ce:
            logging.error(ce.error_code + " : " + ce.message)
        else:
            return json.loads(str(response, encoding="utf-8"))

    # 处理阿里云返回的请求数据，保存到 self.performance
    def deal_performance_rep(self, reponens):
        """
        :param reponens: 传入的json格式的阿里云请求数据原文
        :return: {metrics_name:[{timestamp:value},]}
        """
        measurements = reponens["PerformanceKeys"]["PerformanceItem"]
        # 以 PolarDBDiskUsage 为例会返回多个数据值，需挨个处理
        try:
            for one_mesure in measurements:
                self.performance.append(
                    {
                        "metrics_name": one_mesure["MetricName"],
                        "value": one_mesure["Points"]["PerformanceItemValue"][0][
                            "Value"
                        ],
                    }
                )
        except KeyError:
            logging.error(KeyError)

    # 通过 self.performance 中的数据，根据各个监控项判断后，生成metrics 数据
    def init_metrics(
        self, metrics_registry, cluster_id="", node_id="", cluster_class="", **kwargs
    ):
        """
        :param cluster_id,node_id 为了填充到metrics中
        :param cluster_class 通过集群类型判断最大连接数值
        :param metrics_registry: 初始化后的 metrics 仓库，会出现生成多个监控项的情况
        :return:
        """
        DBClusterDescription = kwargs.get("DBClusterDescription", "")
        for one in self.performance:
            # print(self.performance)
            metrics_name = one["metrics_name"]
            metrics_value = float(one["value"])
            max_connect = self.max_connects.get(cluster_class, 0)["max_connect"]
            if metrics_name == "mean_active_session":
                connect_rate = float(metrics_value) / int(max_connect)
                metrics_registry.labels(
                    cluster_id=cluster_id,
                    node_id=node_id,
                    performance_type="connect_rate",
                    DBClusterDescription=DBClusterDescription
                ).set(connect_rate)
            metrics_registry.labels(
                cluster_id=cluster_id, node_id=node_id, performance_type=metrics_name, DBClusterDescription=DBClusterDescription
            ).set(metrics_value)

    # 保存所有集群信息的metrics
    def save_cluster_info(self, cluster_response):

        clusters = cluster_response["Items"]["DBCluster"]
        # print(clusters)
        for one in clusters:
            cluster_ZoneId = one["ZoneId"]
            ResourceGroupId = one["ResourceGroupId"]
            DBClusterStatus = one["DBClusterStatus"]
            CreateTime = one["CreateTime"]
            DBClusterId = one["DBClusterId"]
            DBClusterDescription = one["DBClusterDescription"]
            DBType = one["DBType"]
            StorageUsed = one.get("StorageUsed")
            DBVersion = one["DBVersion"]
            DBNodeClass = one["DBNodeClass"]
            max_date = self.max_connects.get(DBNodeClass, 0)["max_date"]
            max_connect = self.max_connects.get(DBNodeClass, 0)["max_connect"]
            for one_node in one["DBNodes"]["DBNode"]:
                node_ZoneId = one_node["ZoneId"]
                DBNodeRole = one_node["DBNodeRole"]
                RegionId = one_node["RegionId"]
                DBNodeClass = one_node["DBNodeClass"]
                self.cluster_info_p.labels(
                    cluster_ZoneId,
                    ResourceGroupId,
                    DBClusterStatus,
                    CreateTime,
                    DBClusterId,
                    DBClusterDescription,
                    DBType,
                    DBNodeClass,
                    StorageUsed,
                    DBVersion,
                    node_ZoneId,
                    DBNodeRole,
                    RegionId,
                    DBNodeClass,
                    max_connect,
                    max_date,
                ).set(1)

    # 通过api查询该账号下所有授权的集群信息
    def get_cluster_info(self, client):
        """
        :return: 返回 json 格式，集群信息
        """
        try:
            request = DescribeDBClustersRequest()
            request.set_accept_format("json")
            response = client.do_action_with_exception(request)
            logging.debug(str(response, encoding="utf-8"))
        except ServerException as se:
            logging.error(se.error_code + " : " + se.message)
        except ClientException as ce:
            logging.error(ce.error_code + " : " + ce.message)
        else:
            return json.loads(str(response, encoding="utf-8"))

    # 通过 cluster 信息分离节点信息
    def metrics_by_cluster(self, ali_response):
        try:
            DBCluster = ali_response["Items"]["DBCluster"]
            for one in DBCluster:
                cluster_id = one["DBClusterId"]
                db_class = one["DBNodeClass"]
                db_node = one["DBNodes"]["DBNode"]
                db_DBClusterDescription = one.get("DBClusterDescription", "")
                db_disk_usedage = float(int(one["StorageUsed"]) / 1024 / 1024 / 1000)
                db_nodes = []
                # 整理同一集群下多个节点信息
                for one_node in db_node:
                    db_nodes.append(
                        {
                            "node_id": one_node["DBNodeId"],
                            "node_rules": one_node["DBNodeRole"],
                        }
                    )
                self.cluster_info.append(
                    {
                        "cluster_id": cluster_id,
                        "db_DBClusterDescription": db_DBClusterDescription,
                        "db_class": db_class,
                        "db_diskusage": db_disk_usedage,
                        "db_nodes": db_nodes,
                    }
                )
        except KeyError as e:
            logging.error(str(e.arg) + " 该参数找不到")

    def main(self):
        # 设置访问账号信息
        config_file = [
            {
                "accessKeyId": "",
                "accessSecret": "",
                "RegionId": "",
            },
        ]

        # 设置metrics信息
        self.registry = CollectorRegistry(auto_describe=False)
        event_info = Gauge(
            "aliyun_polarDB_performance",
            "this is a performance Guage",
            ["cluster_id", "node_id", "performance_type", "DBClusterDescription"],
            registry=self.registry,
        )
        event_info_spect = Gauge(
            "aliyun_polarDB_disk_useage_rate",
            "this is a performance Guage",
            ["cluster_id", "performance_type", "DBClusterDescription"],
            registry=self.registry,
        )
        self.cluster_info_p = Gauge(
            "aliyun_polarDB_meta",
            "this is a cluster meta info  Guage",
            [
                "cluster_ZoneId",
                "ResourceGroupId",
                "DBClusterStatus",
                "CreateTime",
                "DBClusterId",
                "DBClusterDescription",
                "DBType",
                "DBNodeClass",
                "StorageUsed",
                "DBVersion",
                "node_ZoneId",
                "DBNodeRole",
                "RegionId",
                "DBNodeClass",
                "max_connect",
                "max_date",
            ],
            registry=self.registry,
        )

        # 初始化账号
        for c in config_file:
            aid = c["accessKeyId"]
            aks = c["accessSecret"]
            ri = c["RegionId"]
            client = self.init_client(
                access_key_id=aid, access_key_secret=aks, region_id=ri
            )
            # 通过账号获取集群信息
            one_clusters = self.get_cluster_info(client)
            # 生成 meta info ,保存大量集群信息
            self.save_cluster_info(one_clusters)

            # 分离集群信息
            self.metrics_by_cluster(one_clusters)
            # print(self.cluster_info)
            for one_cluster in self.cluster_info:
                cluster_id = one_cluster["cluster_id"]
                cluster_class = one_cluster["db_class"]
                cluster_diskuseage = one_cluster["db_diskusage"]
                cluster_DBClusterDescription = one_cluster["db_DBClusterDescription"]
                # 生成集群磁盘使用率监控项
                max_data = self.max_connects.get(cluster_class, 0)["max_date"]
                disk_rate = int(cluster_diskuseage) / int(max_data)
                # print(disk_rate)
                event_info_spect.labels(
                    cluster_id=cluster_id, performance_type="disk_useage_rate", DBClusterDescription=cluster_DBClusterDescription
                ).set(disk_rate)
                # 分离集群下多个节点的信息
                for one_node in one_cluster["db_nodes"]:
                    node_id = one_node["node_id"]

                    # 查询节点的性能数据
                    db_usage = self.get_polardb_performance(
                        client, node_id, "PolarDBDiskUsage"
                    )
                    db_connect = self.get_polardb_performance(
                        client, node_id, "PolarDBConnections"
                    )
                    db_cpu = self.get_polardb_performance(client, node_id, "PolarDBCPU")
                    db_replicate = self.get_polardb_performance(
                        client, node_id, "PolarDBReplicaLag"
                    )
                    self.deal_performance_rep(db_usage)
                    self.deal_performance_rep(db_connect)
                    self.deal_performance_rep(db_cpu)
                    self.deal_performance_rep(db_replicate)
                    self.init_metrics(event_info, cluster_id, node_id, cluster_class, DBClusterDescription=cluster_DBClusterDescription)
            # print(self.performance)
            # print(generate_latest(event_info))



app = Flask(__name__)
cache = Cache(app, config={"CACHE_TYPE": "simple"})
cache.init_app(app)

# web 页面函数
@app.route("/metrics")
@cache.cached(timeout=60)
def web():
    ap.main()
    return Response(generate_latest(ap.registry), mimetype="text/plain")


if __name__ == "__main__":
    ap = aliyun_polarDB_api()
    app.run(host="0.0.0.0", port="19990", debug=True)

