##!/usr/bin/env python
"""

rest tasks

"""
from __future__ import absolute_import
import base64
import sys
sys.path=["../lib"] + sys.path
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from couchbase.document import View
from app.celery import celery
import app
import testcfg as cfg
import json
import time
import random
import eventlet
from eventlet.green import urllib2
from celery.utils.log import get_task_logger
from cache import ObjCacher, CacheHelper
logger = get_task_logger(__name__)

if cfg.SERIESLY_IP != '':
    from seriesly import Seriesly

###
SDK_IP = '127.0.0.1'
SDK_PORT = 50008
###


@celery.task
def multi_query(count, design_doc_name, view_name, params = None, bucket = "default", password = "", type_ = "view", batch_size = 100, hosts = None):

    if params is not None:
        params = urllib2.urllib.urlencode(params)

    pool = eventlet.GreenPool(batch_size)

    api = '%s/_design/%s/_%s/%s?%s' % (bucket,
                                       design_doc_name, type_,
                                       view_name, params)

    qtime = data = url = None

    args = dict(api=api, hosts=hosts)
    for qtime, data, url in pool.imap(send_query, [args for i in xrange(count)]):
        pass

    if cfg.SERIESLY_IP != '' and qtime is not None:
        # store the most recent query response time 'qtime' into seriesly
        seriesly = Seriesly(cfg.SERIESLY_IP, 3133)
        #TODO: do not hardcode fast...we should have per/testdbs
        db='fast'
        seriesly[db].append({'query_latency' : qtime})

    # log to logs/celery-query.log
    try:
        rc = data.read()[0:200]
    except Exception:
        rc = "exception reading query response"

    logger.error('\n')
    logger.error('url: %s' % url)
    logger.error('latency: %s' % qtime)
    logger.error('data: %s' % rc)

def send_query(args):

    api = args['api']
    hosts = args['hosts']

    if hosts and len(hosts) > 0:
        host = hosts[random.randint(0,len(hosts) - 1)]
        capiUrl = "http://%s/couchBase/" % (host)
    else:
        capiUrl = "http://%s:%s/couchBase/" % (cfg.COUCHBASE_IP, cfg.COUCHBASE_PORT)

    url = capiUrl + api

    qtime, data = None, None
    try:
        qtime, data = timed_url_request(url)

    except urllib2.URLError as ex:
        logger.error("Request error: %s" % ex)

    return qtime, data, url


def timed_url_request(url):
    start = time.time()
    data = url_request(url)
    end = time.time()
    qtime = end - start
    return qtime, data

def default_url_headers():
    authorization = base64.encodestring('%s:%s' % (cfg.COUCHBASE_USER, cfg.COUCHBASE_PWD))

    headers = {'Content-Type': 'application/json',
               'Authorization': 'Basic %s' % authorization,
               'Accept': '*/*'}
    return headers

def url_request(url, headers = None):
    if headers is None:
        headers = default_url_headers()

    req = urllib2.Request(url, headers = headers)
    data = urllib2.urlopen(req)
    return data


def _send_msg(message):
    sdk_client = eventlet.connect((SDK_IP, SDK_PORT))
    sdk_client.sendall(json.dumps(message))

@celery.task
def perform_bucket_create_tasks(bucketMsg):
    rest = create_rest()
    if "default" in bucketMsg:
        create_default_buckets(rest, bucketMsg["default"])

    if "sasl" in bucketMsg:
        create_sasl_buckets(rest, bucketMsg["sasl"])

    if "standard" in bucketMsg:
        create_standard_buckets(rest, bucketMsg["standard"])

def parseBucketMsg(bucket):
    bucketMsg = {'count': 1,
                 'ramQuotaMB': 1000,
                 'replicas': 1,
                 'replica_index': 1,
                 'type': 'couchbase'
                 }

    if "count" in bucket:
        bucketMsg['count'] = int(bucket['count'])
    if "quota" in bucket:
        bucketMsg['ramQuotaMB'] = int(bucket['quota'])
    if "replicas" in bucket:
        bucketMsg['replicas'] = int(bucket['replicas'])
    if "replica_index" in bucket:
        bucketMsg['replica_index'] = int(bucket['replica_index'])
    if "type" in bucket:
        bucketMsg['type'] = bucket['type']

    return bucketMsg

def create_default_buckets(rest, bucketMsg):
    bucketMsgParsed = parseBucketMsg(bucketMsg)

    rest.create_bucket(bucket="default",
                       ramQuotaMB = bucketMsgParsed['ramQuotaMB'],
                       replicaNumber = bucketMsgParsed['replicas'],
                       proxyPort = 11211,
                       authType = "none",
                       saslPassword = None,
                       bucketType = bucketMsgParsed['type'],
                       replica_index = bucketMsgParsed['replica_index'])

def create_sasl_buckets(rest, bucketMsg):
    bucketMsgParsed = parseBucketMsg(bucketMsg)

    for i in range(bucketMsgParsed['count']):
        if i == 0:
            name = "saslbucket"
        else:
            name = "saslbucket" + str(i)
        rest.create_bucket(bucket = name,
                           ramQuotaMB = bucketMsgParsed['ramQuotaMB'],
                           replicaNumber = bucketMsgParsed['replicas'],
                           proxyPort = 11211,
                           authType = "sasl",
                           saslPassword = "password",
                           bucketType = bucketMsgParsed['type'],
                           replica_index = bucketMsgParsed['replica_index'])

def create_standard_buckets(rest, bucketMsg):
    bucketMsgParsed = parseBucketMsg(bucketMsg)

    for i in range(bucketMsgParsed['count']):
        if i == 0:
            name = "standardbucket"
        else:
            name = "standardbucket" + str(i)
        rest.create_bucket(bucket = name,
                           ramQuotaMB = bucketMsgParsed['ramQuotaMB'],
                           replicaNumber = bucketMsgParsed['replicas'],
                           proxyPort = 11214+i,
                           authType = "none",
                           saslPassword = None,
                           bucketType = bucketMsgParsed['type'],
                           replica_index = bucketMsgParsed['replica_index'])


@celery.task
def perform_view_tasks(viewMsg):
    rest = create_rest()

    if "create" in viewMsg:
        ddocMsg = parseDdocMsg(viewMsg['create'])
        for ddoc_name, views in ddocMsg.iteritems():
            view_list = []
            bucket_name = ''
            for view in views:
                view_list.append(View(view['view_name'], view['map_func'], view['red_func'],
                                      view['dev_view'], view['is_spatial']))
                bucket_name = view['bucket_name']

            bucket_obj = rest.get_bucket(bucket_name, 2, 2)
            rest.create_ddoc(ddoc_name, bucket_obj, view_list)

    if "delete" in viewMsg:
        for view in viewMsg['delete']:
            viewMsgParsed = parseViewMsg(view)
            bucket_obj = rest.get_bucket(viewMsgParsed['bucket_name'], 2, 2)
            rest.delete_view(bucket_obj, viewMsgParsed['ddoc_name'])

def parseDdocMsg(views):
    ddocs = {}
    for view in views:
        viewMsg = parseViewMsg(view)
        if viewMsg['ddoc_name'] in ddocs:
            ddocs[viewMsg['ddoc_name']].append(viewMsg)
        else:
            ddocs[viewMsg['ddoc_name']] = []
            ddocs[viewMsg['ddoc_name']].append(viewMsg)
    return ddocs

def parseViewMsg(view):

    viewMsg = {'ddoc_name': 'ddoc1',
               'view_name': 'view1',
               'map_func': 'function (doc) { emit(null, doc);}',
               'red_func': None,
               'dev_view': True,
               'is_spatial': False,
               'bucket_name': 'default'
               }

    if 'ddoc' in view:
        viewMsg['ddoc_name'] = view['ddoc']
    if 'view' in view:
        viewMsg['view_name'] = view['view']
    if 'map' in view:
        viewMsg['map_func'] = view['map']
    if 'reduce' in view:
        viewMsg['red_func'] = view['reduce']
    if 'dev' in view:
        if view['dev'] == "True":
            viewMsg['dev_view'] = True
        elif view['dev'] == "False":
            viewMsg['dev_view'] = False
    if 'spatial' in view:
        if view['spatial'] == "True":
            viewMsg['is_spatial'] = True
        elif view['spatial'] == "False":
            viewMsg['is_spatial'] = False
    if 'bucket' in view:
        viewMsg['bucket_name'] = view['bucket']

    return viewMsg

@celery.task
def perform_admin_tasks(adminMsg, cluster_id=cfg.CB_CLUSTER_TAG+"_status"):
    app.workload_manager.updateClusterStatus()
    clusterStatus = CacheHelper.clusterstatus(cluster_id) or ClusterStatus(cluster_id)
    rest = clusterStatus.node_rest()

    # Add nodes
    servers = adminMsg["rebalance_in"]
    add_nodes(rest, servers, cluster_id)

    # Get all nodes
    allNodes = []
    for node in rest.node_statuses():
        allNodes.append(node.id)

    # Remove nodes
    servers = adminMsg["rebalance_out"]
    toBeEjectedNodes  = remove_nodes(rest, servers, adminMsg["involve_orchestrator"], cluster_id)

    # Failover Node
    servers = adminMsg["failover"]
    auto_failover_servers = adminMsg["auto_failover"]
    only_failover = adminMsg["only_failover"]
    add_back_servers = adminMsg["add_back"]
    failoverNodes = failover_nodes(rest, servers, only_failover, adminMsg["involve_orchestrator"], cluster_id)
    autoFailoverNodes = auto_failover_nodes(rest, auto_failover_servers, only_failover, adminMsg["involve_orchestrator"], cluster_id)

    app.workload_manager.updateClusterStatus()
    clusterStatus = CacheHelper.clusterstatus(cluster_id) or ClusterStatus(cluster_id)
    rest = clusterStatus.node_rest()
    addBackNodes = add_back_nodes(rest, add_back_servers, autoFailoverNodes+failoverNodes)
    toBeEjectedNodes.extend(failoverNodes)
    toBeEjectedNodes.extend(autoFailoverNodes)
    for node in addBackNodes:
        toBeEjectedNodes.remove(node)

    # SoftRestart a node
    servers = adminMsg["soft_restart"]
    restart(servers, cluster_id=cluster_id)

    # HardRestart a node
    servers = adminMsg["hard_restart"]
    restart(servers, type='hard', cluster_id=cluster_id)

    if not only_failover and (len(allNodes) > 0 or len(toBeEjectedNodes) > 0):
        logger.error("Rebalance")
        logger.error(allNodes)
        logger.error(toBeEjectedNodes)
        rest.rebalance(otpNodes=allNodes, ejectedNodes=toBeEjectedNodes)


def monitorRebalance():
    rest = create_rest()
    rebalance_success = rest.monitorRebalance()
    return rebalance_success


@celery.task
def perform_xdcr_tasks(xdcrMsg):
    logger.error(xdcrMsg)
    src_master = create_server_obj()
    remote_id = ''
    if len(cfg.CB_REMOTE_CLUSTER_TAG) > 0:
        remote_id = cfg.CB_REMOTE_CLUSTER_TAG[0]+"_status"
    else:
        logger.error("No remote cluster tag. Can not create xdcr")
        return
    clusterStatus = CacheHelper.clusterstatus(remote_id) or ClusterStatus(remote_id)
    remote_ip = clusterStatus.get_random_host().split(":")[0]

    dest_master = create_server_obj(server_ip=remote_ip, username=xdcrMsg['dest_cluster_rest_username'],
                                    password=xdcrMsg['dest_cluster_rest_pwd'])
    dest_cluster_name = xdcrMsg['dest_cluster_name']
    xdcr_link_cluster(src_master, dest_master, dest_cluster_name)
    xdcr_start_replication(src_master, dest_cluster_name)

    if xdcrMsg['replication_type'] == "bidirection":
        src_cluster_name = dest_cluster_name + "_temp"
        xdcr_link_cluster(dest_master, src_master, src_cluster_name)
        xdcr_start_replication(dest_master, src_cluster_name)

def xdcr_link_cluster(src_master, dest_master, dest_cluster_name):
    rest_conn_src = RestConnection(src_master)
    rest_conn_src.add_remote_cluster(dest_master.ip, dest_master.port,
                                 dest_master.rest_username,
                                 dest_master.rest_password, dest_cluster_name)

def xdcr_start_replication(src_master, dest_cluster_name):
        rest_conn_src = RestConnection(src_master)
        for bucket in rest_conn_src.get_buckets():
            (rep_database, rep_id) = rest_conn_src.start_replication("continuous",
                                                                     bucket.name, dest_cluster_name)
            logger.error("rep_database: %s rep_id: %s" % (rep_database, rep_id))

def add_nodes(rest, servers='', cluster_id=cfg.CB_CLUSTER_TAG+"_status"):
    if servers.find('.') != -1 or servers == '':
        servers = servers.split()
    else:
        clusterStatus = CacheHelper.clusterstatus(cluster_id) or ClusterStatus(cluster_id)
        count = int(servers)
        if (len(clusterStatus.all_available_hosts) - len(clusterStatus.nodes)) >= int(count):
            servers = list(set(clusterStatus.all_available_hosts) - set(clusterStatus.get_all_hosts()))
        else:
            logger.error("Add nodes request invalid. # of nodes outside cluster is not enough")
            return
        servers = servers[:count]

    for server in servers:
        logger.error("Adding node %s" % server)
        ip, port = parse_server_arg(server)
        rest.add_node(cfg.COUCHBASE_USER, cfg.COUCHBASE_PWD, ip, port)

def pick_nodesToRemove(servers='', involve_orchestrator=False, cluster_id=cfg.CB_CLUSTER_TAG+"_status"):
    if servers.find('.') != -1 or servers == '':
        servers = servers.split()
    else:
        clusterStatus = CacheHelper.clusterstatus(cluster_id) or ClusterStatus(cluster_id)
        count = int(servers)
        temp_count = count
        servers = []
        if involve_orchestrator:
            servers.append("%s:%s" % (clusterStatus.orchestrator.ip, clusterStatus.orchestrator.port))
            temp_count = temp_count -1

        if len(clusterStatus.nodes) > count:
            non_orchestrator_servers = list(set(clusterStatus.get_all_hosts()) -
                               set(["%s:%s" % (clusterStatus.orchestrator.ip, clusterStatus.orchestrator.port)]))
            servers.extend(non_orchestrator_servers[:temp_count])
        else:
            logger.error("Remove nodes request invalid. # of nodes in cluster is not enough")
            return []

    return servers

def remove_nodes(rest, servers='', remove_orchestrator=False, cluster_id=cfg.CB_CLUSTER_TAG+"_status"):
    toBeEjectedNodes = []
    servers = pick_nodesToRemove(servers, remove_orchestrator, cluster_id)

    for server in servers:
        ip, port = parse_server_arg(server)
        for node in rest.node_statuses():
            if "%s" % node.ip == "%s" % ip and\
                "%s" % node.port == "%s" % port:
                logger.error("Removing node %s" % node.id)
                toBeEjectedNodes.append(node.id)

    return toBeEjectedNodes

def failover_nodes(rest, servers='', only_failover=False, failover_orchestrator=False, cluster_id=cfg.CB_CLUSTER_TAG+"_status"):
    toBeEjectedNodes = []
    servers = pick_nodesToRemove(servers, failover_orchestrator, cluster_id)

    for server in servers:
        ip, port = parse_server_arg(server)
        for node in rest.node_statuses():
            if "%s" % node.ip == "%s" % ip and\
                "%s" % node.port == "%s" % port:
                logger.error("Failing node %s" % node.id)
                rest.fail_over(node.id)
                if not only_failover:
                    toBeEjectedNodes.append(node.id)
    return toBeEjectedNodes

def auto_failover_nodes(rest, servers='', only_failover=False, failover_orchestrator=False, cluster_id=cfg.CB_CLUSTER_TAG+"_status"):
    toBeEjectedNodes = []
    if servers != '':
        rest.reset_autofailover()
        rest.update_autofailover_settings(True, 30)
    servers = pick_nodesToRemove(servers, failover_orchestrator, cluster_id)

    for server in servers:
        ip, port = parse_server_arg(server)
        for node in rest.node_statuses():
            if "%s" % node.ip == "%s" % ip and\
                "%s" % node.port == "%s" % port:
                logger.error("Failing node %s" % node.id)
                failover_by_killing_mc(node.ip)
                if not only_failover:
                    toBeEjectedNodes.append(node.id)
    return toBeEjectedNodes

def failover_by_killing_mc(ip):
    node_ssh, node = create_ssh_conn(ip)
    cmd = "killall -9 memcached & killall -9 beam.smp"
    logger.error(cmd)
    result = node_ssh.execute_command(cmd, node)
    logger.error(result)
    time.sleep(40)

def add_back_nodes(rest, servers='', nodes=[]):
    addBackNodes = []
    if servers.find('.') != -1 or servers == '':
        servers = servers.split()
        for server in servers:
            for node in rest.node_statuses():
                if "%s" % node.ip == "%s" % server:
                    logger.error("Add Back node %s" % node.id)
                    rest.add_back_node(node.id)
                    addBackNodes.append(node.id)
    else:
        count = int(servers)
        servers = nodes[:count]
        for server in servers:
            logger.error("Add Back node %s" % server)
            rest.add_back_node(server)
            addBackNodes.append(server)

    return addBackNodes

def parse_server_arg(server):
    ip = server
    port = 8091
    addr = server.split(":")
    if len(addr) > 1:
        ip = addr[0]
        port = addr[1]
    return ip, port

def _dict_to_obj(dict_):
    return type('OBJ', (object,), dict_)

def restart(servers='', type='soft', cluster_id=cfg.CB_CLUSTER_TAG+"_status"):
    if servers.find('.') != -1 or servers == '':
        servers = servers.split()
    else:
        clusterStatus = CacheHelper.clusterstatus(cluster_id) or ClusterStatus(cluster_id)
        count = int(servers)
        if len(clusterStatus.nodes) >= int(count):
            servers = clusterStatus.get_all_hosts()
        else:
            logger.error("Restart nodes request invalid. # of nodes in cluster is not enough")
            return
        servers = servers[:count]

    for server in servers:
        ip, port = parse_server_arg(server)
        node_ssh, node = create_ssh_conn(ip)
        if type is not 'soft':
            logger.error('Hard Restart')
            cmd = "reboot"
        else:
            logger.error('Soft Restart')
            cmd = "/etc/init.d/couchbase-server restart"

        logger.error(cmd)
        result = node_ssh.execute_command(cmd, node)
        logger.error(result)

def create_server_obj(server_ip=cfg.COUCHBASE_IP, port=cfg.COUCHBASE_PORT,
                      username=cfg.COUCHBASE_USER, password=cfg.COUCHBASE_PWD):
    serverInfo = { "ip" : server_ip,
                   "port" : port,
                   "rest_username" : username,
                   "rest_password" :  password
    }
    node = _dict_to_obj(serverInfo)
    return node

def http_ping(ip, port, timeout=5):
    url = "http://%s:%s/pools" % (ip,port)
    try:
        data = url_request(url)
        pools_info = json.loads(data.read())

        if 'pools' in pools_info:
            pools =  pools_info["pools"]

            if len(pools) > 0:
                return True
    except Exception as ex:
        pass


def create_rest(server_ip=cfg.COUCHBASE_IP, port=cfg.COUCHBASE_PORT,
                username=cfg.COUCHBASE_USER, password=cfg.COUCHBASE_PWD):
    return RestConnection(create_server_obj(server_ip, port, username, password))

def create_ssh_conn(server_ip = '', port=22, username = cfg.SSH_USER,
               password = cfg.SSH_PASSWORD, os='linux'):
    if isinstance(server_ip, unicode):
        server_ip = str(server_ip)

    serverInfo = {"ip" : server_ip,
                  "port" : port,
                  "ssh_username" : username,
                  "ssh_password" : password,
                  "ssh_key": '',
                  "type": os
                }

    node = _dict_to_obj(serverInfo)
    shell = RemoteMachineShellConnection(node)
    return shell, node
