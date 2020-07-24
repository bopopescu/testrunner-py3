from .xdcrbasetests import XDCRReplicationBaseTest
from threading import Thread
from time import sleep
from remote.remote_util import RemoteMachineShellConnection

"""Testing timeouts to upsert due to cbstats hash traversal"""

class CbstatsHashTest(XDCRReplicationBaseTest):

    def setUp(self):
        super(CbstatsHashTest, self).setUp()

    def tearDown(self):
        super(CbstatsHashTest, self).tearDown()

    def setup_extended(self):
        pass

    def __setup_replication_clusters(self, src_main, dest_main, src_cluster_name, dest_cluster_name):
        self._link_clusters(src_main, dest_cluster_name, dest_main)
        self._link_clusters(dest_main, src_cluster_name, src_main)

    def test_verify_mb30553(self):
        # Setup unidirectional replication
        src_cluster_name, dest_cluster_name = "remote-dest-src", "remote-src-dest"
        self.__setup_replication_clusters(self.src_main, self.dest_main, src_cluster_name, dest_cluster_name)
        self._replicate_clusters(self.src_main, dest_cluster_name)

        # Run upsert thread
        thread = Thread(target=self.run_upsert)
        thread.start()

        # Ramp up phase
        sleep(10)

        node = self.src_main.get_main_node()
        conn = RemoteMachineShellConnection()
        command = "/opt/couchbase/bin/cbstats -u cbadminbucket -p password " + node.ip + ":11210 -b default hash"
        output, error = conn.execute_command(command)
        conn.log_command_output(output, error)
        thread.join()


    def run_upsert(self):
        buckets = self._get_cluster_buckets(self.src_main)
        for bucket in buckets:
            if bucket.name == 'default':
                for keySuffix in range(1, 1000000):
                    bucket.upsert('key' + keySuffix, 'Value ' + keySuffix, replicate_to=1)





