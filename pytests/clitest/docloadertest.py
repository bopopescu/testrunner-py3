from clitest.cli_base import CliBaseTest
from membase.api.rest_client import RestConnection, Bucket
from couchbase.cluster import Cluster
import testconstants

class docloaderTests(CliBaseTest):

    def setUp(self):
        super(docloaderTests, self).setUp()
        self.load_filename = self.input.param("filename", "gamesim-sample")
        self.memory_quota = self.input.param("memory_quota", 100)

    def tearDown(self):
        super(docloaderTests, self).tearDown()

    def docloader_test(self):
        """We use cbdocloader to load sample files into cluster

        First use cbdocloader to load the sample files under /opt/couchbase/samples
        into the cluster. Then we try to verify all the docs are loaded into the
        cluster. We verify by compare the number of items in cluster with number of
        doc files in zipped sample file package"""

        for bucket in self.buckets:
            self.shell.execute_cbdocloader(self.couchbase_usrname, self.couchbase_password,
                                           bucket.name, self.memory_quota, self.load_filename)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
        self.verify_results(self.load_filename)

    def verify_results(self, file):
        stats_tasks = []
        for bucket in self.buckets:
            items = self.get_number_of_files(file)
            stats_tasks.append(self.cluster.async_wait_for_stats(self.servers[:self.num_servers], bucket, '',
                               'curr_items', '==', items))
            stats_tasks.append(self.cluster.async_wait_for_stats(self.servers[:self.num_servers], bucket, '',
                               'vb_active_curr_items', '==', items))

            available_replicas = self.num_replicas
            if len(self.servers) == self.num_replicas:
                available_replicas = len(self.servers) - 1
            elif len(self.servers) <= self.num_replicas:
                available_replicas = len(self.servers) - 1

            stats_tasks.append(self.cluster.async_wait_for_stats(self.servers[:self.num_servers], bucket, '',
                               'vb_replica_curr_items', '==', items * available_replicas))
            stats_tasks.append(self.cluster.async_wait_for_stats(self.servers[:self.num_servers], bucket, '',
                               'curr_items_tot', '==', items * (available_replicas + 1)))

        for task in stats_tasks:
            task.result(60)

    def get_number_of_files(self, file):
        os = "linux"
        zip_file = "%s.zip" % (file)
        self.shell.delete_backupFile(file)
#TODO: implement a new function under RestConnectionHelper to use ip:port/nodes/self info to get os info
#We can have cli test work on LINUX first
        if os == "linux":
            command = "unzip %ssamples/%s.zip" % (testconstants.LINUX_CB_PATH, file)
            output, error = self.shell.execute_command(command.format(command))
            self.shell.log_command_output(output, error)

            command = "find %s/ -name *.json | wc -l" % (file)
            output, error = self.shell.execute_command(command.format(command))
            self.shell.log_command_output(output, error)
            a = int(output[0])

            command = "find %s/design_docs/ -name *.json | wc -l" % (file)
            output, error = self.shell.execute_command(command.format(command))
            self.shell.log_command_output(output, error)
            b = int(output[0])

            number_of_items = a - b #design doc create views not items in cluster
            self.shell.delete_backupFile(file)
            return number_of_items
        #elif os == "windows":
            # try to figure out work works for windows
