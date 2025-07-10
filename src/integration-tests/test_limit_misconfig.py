# Copyright 2024 Bloomberg Finance L.P.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Testing primary-replica storage limits misconfiguration.
"""

import json
import re
import time

import blazingmq.dev.it.testconstants as tc
from blazingmq.dev.it.fixtures import (  # pylint: disable=unused-import
    Cluster,
    order,
    cluster,
    multi_node,
    tweak,
    start_cluster,
)
from blazingmq.dev.it.process.admin import AdminClient
from blazingmq.dev.it.process.client import Client
from blazingmq.dev.it.process.proc import Process


@start_cluster(False)
@tweak.cluster.cluster_attributes.is_cslmode_enabled(False)
@tweak.cluster.cluster_attributes.is_fsmworkflow(False)
@tweak.cluster.elector.quorum(4)
@tweak.cluster.partition_config.max_journal_file_size(900)
# @tweak.cluster.partition_config.max_journal_file_size(584)
def test_storage_limits_misconfig(multi_node: Cluster, domain_urls: tc.DomainUrls,  # pylint: disable=unused-argument
                                                        ) -> None:
    cluster = multi_node
    uri_priority = domain_urls.uri_priority

    # Modify cluster config for node "east1": set quorum to 0
    with open(
        cluster.work_dir.joinpath(
            cluster.config.nodes["east1"].config_dir, "clusters.json"
        ),
        "r+",
        encoding="utf-8",
    ) as f:
        data = json.load(f)
        data["myClusters"][0]["elector"]["quorum"] = 0
        f.seek(0)
        json.dump(data, f, indent=4)
        f.truncate()

    # Modify cluster config for node "east2": set maxJournalFileSize=524
    with open(
        cluster.work_dir.joinpath(
            cluster.config.nodes["east2"].config_dir, "clusters.json"
        ),
        "r+",
        encoding="utf-8",
    ) as f:
        data = json.load(f)
        data["myClusters"][0]["partitionConfig"]["maxJournalFileSize"] = 527 # + 180 #527
        f.seek(0)
        json.dump(data, f, indent=4)
        f.truncate()

    # Start east1, west1, and west2
    cluster.start_node("east1")
    cluster.start_node("east2")
    cluster.start_node("west1")
    cluster.start_node("west2")
    # cluster.start_nodes()

    # Wait until "east1" becomes leader. It is the only possible leader because only it has
    # quorum = 0
    leader = cluster.wait_leader()

    # leader = cluster.last_known_leader
    assert leader.name == "east1"

    # proxy = next(cluster.proxy_cycle())
    # print("my proxies: ", cluster.proxies())
    # print("my nodes: ", cluster.nodes())
    # proxy = cluster.nodes()[0]
    # proxy = cluster.proxies()[0]

    # # Create producer and consumer
    producer = leader.create_client("producer")
    producer.open(uri_priority, flags=["write,ack"], succeed=True)

    # # consumer = proxy.create_client("consumer")
    # # consumer.open(
    # #     uri_priority, flags=["read"], max_unconfirmed_messages=1, succeed=True
    # # )

    # Post messages
    for i in range(1, 4):
        time.sleep(1)  
        producer.post(uri_priority, [f"msg{i}"], succeed=True, wait_ack=True)


    # assert False, "Test not implemented yet"


