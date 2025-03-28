import os
import string
import asyncio
import aiohttp
import random
import pytest
import allure

from tests.utils import k8s_utils

# 获取 Kubernetes 客户端
v1 = k8s_utils.get_k8s_client()
namespace = os.environ.get("NAMESPACE")

# 常量定义
WRITE_BATCH_SIZE = 10000
WRITE_TASK_NUM = 100
TIME_GAP = int(1e12)
DB_NAME = "write"
tags = list(string.ascii_lowercase)
table_name = "ma"
headers = {"Authorization": "Basic cm9vdDo="}

# 获取 TSKV Pod 的 IP 地址
def get_tskv_addresses():
    pods = v1.list_namespaced_pod(namespace, label_selector="cnosdb.com/role=query_tskv")
    return [
        {"pod_name": pod.metadata.name, "id": str(i), "addr": f"{pod.status.pod_ip}:8902"}
        for i, pod in enumerate(pods.items)
    ]

tskv_addresses = get_tskv_addresses()

def get_random_tskv_addr():
    return random.choice(tskv_addresses)["addr"]

def get_random_tag():
    return random.choice(tags)

async def write_data(session, num, write_single):
    data = f"{table_name},ta={get_random_tag()} fa={num} {num * TIME_GAP}"
    for i in range(num + 1, num + WRITE_BATCH_SIZE):
        data += f"\n{table_name},ta={get_random_tag()} fa={i} {i * TIME_GAP}"

    addr = get_random_tskv_addr() if not write_single else tskv_addresses[0]["addr"]
    resp = await session.post(f"http://{addr}/api/v1/write?db={DB_NAME}", data=data)

    assert resp.status == 200, f"Failed to write: {await resp.text()}"

async def db_operations(session, replica, vnode_duration, shard):
    await session.post(f"http://{get_random_tskv_addr()}/api/v1/sql", data=f"drop database if exists {DB_NAME}")
    resp = await session.post(f"http://{get_random_tskv_addr()}/api/v1/sql",
                              data=f"create database {DB_NAME} with replica {replica} vnode_duration '{vnode_duration}' shard {shard}")

    assert resp.status == 200, f"Failed to create db: {await resp.text()}"

async def tf_write(write_single):
    async with aiohttp.ClientSession(headers=headers) as session:
        write_tasks = [write_data(session, num, write_single) for num in
                       range(0, WRITE_TASK_NUM * WRITE_BATCH_SIZE, WRITE_BATCH_SIZE)]
        await asyncio.gather(*write_tasks)

async def tf_result_check():
    await asyncio.sleep(20)
    async with aiohttp.ClientSession(headers=headers) as session:
        resp = await session.post(f"http://{get_random_tskv_addr()}/api/v1/sql?db={DB_NAME}",
                                  data=f"select count(*) from {table_name}")
        res = await resp.text()
        assert res, "Result check failed"

@allure.feature("Database Operations")
@pytest.mark.asyncio
async def test_database_operations():
    async with aiohttp.ClientSession(headers=headers) as session:
        await db_operations(session, 1, "365d", 1)
        await tf_write(write_single=False)
        await tf_result_check()
