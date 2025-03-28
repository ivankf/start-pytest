import os
import threading
from tests.utils.k8s_utils import get_k8s_client

condition = os.getenv("MODE") == "COMMUNITY_THREE_THREE"


class GlobalConfig:
    _instance = None
    _lock = threading.Lock()  # 线程锁

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:  # 加锁，确保线程安全
                if cls._instance is None:
                    cls._instance = super(GlobalConfig, cls).__new__(cls)
                    cls._instance.__init__()
        return cls._instance

    def __init__(self):
        if not hasattr(self, "initialized"):
            self.initialized = True
            # 初始化逻辑
            self.k8s_client = get_k8s_client()
            self.namespace = os.getenv("NAMESPACE")


global_config = GlobalConfig()
