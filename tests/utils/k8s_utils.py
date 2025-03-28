import os

from kubernetes import client, config

_v1_client = None


def get_k8s_client(config_file=None):
    global _v1_client
    if _v1_client is None:
        try:
            if config_file:
                config.load_kube_config(config_file=config_file)
            else:
                config.load_kube_config("~/.kube/config")
            _v1_client = client.CoreV1Api()
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Kubernetes client: {e}")
    return _v1_client


def get_namespace():
    return os.getenv("NAMESPACE")

def get_pod_ip_by_name(k8s_client, pod_name, namespace):
    return k8s_client.read_namespaced_pod(pod_name, namespace).status.pod_ip


def get_meta_pod_list(k8s_client, namespace):
    pod = k8s_client.list_namespaced_pod(namespace)
    return [p.metadata.name for p in pod.items if "cnosdb-meta" in p.metadata.name]


def get_data_pod_list(k8s_client, namespace):
    pod = k8s_client.list_namespaced_pod(namespace)


def main():
    pass


if __name__ == '__main__':
    main()
