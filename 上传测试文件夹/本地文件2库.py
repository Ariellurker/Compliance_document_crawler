import os
import requests
import sys
import time
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore

def runtime_base_dir() -> Path:
    """返回运行目录（源码/打包环境均可用）。"""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent.parent


def _load_uploader_config() -> Dict[str, Any]:
    """从运行目录读取 uploader_config.yaml，缺失时返回空字典。"""
    if not yaml:
        return {}
    config_path = runtime_base_dir() / "uploader_config.yaml"
    if not config_path.exists():
        return {}
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception:
        return {}
    base = str(runtime_base_dir())
    path_val = data.get("local_root_path") or ""
    if path_val and not os.path.isabs(path_val):
        data["local_root_path"] = os.path.normpath(os.path.join(base, path_val))
    elif not path_val:
        data["local_root_path"] = os.path.join(base, "downloads")
    return data


_uploader_cfg = _load_uploader_config()
app_id = (_uploader_cfg.get("app_id") or "").strip()
app_secret = (_uploader_cfg.get("app_secret") or "").strip()
space_id = (_uploader_cfg.get("space_id") or "").strip()
cloud_root_folder_token = (_uploader_cfg.get("cloud_root_folder_token") or "").strip()
local_root_path = (_uploader_cfg.get("local_root_path") or "").strip() or str(runtime_base_dir() / "downloads")


def _pause_before_exit() -> None:
    """打包环境下错误退出前暂停，避免窗口闪退。"""
    if getattr(sys, "frozen", False):
        try:
            input("按回车键关闭窗口...")
        except EOFError:
            pass


def _fatal(message: str, code: int = 1) -> None:
    """打印错误并退出。"""
    print(message, file=sys.stderr)
    _pause_before_exit()
    sys.exit(code)

def get_tenant_access_token(app_id: str, app_secret: str) -> Tuple[str, Exception]:
    """获取 tenant_access_token

    Args:
        app_id: 应用ID
        app_secret: 应用密钥

    Returns:
        Tuple[str, Exception]: (access_token, error)
    """
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {
        "app_id": app_id,
        "app_secret": app_secret
    }
    headers = {
        "Content-Type": "application/json; charset=utf-8"
    }
    try:
        # print(f"POST: {url}")
        # print(f"Request body: {json.dumps(payload)}")
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()

        result = response.json()
        # print(f"Response: {json.dumps(result)}")

        if result.get("code", 0) != 0:
            print(f"ERROR: failed to get tenant_access_token: {result.get('msg', 'unknown error')}", file=sys.stderr)
            return "", Exception(f"failed to get tenant_access_token: {response.text}")

        return result["tenant_access_token"], None

    except Exception as e:
        print(f"ERROR: getting tenant_access_token: {e}", file=sys.stderr)
        if hasattr(e, 'response') and e.response is not None:
            print(f"ERROR: Response text: {e.response.text}", file=sys.stderr)
        return "", e

def create_wiki_directory(tenant_access_token: str,space_id: str,dir_name: str,parent_node_token: str = "") -> Tuple[str, Exception]:
    """创建知识库目录节点（文件夹 """
    # API 端点：创建知识空间节点
    url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/{space_id}/nodes"
    
    # 请求体：创建目录节点（node_type 为 directory）
    payload = {
        "title": dir_name,  # 目录名称
        "obj_type": "docx",
        "node_type": "origin",  # 节点类型：目录
        "parent_node_token": parent_node_token  # 父节点 token（可选）
    }
    
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    
    try:
        # print(f"创建知识库目录: {dir_name} (父节点: {parent_node_token or '根目录'})")
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        result = response.json()
        
        if result.get("code") != 0:
            return "", Exception(f"创建目录失败: {result.get('msg')}")
        
        # 提取新目录的 node_token
        node_token = result["data"]["node"]["node_token"]
        print(f"目录创建成功，node_token: {node_token}")
        return node_token, None
        
    except Exception as e:
        error_msg = f"创建目录 {dir_name} 失败: {str(e)}"
        if hasattr(e, "response") and e.response is not None:
            error_msg += f" (响应: {e.response.text})"
        return "", Exception(error_msg)
    
def get_wiki_nodes(tenant_access_token: str, space_id: str, parent_node_token: str = "") -> Tuple[List[Dict], Exception]:
    """获取知识空间节点列表"""
    url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/{space_id}/nodes"
    params = {"parent_node_token": parent_node_token} if parent_node_token else {}
    headers = {"Authorization": f"Bearer {tenant_access_token}"}
    
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        result = response.json()
        if result["code"] != 0:
            return [], Exception(f"获取节点列表失败: {result['msg']}")
        return result["data"].get("items",[]), None
    except Exception as e:
        return [], e

def update_wiki_node_title(tenant_access_token: str, space_id: str, node_token: str, new_title: str) -> Tuple[bool, Exception]:
    """更新知识库节点标题"""
    # API端点：更新知识空间节点标题
    url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/{space_id}/nodes/{node_token}/update_title"
    
    # 请求体：仅包含新标题
    payload = {
        "title": new_title
    }
    
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    
    try:
        print(f"更新节点标题: {node_token} -> {new_title}")
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        result = response.json()
        
        if result.get("code") != 0:
            return False, Exception(f"更新失败: {result.get('msg')}")
        
        print(f"节点标题更新成功")
        return True, None
        
    except Exception as e:
        error_msg = f"更新节点标题失败: {str(e)}"
        if hasattr(e, "response") and e.response is not None:
            error_msg += f" (响应: {e.response.text})"
        return False, Exception(error_msg)

def upload_file(tenant_access_token: str, file_path: str, file_name: str, parent_folder_token: str) -> Tuple[str, Exception]:
    """上传文件（小文件≤20MB）

    Args:
        tenant_access_token: 租户访问令牌
        file_path: 本地文件路径
        file_name: 文件名
        parent_folder_token: 父文件夹token

    Returns:
        Tuple[str, Exception]: (文件token, 错误)
    """
    # 获取文件大小
    file_size = os.path.getsize(file_path)
    
    # 计算Adler-32校验和
    def adler32_checksum(file_path):
        with open(file_path, 'rb') as f:
            data = f.read()
            a = 1
            b = 0
            for byte in data:
                a = (a + byte) % 65521
                b = (b + a) % 65521
            return (b << 16) | a
    
    checksum = adler32_checksum(file_path)
    
    url = "https://open.feishu.cn/open-apis/drive/v1/files/upload_all"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}"
    }
    
    try:
        with open(file_path, 'rb') as f:
            files = {
                'file': (file_name, f, 'application/octet-stream')
            }
            data = {
                'file_name': file_name,
                'parent_type': 'explorer',
                'parent_node': parent_folder_token,
                'size': str(file_size),
                'checksum': str(checksum)
            }
            
            # print(f"POST: {url}")
            # print(f"Form data: {json.dumps(data)}")
            response = requests.post(url, headers=headers, data=data, files=files)
            response.raise_for_status()
            
            result = response.json()
            # print(f"Response: {json.dumps(result)}")
            
            if result.get("code", 0) != 0:
                return "", Exception(f"failed to upload file: {result.get('msg', 'unknown error')}")
            
            return result.get("data", {}).get("file_token", ""), None
            
    except Exception as e:
        print(f"ERROR: uploading file: {e}", file=sys.stderr)
        return "", e

def prepare_upload(tenant_access_token: str, file_name: str, parent_folder_token: str, file_size: int) -> Tuple[str, int, int, Exception]:
    """分片上传-预上传

    Args:
        tenant_access_token: 租户访问令牌
        file_name: 文件名
        parent_folder_token: 父文件夹token
        file_size: 文件大小

    Returns:
        Tuple[str, int, int, Exception]: (upload_id, block_size, block_num, 错误)
    """
    url = "https://open.feishu.cn/open-apis/drive/v1/files/upload_prepare"
    payload = {
        "file_name": file_name,
        "parent_type": "explorer",
        "parent_node": parent_folder_token,
        "size": file_size
    }
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    
    try:
        # print(f"POST: {url}")
        # print(f"Request body: {json.dumps(payload)}")
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        
        result = response.json()
        # print(f"Response: {json.dumps(result)}")
        
        if result.get("code", 0) != 0:
            return "", 0, 0, Exception(f"failed to prepare upload: {result.get('msg', 'unknown error')}")
        
        data = result.get("data", {})
        upload_id = data.get("upload_id", "")
        block_size = data.get("block_size", 0)
        block_num = data.get("block_num", 0)
        
        return upload_id, block_size, block_num, None
        
    except Exception as e:
        print(f"ERROR: preparing upload: {e}", file=sys.stderr)
        return "", 0, 0, e

def upload_part(tenant_access_token: str, upload_id: str, seq: int, size: int, file_data: bytes) -> Tuple[bool, Exception]:
    """分片上传-上传分片

    Args:
        tenant_access_token: 租户访问令牌
        upload_id: 上传ID
        seq: 分片序号
        size: 分片大小
        file_data: 分片数据

    Returns:
        Tuple[bool, Exception]: (是否成功, 错误)
    """
    # 计算Adler-32校验和
    def adler32_checksum(data):
        a = 1
        b = 0
        for byte in data:
            a = (a + byte) % 65521
            b = (b + a) % 65521
        return (b << 16) | a
    
    checksum = adler32_checksum(file_data)
    
    url = "https://open.feishu.cn/open-apis/drive/v1/files/upload_part"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}"
    }
    
    try:
        files = {
            'file': ('part', file_data, 'application/octet-stream')
        }
        data = {
            'upload_id': upload_id,
            'seq': str(seq),
            'size': str(size),
            'checksum': str(checksum)
        }
        
        # print(f"POST: {url}")
        # print(f"Form data: seq={seq}, size={size}")
        response = requests.post(url, headers=headers, data=data, files=files)
        response.raise_for_status()
        
        result = response.json()
        # print(f"Response: {json.dumps(result)}")
        
        if result.get("code", 0) != 0:
            return False, Exception(f"failed to upload part: {result.get('msg', 'unknown error')}")
        
        return True, None
        
    except Exception as e:
        print(f"ERROR: uploading part: {e}", file=sys.stderr)
        return False, e

def finish_upload(tenant_access_token: str, upload_id: str, block_num: int) -> Tuple[str, Exception]:
    """分片上传-完成上传

    Args:
        tenant_access_token: 租户访问令牌
        upload_id: 上传ID
        block_num: 分片数量

    Returns:
        Tuple[str, Exception]: (文件token, 错误)
    """
    url = "https://open.feishu.cn/open-apis/drive/v1/files/upload_finish"
    payload = {
        "upload_id": upload_id,
        "block_num": block_num
    }
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    
    try:
        # print(f"POST: {url}")
        # print(f"Request body: {json.dumps(payload)}")
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        
        result = response.json()
        # print(f"Response: {json.dumps(result)}")
        
        if result.get("code", 0) != 0:
            return "", Exception(f"failed to finish upload: {result.get('msg', 'unknown error')}")
        
        return result.get("data", {}).get("file_token", ""), None
        
    except Exception as e:
        print(f"ERROR: finishing upload: {e}", file=sys.stderr)
        return "", e

def upload_large_file(tenant_access_token: str, file_path: str, file_name: str, parent_folder_token: str) -> Tuple[str, Exception]:
    """上传大文件（>20MB）

    Args:
        tenant_access_token: 租户访问令牌
        file_path: 本地文件路径
        file_name: 文件名
        parent_folder_token: 父文件夹token

    Returns:
        Tuple[str, Exception]: (文件token, 错误)
    """
    file_size = os.path.getsize(file_path)
    
    # 预上传
    upload_id, block_size, block_num, err = prepare_upload(tenant_access_token, file_name, parent_folder_token, file_size)
    if err:
        return "", err
    
    # 上传分片
    with open(file_path, 'rb') as f:
        for i in range(block_num):
            # 读取分片
            data = f.read(block_size)
            if not data:
                break
            
            # 上传分片
            success, err = upload_part(tenant_access_token, upload_id, i, len(data), data)
            if err:
                return "", err
            if not success:
                return "", Exception(f"failed to upload part {i}")
    
    # 完成上传
    file_token, err = finish_upload(tenant_access_token, upload_id, block_num)
    return file_token, err

def get_file_metadata(tenant_access_token: str, file_token: str, file_type: str = "file") -> Tuple[Dict[str, Any], Exception]:
    """获取文件元数据

    Args:
        tenant_access_token: 租户访问令牌
        file_token: 文件token
        file_type: 文件类型

    Returns:
        Tuple[Dict, Exception]: (元数据, 错误)
    """
    url = "https://open.feishu.cn/open-apis/drive/v1/metas/batch_query"
    params = {
        "user_id_type": "open_id"
    }
    payload = {
        "request_docs": [
            {
                "doc_token": file_token,
                "doc_type": file_type
            }
        ],
        "with_url": True
    }
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    
    try:
        # print(f"POST: {url}")
        # print(f"Params: {json.dumps(params)}")
        # print(f"Request body: {json.dumps(payload)}")
        response = requests.post(url, headers=headers, params=params, json=payload)
        response.raise_for_status()
        
        result = response.json()
        # print(f"Response: {json.dumps(result)}")
        
        if result.get("code", 0) != 0:
            return {}, Exception(f"failed to get file metadata: {result.get('msg', 'unknown error')}")
        
        metas = result.get("data", {}).get("metas", [])
        if len(metas) == 0:
            return {}, Exception("no metadata returned")
        
        return metas[0], None
        
    except Exception as e:
        print(f"ERROR: getting file metadata: {e}", file=sys.stderr)
        return {}, e

def get_folder_contents(tenant_access_token: str, folder_token: str = "") -> Tuple[List[Dict[str, Any]], Exception]:
    """获取文件夹中的文件清单

    Args:
        tenant_access_token: 租户访问令牌
        folder_token: 文件夹token，空字符串表示根目录

    Returns:
        Tuple[List[Dict], Exception]: (文件列表, 错误)
    """
    url = "https://open.feishu.cn/open-apis/drive/v1/files"
    params = {
        "folder_token": folder_token,
        "page_size": 200
    }
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    all_files = []
    page_token = ""
    
    try:
        while True:
            if page_token:
                params["page_token"] = page_token
            
            # print(f"GET: {url}")
            # print(f"Params: {json.dumps(params)}")
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            result = response.json()
            # print(f"Response: {json.dumps(result)}")
            
            if result.get("code", 0) != 0:
                return [], Exception(f"failed to get folder contents: {result.get('msg', 'unknown error')}")
            
            files = result.get("data", {}).get("files", [])
            all_files.extend(files)
            
            has_more = result.get("data", {}).get("has_more", False)
            if not has_more:
                break
            page_token = result.get("data", {}).get("next_page_token", "")
            if not page_token:
                break
                
        return all_files, None
        
    except Exception as e:
        print(f"ERROR: getting folder contents: {e}", file=sys.stderr)
        return [], e
    
def delete_file(tenant_access_token: str, file_token: str, file_type: str) -> Exception:
    """删除文件或快捷方式"""
    url = f"https://open.feishu.cn/open-apis/drive/v1/files/{file_token}?type={file_type}"
    headers = {"Authorization": f"Bearer {tenant_access_token}"}
    try:
        response = requests.delete(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        if result.get("code") != 0:
            return Exception(f"删除失败: {result.get('msg')}")
        return None
    except Exception as e:
        return Exception(f"删除接口调用失败: {str(e)}")
    
def move_file_to_wiki(tenant_access_token: str,space_id: str,parent_node_token: str,file_token: str) -> Tuple[str, Exception]:
    """将云空间文件移动到知识空间节点
    
    Args:
        tenant_access_token: 租户访问凭证
        space_id: 知识空间ID
        parent_node_token: 知识空间父节点token（空表示根节点）
        file_token: 云空间文件token
        file_type: 文件类型（file/doc/docx等）
    
    Returns:
        Tuple[str, Exception]: (知识空间节点token, 错误信息)
    """
    url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/{space_id}/nodes/move_docs_to_wiki"
    
    payload = {
        "parent_wiki_token": parent_node_token,  # 知识空间父节点token
        "obj_type": "file",                  # 文件类型
        "obj_token": file_token                 # 云空间文件token
    }
    
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        result = response.json()
        
        if result.get("code") != 0:
            return "", Exception(f"移动失败: {result.get('msg')}")
        
        # 处理异步任务（若返回task_id则需轮询结果）
        if "task_id" in result["data"]:
            task_id = result["data"]["task_id"]
            return wait_for_move_task(tenant_access_token, task_id)
        return result["data"]["wiki_token"], None  # 同步完成时直接返回节点token
    
    except Exception as e:
        error_msg = f"移动文件到知识库失败: {str(e)}"
        if hasattr(e, "response") and e.response is not None:
            error_msg += f" (响应: {e.response.text})"
        return "", Exception(error_msg)

def wait_for_move_task(tenant_access_token: str, task_id: str) -> Tuple[str, Exception]:
    """轮询移动文件任务结果
    
    Args:
        tenant_access_token: 租户访问凭证
        task_id: 移动任务ID
    
    Returns:
        Tuple[str, Exception]: (知识空间节点token, 错误信息)
    """
    url = f"https://open.feishu.cn/open-apis/wiki/v2/tasks/{task_id}?task_type=move"
    headers = {"Authorization": f"Bearer {tenant_access_token}"}
    
    for _ in range(30):  # 最多等待30秒
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            result = response.json()
            
            if result.get("code") != 0:
                return "", Exception(f"查询任务失败: {result.get('msg')}")
            
            move_result = result["data"]["task"]["move_result"][0]
            if move_result["status"] == 0:  # 任务成功
                return move_result["node"]["node_token"], None
            elif move_result["status"] < 0:  # 任务失败
                return "", Exception(f"移动任务失败: {move_result['status_msg']}")
            
            time.sleep(1)  # 未完成，等待1秒后重试
        except Exception as e:
            return "", Exception(f"轮询任务失败: {str(e)}")
    
    return "", Exception("移动任务超时")

def upload_local_to_wiki(
    tenant_access_token: str,
    space_id: str,
    local_file_path: str,
    file_name: str,
    parent_node_token: str = ""
) -> Tuple[str, Exception]:
    """上传本地文件到知识空间节点
    
    Args:
        tenant_access_token: 租户访问凭证
        space_id: 知识空间ID
        local_file_path: 本地文件路径
        parent_node_token: 知识空间父节点token（空表示根节点）
    
    Returns:
        Tuple[str, Exception]: (知识空间节点token, 错误信息)
    """
    # 步骤1：上传文件到个人云空间
    file_size = os.path.getsize(local_file_path)
    if file_size <= 20 * 1024 * 1024:  # 20MB
        file_token, err = upload_file(tenant_access_token, local_file_path, file_name, cloud_root_folder_token)
    else:
        file_token, err = upload_large_file(tenant_access_token, local_file_path, file_name, cloud_root_folder_token)
    
    if err:
        print(f"ERROR: 上传文件 {file_name} 失败: {err}", file=sys.stderr)
        return "" , err
    
    # 步骤2：移动文件到知识空间
    node_token, err = move_file_to_wiki(
        tenant_access_token, space_id, parent_node_token, file_token
    )
    if err:
        return "", err
    
    cloud_files, err = get_folder_contents(tenant_access_token, cloud_root_folder_token)
    shortcut_token = [i["token"] for i in cloud_files if i["name"]==file_name]
    for SCTK in shortcut_token:
        err = delete_file(tenant_access_token, SCTK, "shortcut")
        if err:
            print(f"WARNING: 临时文件删除失败: {err}")
    return node_token, None

def sync_local_to_wiki(tenant_access_token: str, local_path: str, space_id: str = "", parent_node_token:str = "") -> Exception:
    """同步本地文件夹到知识库

    Args:
        tenant_access_token: 租户访问令牌
        local_path: 本地文件夹路径
        space_id: 知识空间节点

    Returns:
        Exception: 错误信息
    """
    try:
        print(f"开始同步本地文件夹: {local_path} 到知识库: {parent_node_token or '根目录'}")
        
        # 获取知识空间节内容
        cloud_files, err = get_wiki_nodes(tenant_access_token, space_id, parent_node_token)
        if err:
            return err
        
        # 创建文件名到文件信息的映射
        cloud_file_map = {f["title"]: f for f in cloud_files}
        
        # 遍历本地文件夹
        for item in os.listdir(local_path):
            local_item_path = os.path.join(local_path, item)
            
            if os.path.isdir(local_item_path):
                # 处理文件夹
                if item in cloud_file_map:
                    # 文件夹已存在，递归处理
                    print(f"文件夹已存在: {item}, 进入递归处理")
                    sub_folder_token = cloud_file_map[item]["node_token"]
                    err = sync_local_to_wiki(tenant_access_token, local_item_path, space_id, sub_folder_token)
                    if err:
                        print(f"ERROR: 同步子文件夹 {item} 失败: {err}", file=sys.stderr)
                        continue
                else:
                    # 文件夹不存在，创建新文件夹
                    print(f"创建新文件夹: {item}")
                    new_folder_token, err = create_wiki_directory(tenant_access_token, space_id, item, parent_node_token)
                    if err:
                        print(f"ERROR: 创建文件夹 {item} 失败: {err}", file=sys.stderr)
                        continue
                    
                    # 递归处理新创建的文件夹
                    err = sync_local_to_wiki(tenant_access_token, local_item_path, space_id, new_folder_token)
                    if err:
                        print(f"ERROR: 同步新文件夹 {item} 失败: {err}", file=sys.stderr)
                        continue
                        
            elif os.path.isfile(local_item_path):
                # 处理文件
                file_name = item
                file_ext = os.path.splitext(file_name)[1].lower()
                if file_ext in {".html", ".htm"}:
                    print(f"跳过 HTML 文件: {file_name}")
                    continue
                local_modified_time = int(os.path.getmtime(local_item_path))
                
                if file_name in cloud_file_map:
                    # 文件已存在，检查是否需要更新
                    cloud_file = cloud_file_map[file_name]
                    node_token = cloud_file["node_token"]
                    cloud_file_token = cloud_file["obj_token"]
                    cloud_file_type = cloud_file["obj_type"]
                    
                    # 获取云端文件元数据
                    meta, err = get_file_metadata(tenant_access_token, cloud_file_token, cloud_file_type)
                    if err:
                        print(f"WARNING: 获取文件 {file_name} 元数据失败，将覆盖上传: {err}")
                        # 继续执行覆盖逻辑
                    else:
                        cloud_modified_time = int(meta.get("latest_modify_time", "0"))
                        
                        # 比较修改时间，如果本地文件更新，则需要更新
                        if local_modified_time <= cloud_modified_time:
                            print(f"文件 {file_name} 无需更新，跳过")
                            continue
                    
                    # 需要更新，先重命名旧文件
                    file_modify_time = datetime.fromtimestamp(cloud_modified_time)
                    modify_date = file_modify_time.strftime("%Y%m%d")
                    new_name = f"{os.path.splitext(file_name)[0]}_{modify_date}{os.path.splitext(file_name)[1]}"
                    
                    # 检查重命名后是否仍存在冲突
                    counter = 1
                    temp_name = new_name
                    while temp_name in cloud_file_map:
                        temp_name = f"{os.path.splitext(file_name)[0]}_{modify_date}_{counter}{os.path.splitext(file_name)[1]}"
                        counter += 1
                    new_name = temp_name
                    
                    print(f"重命名旧文件: {file_name} -> {new_name}")
                    _, err = update_wiki_node_title( tenant_access_token, space_id, node_token, new_name)
                    if err:
                        print(f"ERROR: 重命名文件 {file_name} 失败: {err}", file=sys.stderr)
                        continue
                
                _, err = upload_local_to_wiki(tenant_access_token,space_id,local_item_path,file_name,parent_node_token)
                if err:
                    print(f"WARNING: 创建导入任务失败: {err}")
                else:
                    print(f"文件上传完成: {file_name}")
        
        return None
        
    except Exception as e:
        print(f"ERROR: 同步过程中发生错误: {e}", file=sys.stderr)
        return e

if __name__ == "__main__":
    config_path = runtime_base_dir() / "uploader_config.yaml"
    if not config_path.exists():
        _fatal(f"ERROR: 未找到配置文件 {config_path}，请在该目录创建 uploader_config.yaml 并填写 app_id、app_secret、space_id、cloud_root_folder_token 等。")
    if not app_id:
        _fatal("ERROR: uploader_config.yaml 中缺少 app_id 或为空。")
    if not app_secret:
        _fatal("ERROR: uploader_config.yaml 中缺少 app_secret 或为空。")
    if not space_id:
        _fatal("ERROR: uploader_config.yaml 中缺少 space_id 或为空。")
    if not cloud_root_folder_token:
        _fatal("ERROR: uploader_config.yaml 中缺少 cloud_root_folder_token 或为空。")
    if not local_root_path:
        _fatal("ERROR: 本地根路径未配置（local_root_path 为空）。")
    if not os.path.exists(local_root_path):
        try:
            os.makedirs(local_root_path, exist_ok=True)
            print(f"INFO: 本地路径不存在，已自动创建: {local_root_path}")
        except Exception as exc:
            _fatal(f"ERROR: 本地路径不存在且创建失败: {local_root_path}; {exc}")
    
    # 获取 tenant_access_token
    tenant_access_token, err = get_tenant_access_token(app_id, app_secret)
    if err:
        _fatal(f"ERROR: 获取 tenant_access_token 失败: {err}")
    
    # 开始同步
    err = sync_local_to_wiki(tenant_access_token, local_root_path, space_id or "")
    if err:
        _fatal(f"ERROR: 同步过程中发生错误: {err}")
    
    print("同步完成!")