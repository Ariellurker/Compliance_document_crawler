import os
import requests
import sys
import time
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
from pathlib import Path

# # === input params start
# app_id = os.getenv("APP_ID")               # app_id, required, 应用 ID
# # 应用唯一标识，创建应用后获得。有关app_id 的详细介绍。请参考通用参数https://open.feishu.cn/document/ukTMukTMukTM/uYTM5UjL2ETO14iNxkTN/terminology。
# app_secret = os.getenv("APP_SECRET")       # app_secret, required, 应用 secret
# # 应用秘钥，创建应用后获得。有关 app_secret 的详细介绍，请参考https://open.feishu.cn/document/ukTMukTMukTM/uYTM5UjL2ETO14iNxkTN/terminology。
# local_root_path = os.getenv("LOCAL_ROOT_PATH")  # string, required, 本地文件夹路径
# # 本地需要同步到飞书云文档的根文件夹路径
# cloud_root_folder_token = os.getenv("CLOUD_ROOT_FOLDER_TOKEN")  # string, optional, 云端目标文件夹 token
# # 云端目标文件夹的 token，如果不提供则使用根目录。了解如何获取文件夹 token，参考[文件夹概述](https://go.feishu.cn/s/6pmWNBA4404)。
# # === input params end

app_id = "cli_a9094ea2e4391cda"
app_secret = "rKGveJt76bNEkWPXGEy2EfkxCSKHoQ02"

root = Path(sys.executable).parent
file_name = input('请输入文件夹名称:')
local_root_path = str(root/file_name)
# local_root_path = './dist/上传文件夹'

cloud_root_folder_token = "TwSofqIwmlv6yWdR7JLc9tsPnld"

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

def create_folder(tenant_access_token: str, name: str, parent_folder_token: str = "") -> Tuple[str, Exception]:
    """创建文件夹

    Args:
        tenant_access_token: 租户访问令牌
        name: 文件夹名称
        parent_folder_token: 父文件夹token

    Returns:
        Tuple[str, Exception]: (文件夹token, 错误)
    """
    url = "https://open.feishu.cn/open-apis/drive/v1/files/create_folder"
    payload = {
        "name": name,
        "folder_token": parent_folder_token
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
            return "", Exception(f"failed to create folder: {result.get('msg', 'unknown error')}")
        
        return result.get("data", {}).get("token", ""), None
        
    except Exception as e:
        print(f"ERROR: creating folder: {e}", file=sys.stderr)
        return "", e

def copy_file(tenant_access_token: str, file_token: str, new_name: str, target_folder_token: str, file_type: str) -> Tuple[str, Exception]:
    """复制文件并指定新名称（用于重命名）"""
    url = f"https://open.feishu.cn/open-apis/drive/v1/files/{file_token}/copy"
    payload = {
        "name": new_name,  # 新文件名
        "type": file_type,
        "folder_token": target_folder_token  # 与原文件夹相同
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
            return "", Exception(f"复制失败: {result.get('msg')}")
        
        new_file_token = result["data"]["file"]["token"]
        return new_file_token, None
    except Exception as e:
        return "", e

def delete_file(tenant_access_token: str, file_token: str, file_type: str) -> Exception:
    """删除文件"""
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
        return e
    
def rename_file(tenant_access_token: str, file_token: str, old_name: str, new_name: str, folder_token: str, file_type: str) -> Tuple[str, Exception]:
    """重命名文件（复制+删除）"""
    # 步骤1：复制文件到同一文件夹，指定新名称
    new_file_token, err = copy_file(tenant_access_token, file_token, new_name, folder_token, file_type)
    if err:
        return "", err
    
    # 步骤2：删除原文件
    err = delete_file(tenant_access_token, file_token, file_type)
    if err:
        # 若删除失败，需清理已复制的新文件
        delete_file(tenant_access_token, new_file_token, file_type)
        return "", err
    
    return new_file_token, None

def move_file_or_folder(tenant_access_token: str, file_token: str, target_folder_token: str, file_type: str = "file") -> Tuple[str, Exception]:
    """移动文件或文件夹

    Args:
        tenant_access_token: 租户访问令牌
        file_token: 文件或文件夹token
        new_name: 新名称
        target_folder_token: 目标文件夹token
        file_type: 文件类型

    Returns:
        Tuple[str, Exception]: (任务ID, 错误)
    """
    url = f"https://open.feishu.cn/open-apis/drive/v1/files/{file_token}/move"
    payload = {
        "type": file_type,
        "folder_token": target_folder_token,
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
            return "", Exception(f"failed to move file: {result.get('msg', 'unknown error')}")
        
        task_id = result.get("data", {}).get("task_id", "")
        return task_id, None
        
    except Exception as e:
        print(f"ERROR: moving/renaming file: {e}", file=sys.stderr)
        return "", e

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

def create_import_task(tenant_access_token: str, file_token: str, file_extension: str, target_type: str, 
                      file_name: str, mount_folder_token: str) -> Tuple[str, Exception]:
    """创建导入任务（用于将文件转换为在线文档）

    Args:
        tenant_access_token: 租户访问令牌
        file_token: 源文件token
        file_extension: 文件扩展名
        target_type: 目标文档类型
        file_name: 导入后的文档名称
        mount_folder_token: 挂载文件夹token

    Returns:
        Tuple[str, Exception]: (任务ticket, 错误)
    """
    url = "https://open.feishu.cn/open-apis/drive/v1/import_tasks"
    payload = {
        "file_extension": file_extension,
        "file_token": file_token,
        "type": target_type,
        "file_name": file_name,
        "point": {
            "mount_type": 1,
            "mount_key": mount_folder_token
        }
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
            return "", Exception(f"failed to create import task: {result.get('msg', 'unknown error')}")
        
        return result.get("data", {}).get("ticket", ""), None
        
    except Exception as e:
        print(f"ERROR: creating import task: {e}", file=sys.stderr)
        return "", e

def query_import_result(tenant_access_token: str, ticket: str) -> Tuple[Dict[str, Any], Exception]:
    """查询导入结果

    Args:
        tenant_access_token: 租户访问令牌
        ticket: 导入任务ID

    Returns:
        Tuple[Dict, Exception]: (导入结果, 错误)
    """
    url = f"https://open.feishu.cn/open-apis/drive/v1/import_tasks/{ticket}"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    
    try:
        # print(f"GET: {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        result = response.json()
        # print(f"Response: {json.dumps(result)}")
        
        if result.get("code", 0) != 0:
            return {}, Exception(f"failed to query import result: {result.get('msg', 'unknown error')}")
        
        return result.get("data", {}).get("result", {}), None
        
    except Exception as e:
        print(f"ERROR: querying import result: {e}", file=sys.stderr)
        return {}, e

def should_import_file(file_name: str) -> Tuple[bool, str, str]:
    """判断文件是否需要导入为在线文档

    Args:
        file_name: 文件名

    Returns:
        Tuple[bool, str, str]: (是否需要导入, 扩展名, 目标类型)
    """
    ext = file_name.split('.')[-1].lower() if '.' in file_name else ""
    
    import_map = {
        "doc": ("docx", "docx"),
        "docx": ("docx", "docx"),
        "txt": ("txt", "docx"),
        "md": ("md", "docx"),
        "markdown": ("markdown", "docx"),
        "xls": ("xls", "sheet"),
        "xlsx": ("xlsx", "sheet"),
        "csv": ("csv", "sheet")
    }
    
    if ext in import_map:
        return True, import_map[ext][0], import_map[ext][1]
    
    return False, ext, ""

def sync_local_to_cloud(tenant_access_token: str, local_path: str, cloud_folder_token: str = "") -> Exception:
    """同步本地文件夹到飞书云文档

    Args:
        tenant_access_token: 租户访问令牌
        local_path: 本地文件夹路径
        cloud_folder_token: 云端文件夹token

    Returns:
        Exception: 错误信息
    """
    try:
        print(f"开始同步本地文件夹: {local_path} 到云端文件夹: {cloud_folder_token or '根目录'}")
        
        # 获取云端文件夹内容
        cloud_files, err = get_folder_contents(tenant_access_token, cloud_folder_token)
        if err:
            return err
        
        # 创建文件名到文件信息的映射
        cloud_file_map = {f["name"]: f for f in cloud_files}
        
        # 遍历本地文件夹
        for item in os.listdir(local_path):
            local_item_path = os.path.join(local_path, item)
            
            if os.path.isdir(local_item_path):
                # 处理文件夹
                if item in cloud_file_map and cloud_file_map[item]["type"] == "folder":
                    # 文件夹已存在，递归处理
                    print(f"文件夹已存在: {item}, 进入递归处理")
                    sub_folder_token = cloud_file_map[item]["token"]
                    err = sync_local_to_cloud(tenant_access_token, local_item_path, sub_folder_token)
                    if err:
                        print(f"ERROR: 同步子文件夹 {item} 失败: {err}", file=sys.stderr)
                        continue
                else:
                    # 文件夹不存在，创建新文件夹
                    print(f"创建新文件夹: {item}")
                    new_folder_token, err = create_folder(tenant_access_token, item, cloud_folder_token)
                    if err:
                        print(f"ERROR: 创建文件夹 {item} 失败: {err}", file=sys.stderr)
                        continue
                    
                    # 递归处理新创建的文件夹
                    err = sync_local_to_cloud(tenant_access_token, local_item_path, new_folder_token)
                    if err:
                        print(f"ERROR: 同步新文件夹 {item} 失败: {err}", file=sys.stderr)
                        continue
                        
            elif os.path.isfile(local_item_path):
                # 处理文件
                file_name = item
                file_size = os.path.getsize(local_item_path)
                local_modified_time = int(os.path.getmtime(local_item_path))
                
                if file_name in cloud_file_map:
                    # 文件已存在，检查是否需要更新
                    cloud_file = cloud_file_map[file_name]
                    cloud_file_token = cloud_file["token"]
                    cloud_file_type = cloud_file["type"]
                    
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
                    _, err = rename_file(
                        tenant_access_token, 
                        cloud_file_token, 
                        file_name, 
                        new_name, 
                        cloud_folder_token, 
                        cloud_file_type
                    )
                    if err:
                        print(f"ERROR: 重命名文件 {file_name} 失败: {err}", file=sys.stderr)
                        continue
                
                # 上传新文件
                print(f"上传文件: {file_name}")
                if file_size <= 20 * 1024 * 1024:  # 20MB
                    file_token, err = upload_file(tenant_access_token, local_item_path, file_name, cloud_folder_token)
                else:
                    file_token, err = upload_large_file(tenant_access_token, local_item_path, file_name, cloud_folder_token)
                
                if err:
                    print(f"ERROR: 上传文件 {file_name} 失败: {err}", file=sys.stderr)
                    continue
                
                # 检查是否需要导入为在线文档
                need_import, extension, target_type = should_import_file(file_name)
                if need_import:
                    print(f"创建导入任务: {file_name} -> {target_type}")
                    ticket, err = create_import_task(
                        tenant_access_token,
                        file_token,
                        extension,
                        target_type,
                        os.path.splitext(file_name)[0],  # 使用不带扩展名的文件名
                        cloud_folder_token
                    )
                    if err:
                        print(f"WARNING: 创建导入任务失败: {err}")
                    else:
                        # 轮询导入结果
                        max_retries = 30
                        retry_count = 0
                        while retry_count < max_retries:
                            time.sleep(2)  # 等待2秒
                            result, err = query_import_result(tenant_access_token, ticket)
                            if err:
                                print(f"WARNING: 查询导入结果失败: {err}")
                                break
                            
                            job_status = result.get("job_status", 1)  # 默认为处理中
                            if job_status == 0:  # 成功
                                print(f"导入成功: {file_name} -> {result.get('url', '')}")
                                break
                            elif job_status in [3, 100, 101, 102, 103, 104, 105, 106, 108, 109, 110, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 5000, 7000, 7001, 7002]:
                                print(f"ERROR: 导入失败: {result.get('job_error_msg', '未知错误')}", file=sys.stderr)
                                break
                            else:
                                print(f"导入中... ({retry_count + 1}/{max_retries})")
                                retry_count += 1
                        
                        if retry_count >= max_retries:
                            print(f"WARNING: 导入超时: {file_name}")
                
                print(f"文件上传完成: {file_name}")
        
        print(f"同步完成: {local_path}")
        return None
        
    except Exception as e:
        print(f"ERROR: 同步过程中发生错误: {e}", file=sys.stderr)
        return e

if __name__ == "__main__":
    # 参数验证
    if not app_id:
        print("ERROR: APP_ID 环境变量未设置", file=sys.stderr)
        exit(1)
    if not app_secret:
        print("ERROR: APP_SECRET 环境变量未设置", file=sys.stderr)
        exit(1)
    if not local_root_path:
        print("ERROR: LOCAL_ROOT_PATH 环境变量未设置", file=sys.stderr)
        exit(1)
    if not os.path.exists(local_root_path):
        print(f"ERROR: Local path does not exist: {local_root_path}", file=sys.stderr)
        exit(1)
    
    # 获取 tenant_access_token
    tenant_access_token, err = get_tenant_access_token(app_id, app_secret)
    if err:
        print(f"ERROR: 获取 tenant_access_token 失败: {err}", file=sys.stderr)
        exit(1)
    
    # 开始同步
    err = sync_local_to_cloud(tenant_access_token, local_root_path, cloud_root_folder_token or "")
    if err:
        print(f"ERROR: 同步过程中发生错误: {err}", file=sys.stderr)
        exit(1)
    
    print("同步完成!")