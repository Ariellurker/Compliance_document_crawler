import os
import requests
import sys
import time
from typing import Dict, Any, List, Tuple, Optional
from urllib.parse import quote
from pathlib import Path

# # === input params start
# app_id = os.getenv("APP_ID")               # app_id, required, 应用 ID
# # 应用唯一标识，创建应用后获得。有关app_id 的详细介绍。请参考通用参数https://open.feishu.cn/document/ukTMukTMukTM/uYTM5UjL2ETO14iNxkTN/terminology。
# app_secret = os.getenv("APP_SECRET")       # app_secret, required, 应用 secret
# # 应用秘钥，创建应用后获得。有关 app_secret 的详细介绍，请参考https://open.feishu.cn/document/ukTMukTMukTM/uYTM5UjL2ETO14iNxkTN/terminology。
# local_root_path = os.getenv("LOCAL_BASE_PATH")  # string, required, 本地同步基础路径
# # 云盘文件将同步到此本地目录下，需确保程序有写入权限。
# root_folder_token = os.getenv("ROOT_FOLDER_TOKEN", "")  # string, optional, 云盘根文件夹 token
# # 要同步的云盘文件夹 token，留空则同步个人云盘根目录。了解如何获取文件夹 token，参考[文件夹概述](https://go.feishu.cn/s/6pmWNBA4404)。
# # === input params end
app_id = "cli_a9094ea2e4391cda"
app_secret = "rKGveJt76bNEkWPXGEy2EfkxCSKHoQ02"

root = Path(sys.executable).parent
file_name = input('请输入文件夹名称:')
local_root_path = str(root/file_name)
# local_root_path = './dist/上传文件夹'

root_folder_token = "TwSofqIwmlv6yWdR7JLc9tsPnld"
space_id = "7603276829465709762"

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
        # print(f"Request body: {{'app_id': '***', 'app_secret': '***'}}")  # 隐藏敏感信息
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()

        result = response.json()
        # print(f"Response: {result}")

        if result.get("code", 0) != 0:
            print(f"ERROR: failed to get tenant_access_token: {result.get('msg', 'unknown error')}", file=sys.stderr)
            return "", Exception(f"failed to get tenant_access_token: {response.text}")

        return result["tenant_access_token"], None

    except Exception as e:
        print(f"ERROR: getting tenant_access_token: {e}", file=sys.stderr)
        if hasattr(e, 'response') and e.response is not None:
            print(f"ERROR: Response body: {e.response.text}", file=sys.stderr)
        return "", e

def list_folder_files(tenant_access_token: str, folder_token: str = "") -> List[Dict[str, Any]]:
    """获取文件夹中的文件清单

    Args:
        tenant_access_token: 租户访问令牌
        folder_token: 文件夹token，空字符串表示根目录

    Returns:
        List[Dict[str, Any]]: 文件清单列表
    """
    all_files = []
    page_token = ""
    
    while True:
        url = f"https://open.feishu.cn/open-apis/drive/v1/files?folder_token={quote(folder_token)}&page_size=100"
        if page_token:
            url += f"&page_token={quote(page_token)}"
        
        headers = {
            "Authorization": f"Bearer {tenant_access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        try:
            # print(f"GET: {url}")
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            result = response.json()
            # print(f"Response code: {result.get('code', 0)}")
            
            if result.get("code", 0) != 0:
                print(f"ERROR: 获取文件夹内容失败: {result.get('msg', 'unknown error')}", file=sys.stderr)
                break
            
            data = result.get("data", {})
            files = data.get("files", [])
            all_files.extend(files)
            
            has_more = data.get("has_more", False)
            if not has_more:
                break
            page_token = data.get("next_page_token", "")
            if not page_token:
                break
                
        except Exception as e:
            print(f"ERROR: 获取文件夹内容时发生错误: {e}", file=sys.stderr)
            break
    
    return all_files

def get_wiki_space_list(tenant_access_token: str) -> Tuple[List[Dict], Exception]:
    """获取知识空间列表"""
    url = "https://open.feishu.cn/open-apis/wiki/v2/spaces"
    headers = {"Authorization": f"Bearer {tenant_access_token}"}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        if result["code"] != 0:
            return [], Exception(f"获取知识空间失败: {result['msg']}")
        return result["data"]["spaces"], None
    except Exception as e:
        return [], e

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
        return result["data"]["nodes"], None
    except Exception as e:
        return [], e

def get_wiki_node_info(tenant_access_token: str, space_id: str, node_token: str) -> Tuple[Dict, Exception]:
    """获取知识空间节点信息（含文档 token）"""
    url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/{space_id}/nodes/{node_token}"
    headers = {"Authorization": f"Bearer {tenant_access_token}"}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        if result["code"] != 0:
            return {}, Exception(f"获取节点信息失败: {result['msg']}")
        return result["data"]["node"], None
    except Exception as e:
        return {}, e

def download_file(tenant_access_token: str, file_token: str, local_path: str) -> bool:
    """下载文件到本地

    Args:
        tenant_access_token: 租户访问令牌
        file_token: 文件token
        local_path: 本地保存路径

    Returns:
        bool: 是否成功
    """
    url = f"https://open.feishu.cn/open-apis/drive/v1/files/{quote(file_token)}/download"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}"
    }
    
    try:
        # print(f"GET: {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # 确保目录存在
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # 写入文件
        with open(local_path, 'wb') as f:
            f.write(response.content)
        
        print(f"文件下载成功: {local_path}")
        return True
        
    except Exception as e:
        print(f"ERROR: 下载文件失败 {file_token}: {e}", file=sys.stderr)
        return False

def export_online_document(tenant_access_token: str, file_token: str, file_type: str) -> Tuple[str, Exception]:
    """导出在线文档为可下载格式
    
    Args:
        tenant_access_token: 租户访问令牌
        file_token: 在线文档的 token
        file_type: 文档类型（doc, docx, sheet, bitable 等）
    
    Returns:
        Tuple[str, Exception]: (导出文件的 file_token, 错误信息)
    """
    # 1. 定义导出格式（根据文档类型选择）
    ext_map = {
        "doc": "docx",    # 旧版文档 → DOCX
        "docx": "docx",   # 新版文档 → DOCX
        "sheet": "xlsx",  # 电子表格 → XLSX
        "bitable": "xlsx",# 多维表格 → XLSX
        "slides": "pdf"   # 幻灯片 → PDF
    }
    file_extension = ext_map.get(file_type, "pdf")
    
    # 2. 创建导出任务
    url = "https://open.feishu.cn/open-apis/drive/v1/export_tasks"
    payload = {
        "token": file_token,
        "type": file_type,
        "file_extension": file_extension
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
            return "", Exception(f"导出任务创建失败: {result.get('msg')}")
        
        ticket = result["data"]["ticket"]
        print(f"导出任务创建成功，ticket: {ticket}")
        
        # 3. 轮询查询导出结果（导出为异步任务，需等待完成）
        for _ in range(30):  # 最多等待 30 秒
            time.sleep(1)
            export_result, err = get_export_result(tenant_access_token, ticket, file_token)
            if err:
                return "", err
            if export_result["job_status"] == 0:  # 0 表示成功
                return export_result["file_token"], None
            elif export_result["job_status"] in [3, 107, 108, 110]:  # 失败状态码
                return "", Exception(f"导出失败: {export_result.get('job_error_msg', '未知错误')}")
        
        return "", Exception("导出任务超时")
        
    except Exception as e:
        return "", e

def get_export_result(tenant_access_token: str, ticket: str, file_token: str) -> Tuple[Dict, Exception]:
    """查询导出任务结果
    
    Args:
        tenant_access_token: 租户访问令牌
        ticket: 导出任务 ID
        file_token: 文件 Token
    
    Returns:
        Tuple[Dict, Exception]: (导出结果, 错误信息)
    """
    url = f"https://open.feishu.cn/open-apis/drive/v1/export_tasks/{ticket}?token={quote(file_token)}"
    headers = {"Authorization": f"Bearer {tenant_access_token}"}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        
        if result.get("code") != 0:
            return {}, Exception(f"查询导出结果失败: {result.get('msg')}")
        
        return result["data"]["result"], None
    except Exception as e:
        return {}, e

def download_exported_file(tenant_access_token: str, export_file_token: str, local_path: str) -> bool:
    """下载导出后的文件
    
    Args:
        tenant_access_token: 租户访问令牌
        export_file_token: 导出文件的 token
        local_path: 本地保存路径
    
    Returns:
        bool: 是否成功
    """
    url = f"https://open.feishu.cn/open-apis/drive/v1/export_tasks/file/{export_file_token}/download"
    headers = {"Authorization": f"Bearer {tenant_access_token}"}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, 'wb') as f:
            f.write(response.content)
        
        print(f"导出文件下载成功: {local_path}")
        return True
    except Exception as e:
        print(f"ERROR: 下载导出文件失败: {e}", file=sys.stderr)
        return False
    
def format_timestamp(timestamp_str: str) -> str:
    """格式化时间戳为 YYYYMMDD_HHMMSS 格式

    Args:
        timestamp_str: 秒级时间戳字符串

    Returns:
        str: 格式化后的时间字符串
    """
    try:
        timestamp = int(timestamp_str)
        time_struct = time.localtime(timestamp)
        return time.strftime("%Y%m%d_%H%M%S", time_struct)
    except:
        # 如果转换失败，返回当前时间
        return time.strftime("%Y%m%d_%H%M%S")

def get_unique_filename(base_path: str, filename: str, modified_time: str) -> str:
    """获取不重复的文件名

    Args:
        base_path: 文件所在目录路径
        filename: 原始文件名
        modified_time: 文件修改时间戳

    Returns:
        str: 唯一的文件名
    """
    if not os.path.exists(os.path.join(base_path, filename)):
        return filename
    
    # 分离文件名和扩展名
    name, ext = os.path.splitext(filename)
    formatted_time = format_timestamp(modified_time)
    new_name = f"{name}_{formatted_time}{ext}"
    counter = 1
    
    # 检查新文件名是否仍存在，如果存在则添加序号
    while os.path.exists(os.path.join(base_path, new_name)):
        new_name = f"{name}_{formatted_time}_{counter}{ext}"
        counter += 1
    
    return new_name

def sync_cloud_folder(tenant_access_token: str, folder_token: str, local_path: str, folder_name: str = ""):
    """递归同步云盘文件夹到本地

    Args:
        tenant_access_token: 租户访问令牌
        folder_token: 文件夹token
        local_path: 本地路径
        folder_name: 文件夹名称（用于显示）
    """
    print(f"开始同步文件夹: {folder_name or '根目录'} -> {local_path}")
    
    # 创建本地目录
    os.makedirs(local_path, exist_ok=True)
    
    # 获取文件夹内容
    files = list_folder_files(tenant_access_token, folder_token)
    
    for file_info in files:
        file_name = file_info.get("name", "")
        file_type = file_info.get("type", "")
        file_token = file_info.get("token", "")
        modified_time = file_info.get("modified_time", "0")
        
        if not file_name or not file_token:
            continue
            
        if file_type == "folder":
            # 递归处理子文件夹
            sub_folder_path = os.path.join(local_path, file_name)
            sync_cloud_folder(tenant_access_token, file_token, sub_folder_path, file_name)
        elif file_type == "file":
            # 处理普通文件
            local_file_path = os.path.join(local_path, file_name)
            
            # 检查本地是否已存在同名文件
            if os.path.exists(local_file_path):
                # 获取本地文件修改时间
                local_modified_time = str(int(os.path.getmtime(local_file_path)))
                
                # 如果云文件更新，则重命名本地文件
                if modified_time > local_modified_time:
                    unique_name = get_unique_filename(local_path, file_name, modified_time)
                    if unique_name != file_name:
                        old_path = local_file_path
                        new_path = os.path.join(local_path, unique_name)
                        os.rename(old_path, new_path)
                        print(f"重命名本地文件: {file_name} -> {unique_name}")
                else:
                    continue
            
            # 下载文件
            final_local_path = os.path.join(local_path, file_name)
            if download_file(tenant_access_token, file_token, final_local_path):
                # 设置文件修改时间为云文件的修改时间
                try:
                    os.utime(final_local_path, (int(modified_time), int(modified_time)))
                except Exception as e:
                    print(f"WARNING: 设置文件修改时间失败 {final_local_path}: {e}")
        elif file_type in ["doc", "docx", "sheet", "bitable", "slides"]:
            # 处理在线文档（需导出后下载）
            # print(f"处理在线文档: {file_name} (类型: {file_type})")
            
            # 生成导出后的文件名（添加导出格式后缀）
            export_ext_map = {
                "doc": ".docx", "docx": ".docx", "sheet": ".xlsx", 
                "bitable": ".xlsx", "slides": ".pdf"
            }
            export_ext = export_ext_map[file_type]
            name_without_ext = os.path.splitext(file_name)[0]
            export_file_name = f"{name_without_ext}{export_ext}"
            local_file_path = os.path.join(local_path, export_file_name)
            
            # 检查本地文件是否已存在且最新
            if os.path.exists(local_file_path):
                local_modified_time = str(int(os.path.getmtime(local_file_path)))
                if modified_time > local_modified_time:
                    unique_name = get_unique_filename(local_path, file_name+export_ext, modified_time)
                    if unique_name != file_name:
                        old_path = local_file_path
                        new_path = os.path.join(local_path, unique_name)
                        os.rename(old_path, new_path)
                        print(f"重命名本地文件: {file_name} -> {unique_name}")
                else:
                    continue
            
            # 导出并下载
            export_file_token, err = export_online_document(tenant_access_token, file_token, file_type)
            if err:
                print(f"ERROR: 导出在线文档失败 {file_name}: {err}", file=sys.stderr)
                continue
            
            # 下载导出文件
            if download_exported_file(tenant_access_token, export_file_token, local_file_path):
                # 设置文件修改时间
                os.utime(local_file_path, (int(modified_time), int(modified_time)))
        else:
            print(f"跳过文档: {file_name} (类型: {file_type})")

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
    
    # 获取 tenant_access_token
    tenant_access_token, err = get_tenant_access_token(app_id, app_secret)
    if err:
        print(f"ERROR: 获取 tenant_access_token 失败: {err}", file=sys.stderr)
        exit(1)
    
    print("开始同步飞书云盘资产到本地...")
    
    try:
        # 开始同步
        sync_cloud_folder(tenant_access_token, root_folder_token, local_root_path)
        print("同步完成!")
    except Exception as e:
        print(f"ERROR: 同步过程中发生错误: {e}", file=sys.stderr)
        exit(1)
    