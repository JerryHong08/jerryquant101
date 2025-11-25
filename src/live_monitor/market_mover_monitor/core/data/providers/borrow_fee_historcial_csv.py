"""
Get the historical Borrow Fee data from chartexchange.com.
Directly access the download url and save csv.
Slow but can be asynced for subsequent analysis.
"""

import json
import os
import re
from io import StringIO
from urllib.parse import urljoin

import pandas as pd
import requests


def find_correct_cx_table(html):
    """
    找到包含download_data的cx_table实例
    """
    # 查找所有cx_table实例
    cx_table_pattern = r"new cx_table\(\s*({.*?})\s*\)"
    matches = list(re.finditer(cx_table_pattern, html, re.DOTALL))

    print(f"找到 {len(matches)} 个cx_table实例")

    for i, match in enumerate(matches):
        try:
            table_config = json.loads(match.group(1))

            # 检查是否包含download_data
            if "download_data" in table_config:
                print(f"实例 {i+1} 包含download_data")
                return match.group(1)
            else:
                print(f"实例 {i+1} 不包含download_data")

        except json.JSONDecodeError as e:
            print(f"实例 {i+1} JSON解析失败: {e}")

    return None


def extract_download_params_from_html(url):
    """
    从HTML中提取下载参数
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        html = response.text

        # 找到正确的cx_table实例
        correct_table_json = find_correct_cx_table(html)
        if not correct_table_json:
            print("未找到包含download_data的cx_table实例")
            return None

        # 解析JSON
        table_config = json.loads(correct_table_json)
        download_data = table_config.get("download_data", {})

        if not download_data:
            print("找到的cx_table实例不包含download_data")
            return None

        download_url = download_data.get("url", "")
        params = download_data.get("params", {})

        print(f"找到下载参数: {params}")

        return {"base_url": download_url, "params": params}

    except Exception as e:
        print(f"提取下载参数失败: {e}")
        return None


def download_borrow_fee_csv_fixed(url, save_path="borrow_fee_data.csv"):
    """
    修复版的借股费率CSV下载
    """
    # 提取下载参数
    download_info = extract_download_params_from_html(url)
    if not download_info:
        return None

    # 构建下载URL
    base_url = download_info["base_url"]
    params = download_info["params"]

    # 如果base_url是相对路径，转换为绝对路径
    if not base_url.startswith("http"):
        base_url = urljoin(url, base_url)

    # 添加参数到URL
    download_url = base_url + "?" + "&".join([f"{k}={v}" for k, v in params.items()])

    print(f"构建的下载URL: {download_url}")

    # 下载CSV文件
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/csv,application/csv,*/*",
        "Referer": url,
    }

    try:
        response = requests.get(download_url, headers=headers, timeout=30)
        response.raise_for_status()

        # 检查响应内容
        content_type = response.headers.get("content-type", "").lower()
        print(f"响应内容类型: {content_type}")

        # 即使内容类型是application/octet-stream，也可能是CSV文件
        # 尝试直接解析为CSV
        csv_content = response.text

        # 检查内容是否为空
        if not csv_content.strip():
            print("响应内容为空")
            return None

        # 检查内容是否为CSV格式（包含逗号分隔的值）
        if "," not in csv_content.split("\n")[0]:
            print("响应内容可能不是CSV格式")
            print(f"内容前100字符: {csv_content[:100]}")
            return None

        # 保存文件
        if save_path:
            # 确保目录存在
            os.makedirs(
                os.path.dirname(save_path) if os.path.dirname(save_path) else ".",
                exist_ok=True,
            )
            with open(save_path, "w", encoding="utf-8") as f:
                f.write(csv_content)
            print(f"CSV文件已保存到: {save_path}")

        # 返回DataFrame
        df = pd.read_csv(StringIO(csv_content))
        return df

    except Exception as e:
        print(f"下载CSV文件失败: {e}")
        # 打印响应内容的前200字符以便调试
        if "response" in locals():
            print(f"响应内容前200字符: {response.text[:200]}")
        return None


# 使用修复版下载方法
url = "https://chartexchange.com/symbol/nasdaq-alzn/borrow-fee/"
df = download_borrow_fee_csv_fixed(url, "borrow_fee_data.csv")

if df is not None:
    print(f"成功下载 {len(df)} 行数据")
    print(df.head())
else:
    print("下载失败")
