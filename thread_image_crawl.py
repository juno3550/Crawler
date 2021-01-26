import requests
import re
import os
import threading
import queue  # Python的queue模块中提供了同步的、线程安全的队列类
from bs4 import BeautifulSoup
import traceback


# 队列，用来存放图片下载页url和翻页url
queue = queue.Queue()
# 种子url
result_urls = []
# 图片命名的起始编号
image_no = 0
# 图片存储路径
image_dir = "e:\\crawl_image"
if not os.path.exists(image_dir):
    os.makedirs(image_dir)


# 将图片下载到本地
def download_image(url, image_dir, image_no):
    try:
        # 访问图片并设置超时时间
        r = requests.get(url, timeout=60)
        # 获取图片的后缀名
        image_ext = url.split(".")[-1]
        # 设置下载路径与图片名称
        image_name = str(image_no) + "." + image_ext
        image_path = os.path.join(image_dir, image_name)
        # 保存图片到本地
        with open(image_path, "wb") as f:
            f.write(r.content)
        print("图片【%s】下载成功，保存路径【%s】" % (url, image_path))
    except:
        print("图片下载失败【%s】" % url)
        traceback.print_exc()


# 获取图片下载url（绝对路径）
def get_image_url(url):
    # 由于网站中的图片url都是相对路径，因此需要在此函数中拼接图片的绝对路径
    # 获取网站首页链接
    try:
        home_page = re.match(r"http[s]?://\w+.\w+\.com", url).group()
        r = requests.get(url, timeout=60)
        r.encoding = "gbk"
        # 通过a标签获取其中的src下载路径
        # 通过BeautifulSoup解析网页内容
        soup = BeautifulSoup(r.text, "html.parser")
        image_a = soup.find_all("a", attrs={"id": "img"})  # 找出id属性值为img的a标签，即主图
        if image_a:
            # 获得图片的相对路径
            image_relative_url = re.search(r'src="(.+?)"', str(image_a[0])).group(1)
            # 拼接绝对路径
            image_abs_url = home_page + image_relative_url
            return image_abs_url
    except:
        print("获取图片下载url失败【%s】" % url)
        traceback.print_exc()


# 获取图片浏览页中的图片下载页url和翻页url
def get_page_url(url):
    try:
        home_page = re.match(r"http[s]?://\w+.\w+\.com", url).group()
        r = requests.get(url, timeout=60)
        r.encoding = "gbk"
        soup = BeautifulSoup(r.text, "html.parser")
        # 存储所有图片的a标签跳转url及翻页url
        image_page_urls = []
        image_a_lists = soup.find_all("a")
        for image_a in image_a_lists:
            # 获取a标签中的相对url
            relative_url = image_a["href"]
            # 根据url特征，只需要图片跳转页url和翻页url
            if relative_url.startswith("/tupian") and relative_url.endswith(".html") or "/index_" in relative_url:
                # 拼接绝对路径
                image_or_index_abs_url = home_page + relative_url
                image_page_urls.append(image_or_index_abs_url)
        return image_page_urls
    except:
        print("获取图片下载页url和翻页url失败【%s】" % url)
        traceback.print_exc()


# 任务函数
def task(queue):
    global result_urls
    global image_dir
    global image_no
    while not queue.empty():
        url = queue.get()
        try:
            # 如果该页为主图页，则下载图片
            image_download_url = get_image_url(url)
            if image_download_url and image_download_url not in result_urls:
                image_no += 1
                # 下载图片到本地
                download_image(image_download_url, image_dir, image_no)
                result_urls.append(image_download_url)
        except:
            traceback.print_exc()
        try:
            # 获取图片下载页url和翻页url，加入队列中
            image_page_urls = get_page_url(url)
            while image_page_urls:
                image_page_url = image_page_urls.pop()
                if image_page_url not in result_urls:
                    queue.put(image_page_url)
                    result_urls.append(image_page_url)
        except:
            traceback.print_exc()


if __name__ == "__main__":
    # 将种子页面放入队列中
    image_resource_url = "http://pic.netbian.com"
    queue.put(image_resource_url)
    # 开启100个线程，执行任务函数
    t_list = []
    for i in range(100):
        t = threading.Thread(target=task, args=(queue,))
        t_list.append(t)
        t.start()
    # 等待所有子线程结束后，主线程才结束
    for t in t_list:
        t.join()