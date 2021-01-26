import requests
import re
from threading import Thread, Lock
import queue
import pymysql
import traceback
import time


# 线程同步锁
lock = Lock()
queue = queue.Queue()
# 设定需要爬取的网页数
crawl_num = 20
# 存储已爬取的url
result_urls = []
# 当前已爬取的网页个数
current_url_count = 0
# 爬取的关键字
key_word = "新闻"


# mysql操作封装
class MysqlTool():

    def __init__(self, host, port, db, user, passwd, charset="utf8"):
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.passwd = passwd
        self.charset = charset

    def connect(self):
        '''创建数据库连接与执行对象'''
        try:
            self.conn = pymysql.connect(host=self.host, port=self.port, db=self.db, user=self.user, passwd=self.passwd, charset=self.charset)
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(e)

    def close(self):
        '''关闭数据库连接与执行对象'''
        try:
            self.cursor.close()
            self.conn.close()
        except Exception as e:
            print(e)

    def __edit(self, sql):
        '''增删改查的私有方法'''
        try:
            execute_count = self.cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            print(e)
        else:
            return execute_count

    def insert(self, sql):
        '''插入数据'''
        self.connect()
        self.__edit(sql)
        self.close()


# 获取URL的所需信息
def get_page_message(url):
    try:
        if ".pdf" in url or ".jpg" in url or ".jpeg" in url or ".png" in url or ".apk" in url or "microsoft" in url:
            return
        r = requests.get(url, timeout=5)
        # 获取该URL的源码内容
        page_content = r.text
        # 获取页面标题
        page_title = re.search(r"<title>(.+?)</title>", page_content).group(1)
        # 使用正则提取该URL中的所有URL
        page_url_list = re.findall(r'href="(http.+?)"', page_content)
        # 过滤图片、pdf等链接
        # 获取URL中的正文内容
        page_text = "".join(re.findall(r"<p>(.+?)</p>", page_content))
        # 将正文中的标签去掉
        page_text = "".join(re.split(r"<.+?>", page_text))
        # 将正文中的双引号改成单引号，以防落库失败
        page_text = page_text.replace('"', "'")
        return page_url_list, page_title, page_text
    except:
        print("获取URL所需信息失败【%s】" % url)
        traceback.print_exc()


# 任务函数：获取本url中的所有链接url
def get_html(queue, lock, mysql, key_word, crawl_num, threading_no):
    global current_url_count
    try:
        while not queue.empty():
            # 已爬取的数据量达到要求，则结束爬虫
            lock.acquire()
            if len(result_urls) >= crawl_num:
                print("【线程%d】爬取总数【%d】达到要求，任务函数结束" % (threading_no, len(result_urls)))
                lock.release()
                return
            else:
                lock.release()
            # 从队列中获取url
            url = queue.get()
            lock.acquire()
            current_url_count += 1
            lock.release()
            print("【线程%d】队列中还有【%d】个URL，当前爬取的是第【%d】个URL：%s" % (threading_no, queue.qsize(), current_url_count, url))
            # 判断url是否已爬取过，以防止重复落库
            if url not in result_urls:
                page_message = get_page_message(url)
                page_url_list = page_message[0]
                page_title = page_message[1]
                page_text = page_message[2]
                if not page_message:
                    continue
                # 将源码中的所有URL放到队列中
                while page_url_list:
                    url = page_url_list.pop()
                    lock.acquire()
                    if url not in result_urls:
                        queue.put(url.strip())
                    lock.release()
                # 标题或正文包含关键字，才会入库
                if key_word in page_title or key_word in page_text:
                    lock.acquire()
                    if not len(result_urls) >= crawl_num:
                        sql = 'insert into crawl_page(url, title, text) values("%s", "%s", "%s")' % (url, page_title, page_text)
                        mysql.insert(sql)
                        result_urls.append(url)
                        print("【线程%d】关键字【%s】，目前已爬取【%d】条数据，距离目标数据还差【%d】条，当前落库URL为【%s】" % (threading_no, key_word, len(result_urls), crawl_num-len(result_urls), url))
                        lock.release()
                    else:
                        # 已爬取的数据量达到要求，则结束爬虫
                        print("【线程%d】爬取总数【%d】达到要求，任务函数结束" % (threading_no, len(result_urls)))
                        lock.release()
                        return

        print("【线程%d】队列为空，任务函数结束" % threading_no)

    except:
        print("【线程%d】任务函数执行失败" % threading_no)
        traceback.print_exc()


if __name__ == "__main__":
    # 爬取的种子页
    home_page_url = "https://www.163.com"
    queue.put(home_page_url)
    mysql = MysqlTool("127.0.0.1", 3306, "test", "root", "admin")
    t_list = []
    for i in range(50):
        t = Thread(target=get_html, args=(queue, lock, mysql, key_word, crawl_num, i))
        time.sleep(0.05)
        t.start()
        t_list.append(t)
    for t in t_list:
        t.join()