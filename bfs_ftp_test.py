import os
import ftplib
import time
from collections import deque
import shutil
import threading
from queue import Queue

source_path = 'test_data'

def get_path():
    path_list = []
    q = deque()
    q.appendleft(source_path)
    while q:
        current = q.pop()
        # 패스 순회
        for path in os.listdir(current):
            full_path = os.path.join(current, path)
            if os.path.isdir(full_path):
                q.appendleft(full_path)
            elif os.path.isfile(full_path):
                path_list.append(full_path)
    return path_list

path_list = get_path()
# print(path_list)


count = 0
lock = threading.Lock()
def write_to_target(path_list,target = './target'):
    count_no_thread = 0
    for path in path_list:
        dirname = os.path.dirname(path)
        # if not os.path.exists(os.path.join(target,dirname)):
        os.makedirs(os.path.join(target,dirname),exist_ok=True)

        if not os.path.exists(os.path.join(target, path)): # 있으면 다음걸로
            # with open(os.path.join(target, path), 'wb') as f:
            # # 여기에 ftp 다운로드 코드 
            #     f.write(b'dummy test')
            shutil.copyfile(path, os.path.join(target, path))
            count_no_thread += 1

            with lock:
                count += 1
            # print(f'{path} copy done')
            
    return count_no_thread

start = time.time()
count_no_thread = write_to_target(path_list)
print(f'execution time : {time.time() - start}')
print(f'count : {count_no_thread}')

#using Thread
threads = []
num_threads = 10
start = time.time()

for i in range(num_threads):
    th = threading.Thread(target = write_to_target, kwargs = {'path_list' : path_list,'target':'./target_threading'})
    th.start()
    threads.append(th)

for th in threads:
    th.join()
print(f'execution time with threading: {time.time() - start}')
print(f'count : {count}')


# def get_filepath_from_ftp(host, user, password):
#     ftp = ftplib.FTP(host)
#     print(ftp.login(user, password))
#     file_list = []
#     q = deque()
#     q.appendleft(source_path)
#     while q:
#         current = q.pop()
#         try: 
#             path_list = ftp.nlst()
#         except ftplib.error_perm as msg:
#             continue
#         #패스 순회
#         for path in path_list:
#             full_path = os.path.join(current, path)
#             try:
#                 #폴더인지 확인
#                 ftp.cwd(full_path) # check dir or not
#                 #폴더면
#                 q.appendleft(full_path)
#             except ftplib.error_perm as msg:
#                 #if is file
#                 file_list.append(full_path)
#     ftp.quit()
    
#     return file_list
                
                