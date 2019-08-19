#!/usr/bin/env python3
# encoding: utf-8

import sys
import os
import argparse
import logging
import hashlib
import json
import math
import mmap
import urllib.request
import time
import threading


LOG = logging.getLogger("pymultidown")
LOG.setLevel(logging.DEBUG)
LOG_HANDLER = logging.StreamHandler(sys.stderr)
LOG_HANDLER.setLevel(logging.INFO)
LOG_HANDLER.setFormatter(logging.Formatter("[%(asctime)s] %(name)s %(levelname)s - %(message)s"))
LOG.addHandler(LOG_HANDLER)


class Obj(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


class Multidown(object):
    class Error(Exception):
        __init__ = lambda self, msg: Exception.__init__(self, msg)

    def __init__(self, url, num_sections, timeout):
        self.__url = url
        self.__num_sections = num_sections
        self.__timeout = timeout

    def __get_task_info(self):
        request = urllib.request.Request(self.__url, method="GET", headers={
            "Range": "bytes=0-9223372036854775807",
            "Accept": "*/*",
            "User-Ugent": "curl",
        })
        response = urllib.request.urlopen(request, timeout=self.__timeout)
        headers = {key.lower(): value for key, value in response.headers.items()}

        # get file task_info
        output_filename = self.__url.strip("/").split("/")[-1]
        output_len = int(headers.get("content-length", -1))
        is_range_supported = bool(headers.get("content-range", -1))
        if output_len < 0:
            raise Multidown.Error("invalid response content length.")

        # get section count
        section_count = self.__num_sections if is_range_supported else 1
        section_ranges = []
        section_len = max(math.ceil(output_len / section_count), 1024)
        for i in range(0, output_len, section_len):
            section_ranges.append([i, min(i + section_len, output_len)])
        section_count = len(section_ranges)

        self.__task_info = Obj(
            url=self.__url,
            output_filename=output_filename,
            output_len=output_len,
            is_range_supported=is_range_supported,
            section_count=section_count,
            section_ranges=section_ranges,
            section_downloaded_lens=[0] * section_count)

        # try recover downloaded lens from saved task info
        task_info_filename = self.__task_info.output_filename + ".mdowntaskinfo"
        try:
            if os.path.getsize(self.__task_info.output_filename) != self.__task_info.output_len:
                raise Multidown.Error("output file size not match.")
            with open(task_info_filename, "r") as task_info_file:
                saved_task_info = Obj(**json.load(task_info_file))
                cmp1 = json.dumps(dict(saved_task_info, section_downloaded_lens=[]))
                cmp2 = json.dumps(dict(self.__task_info, section_downloaded_lens=[]))
                if cmp2 != cmp1:
                    raise Multidown.Error("task info file is out of date.")

            LOG.info("resuming an existed downloading task ...")
            self.__task_info.section_downloaded_lens = saved_task_info.section_downloaded_lens
        except Exception as e:
            LOG.info("cannot resume an existed downloading task, due to %s", e)
            LOG.info("creating a new download task...")


    def __save_task_info_file(self):
        task_info_filename = self.__task_info.output_filename + ".mdowntaskinfo"
        with open(task_info_filename, "w") as task_info_file:
            json.dump(self.__task_info, task_info_file, indent=2)


    def __delete_task_info_file(self):
        task_info_filename = self.__task_info.output_filename + ".mdowntaskinfo"
        try:
            os.remove(task_info_filename)
        except Exception as e:
            LOG.error("deleting .mdowntaskinfo file failed, you need to delete it by yourself.")


    def __download_section(self, section_index, output_mmap, status):
        time.sleep(0.1 * section_index)  # avoid starting all threads in the same time
        download_range_l = lambda: \
            self.__task_info.section_ranges[section_index][0] + self.__task_info.section_downloaded_lens[section_index]
        download_range_r = lambda: \
            self.__task_info.section_ranges[section_index][1] - 1

        while not status.shutdown.is_set() and download_range_l() <= download_range_r():
            try:
                request = urllib.request.Request(self.__task_info.url, method="GET", headers={
                    "Range": "bytes=%s-%s" % (download_range_l(), download_range_r()),
                    "Accept": "*/*",
                    "User-Ugent": "curl",
                })
                response = urllib.request.urlopen(request, timeout=self.__timeout)
                buf = bytearray()

                while not status.shutdown.is_set() and download_range_l() <= download_range_r():
                    read_bytes = response.read(1024)
                    buf += read_bytes
                    if len(buf) >= 65536 or len(read_bytes) < 1024:
                        buf = buf[ : download_range_r() - download_range_l() + 1]
                        status.lock.acquire()
                        try:
                            output_mmap.seek(download_range_l())
                            output_mmap.write(buf)
                            self.__task_info.section_downloaded_lens[section_index] += len(buf)
                        finally:
                            status.lock.release()
                        buf.clear()
            except (
                urllib.request.socket.timeout,
                urllib.error.HTTPError,
                urllib.error.URLError,
                urllib.error.ContentTooShortError
            ) as e:
                LOG.debug("error: %s, will retry", e)
                time.sleep(1)
                continue
            except Exception as e:
                LOG.fatal("unexcepted error: %s", e)
                status.shutdown.set()
                return
        status.thread_rets[section_index] = download_range_l() > download_range_r()

    def get_checksums(self):
        hash_md5 = hashlib.md5()
        hash_sha1 = hashlib.sha1()
        hash_sha224 = hashlib.sha224()
        hash_sha256 = hashlib.sha256()
        with open(self.__task_info.output_filename, "rb") as file:
            for chunk in iter(lambda: file.read(4096), b""):
                hash_md5.update(chunk)
                hash_sha1.update(chunk)
                hash_sha224.update(chunk)
                hash_sha256.update(chunk)
        return Obj(
            md5=hash_md5.hexdigest(),
            sha1=hash_sha1.hexdigest(),
            sha224=hash_sha224.hexdigest(),
            sha256=hash_sha256.hexdigest())

    def start(self):
        self.__start_time = time.time_ns()
        self.__get_task_info()
        LOG.info("output_filename %s", self.__task_info.output_filename)
        LOG.info("output_len: %s", self.__task_info.output_len)
        LOG.info("is_range_supported: %s", self.__task_info.is_range_supported)
        LOG.info("section_count: %s", self.__task_info.section_count)

        # create mmapping file
        if not os.path.exists(self.__task_info.output_filename):
            open_mode = "wb+"
        else:
            open_mode = "rb+"
        with open(self.__task_info.output_filename, mode=open_mode) as output_file:
            output_file.truncate(self.__task_info.output_len)
            output_file.seek(0)
            with mmap.mmap(output_file.fileno(), 0, prot=mmap.PROT_WRITE) as output_mmap:
                status = Obj(
                    shutdown=threading.Event(),
                    thread_rets=[None] * self.__task_info.section_count,
                    lock=threading.Lock())
                threads = [
                    threading.Thread(target=self.__download_section, args=(i, output_mmap, status))
                    for i in range(self.__task_info.section_count)
                ]

                def statistic():
                    downloaded_len_start = sum(self.__task_info.section_downloaded_lens)
                    while not status.shutdown.is_set():
                        time.sleep(1)
                        status.lock.acquire()
                        try:
                            downloaded_len = sum(self.__task_info.section_downloaded_lens)
                            downloaded_progress = downloaded_len / self.__task_info.output_len * 100
                            downloaded_kbps = (downloaded_len - downloaded_len_start) / (time.time_ns() - self.__start_time) * 1e6

                            LOG.info("progress: %.2f%%, speed: %.1f KB/s", downloaded_progress, downloaded_kbps)
                            self.__save_task_info_file()
                            if downloaded_len >= self.__task_info.output_len:
                                self.__delete_task_info_file()
                                break
                        finally:
                            status.lock.release()
                    return True
                threads.append(threading.Thread(target=statistic))
                for thread in threads:
                    thread.start()
                for thread in threads:
                    try:
                        thread.join()
                    except KeyboardInterrupt as e:
                        status.shutdown.set()
                return all(status.thread_rets)


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-n", "--num_sections", default="4", type=int, help="number of sections")
    argparser.add_argument("-t", "--timeout", default="10", type=int, help="number of seconds before timing out")
    argparser.add_argument("url", type=str)
    args = argparser.parse_args()

    # start downloading
    multidown = Multidown(args.url, args.num_sections, args.timeout)
    if multidown.start():
        LOG.info("download finished.")
        LOG.info("checksums: %s", json.dumps(multidown.get_checksums(), indent=2))
    else:
        LOG.warning("download not finished.")
        sys.exit(-1)
