#!/usr/bin/env python
# coding: utf-8

from __future__ import print_function, division

import os
import os.path
import sys
import argparse
import itertools
import re
import time
import random
import socket

import psutil

from twisted.python import reflect, lockfile

from ._misc import required_attr, natural_sorted, mkdir_p


if sys.hexversion < 0x3000000:
    from itertools import imap
else:
    imap = map


class GenInit(object):

    appname = required_attr

    workers = 1
    singletone = False
    disabled = False

    log_dir = os.path.expanduser("~")
    pid_dir = os.path.expanduser("~")

    env = None
    cwd = os.path.expanduser("~")

    flock_timeout = 15

    def __init__(self):
        assert not self.singletone or self.workers == 1
        self._verbose = False
        self._quiet = False
        self._remove_stale_pidfiles = False
        self._process_cache = {}

    def wait_for_processes(self, processes, timeout):

        processes = list(processes)
        if not processes:
            return
        self.log_debug("waiting for processes {0}", [p.pid for p in processes])

        if not self._quiet:
            print("waiting...", end="")
            sys.stdout.flush()

        end_time = time.time() + timeout
        step = 0.2
        last_dot_time = time.time()

        while processes and time.time() <= end_time:

            if not self._quiet and last_dot_time + 1 < time.time():
                print(".", end="")
                last_dot_time = time.time()
                sys.stdout.flush()

            pr = random.choice(processes)
            try:
                pr.wait(timeout=step)
            except psutil.TimeoutExpired:
                step = min(step, end_time - time.time())
            except psutil.NoSuchProcess:
                step = 0
                pass

            processes = [p for p in processes if p.is_running()]

        if not processes:
            print(" ok")
            return True
        else:
            print(" fail")

    @property
    def default_workerids(self):
        if self.singletone:
            return [self.appname]
        else:
            return imap(self.coerce_workerid, range(self.workers))

    @property
    def all_possible_workerids(self):
        return imap(self.coerce_workerid, itertools.count())

    def log_debug(self, msg, *args, **kwargs):
        if self._verbose:
            print(msg.format(*args, **kwargs))

    def log_error(self, msg, *args, **kwargs):
        print(msg.format(*args, **kwargs), file=sys.stderr)

    def log(self, msg, *args, **kwargs):
        if not self._quiet:
            print(msg.format(*args, **kwargs))

    def load_workerids_from_pidfiles(self):

        self.log_debug("scan directory {0!r}", self.pid_dir)
        pat = re.compile(r"(" + re.escape(self.appname) + r".*?)\.pid")

        result = []
        running_pids = []

        for fn in natural_sorted(os.listdir(self.pid_dir)):
            m = pat.match(fn)
            if not m:
                continue

            workerid = m.group(1)
            self.log_debug("found pidfile {0!r}, workerid {1!r}", fn, workerid)

            p = self.worker_process(workerid)
            if p:
                result.append(workerid)
                running_pids.append(p.pid)
            else:
                self.log_error("stale pidfile {0!r}", fn)

        self._check_workers_without_pidfiles(running_pids)
        return result

    def _check_workers_without_pidfiles(self, known_pids):
        if self._quiet:
            return
        known_pids = set(known_pids)
        for p in psutil.process_iter():
            if p.pid in known_pids:
                continue
            workerid = self.workerid_of_process(p)
            if workerid:
                self.log_error("! hanged worker {0} (pid {1})", workerid, p.pid)

    def load_worker_pidfile(self, workerid):

        pidfile = os.path.join(self.pid_dir, workerid + ".pid")
        if not os.path.exists(pidfile):
            self.log_debug("no pidfile {0!r}", pidfile)
            return None
        with open(pidfile, 'r') as f:
            self.log_debug("read pidfile {0!r}", pidfile)
            pid_str = "".join(f.readlines())
            try:
                pid = int(pid_str)
            except ValueError:
                self.log_error("invalid pid {0!r}", pid_str)
                return None

        try:
            p = psutil.Process(pid)
        except psutil.NoSuchProcess:
            self.log_debug("no process with pid {0}", pid)
            p = None

        if p and self.is_worker_process(workerid, p):
            return pid
        elif self._remove_stale_pidfiles:
            self.log_error("remove stale pidfile {0!r}", pidfile)
            os.remove(pidfile)
            return None

    def is_worker_process(self, workerid, p):
        return self.workerid_of_process(p) == workerid

    def _process_environ(self, p):
        try:
            with open("/proc/%s/environ" % p.pid, 'rb') as f:
                d = f.read()
        except IOError:
            return {}

        lines = d.strip(b"\0").split(b"\0")
        return dict(
            line.split(b"=", 1)
            for line in lines
        )

    def workerid_of_process(self, p):
        env = self._process_environ(p) or {}
        wid = env.get('WORKERID')
        pid_dir = env.get('PID_DIR')
        if wid and self.is_workerid(wid) and self.pid_dir == pid_dir:
            return wid

    def create_worker_environ(self, workerid):
        env = dict(self.env or os.environ)
        env['WORKERID'] = str(workerid)
        env['PID_DIR'] = self.pid_dir
        env['LOG_DIR'] = self.log_dir
        return env

    def run_worker_process(self, workerid):
        cmdline_list = self.create_worker_command_line(workerid)
        stdout = sys.stdout
        stderr = sys.stderr
        cwd = self.cwd
        env = self.create_worker_environ(workerid)
        self.log_debug("env is {0!r}", env)
        self.log_debug("run {0!r}", cmdline_list)
        return psutil.Popen(cmdline_list, stdout=stdout, stderr=stderr, cwd=cwd, env=env)

    def create_worker_command_line(self, workerid):
        raise NotImplementedError

    def worker_process(self, workerid):

        # store *live* processes in cache to calculate CPU usage
        wp = self._process_cache.get(workerid)
        if wp and wp.is_running():
            return wp

        pid = self.load_worker_pidfile(workerid)
        p = pid and psutil.Process(pid)
        if p:
            p.cpu_percent(0)

        self._process_cache[workerid] = p
        return p

    # --- commands

    def command_worker_start(self, workerid, **kwargs):

        if self.disabled:
            self.log_error("app is disabled, can't run {0}", workerid)
            return False

        self.log("start worker {0}", workerid)

        np = self.worker_process(workerid)
        if np and np.is_running():
            self.log_error("worker {0} is already running", workerid)
            return False

        p = self.run_worker_process(workerid)
        self.log_debug("wait twistd...")
        exit_code = p.wait(timeout=60)
        if exit_code:
            self.log_error("command terimnated with exit code {0}", exit_code)

        np = self.worker_process(workerid)
        return np and np.is_running()

    def _maybe_kill_worker(self, workerid, wait=0.2):

        np = self.worker_process(workerid)
        if not np or not np.is_running():
            return

        self.log("kill process {0}", np.pid)
        np.kill()
        if not wait:
            return

        self.wait_for_processes([np], wait)
        if np.is_running():
            self.log_error("process doesn't respond to SIGKILL, argh!")

    def command_worker_stop(self, workerid, wait=30, kill=False, **kwargs):

        self.log("stop worker {0}", workerid)

        np = self.worker_process(workerid)
        if not np or not np.is_running():
            self.log_error("worker {0} is already down!", workerid)
            return False

        children = np.children()

        np.terminate()
        if wait:
            self.wait_for_processes([np], wait)

        if wait and np.is_running():
            if kill:
                self._maybe_kill_worker(workerid, wait)
            else:
                self.log_error("process still running")

        result = not wait or not np.is_running()

        if not np.is_running():
            for c in children:
                if c.is_running():
                    self.log_error("! child process still alive (pid {0})", c.pid)

        return result

    def command_worker_status(self, workerid, **kwargs):
        self.log_debug("check command_status of worker {0}", workerid)
        np = self.worker_process(workerid)
        if workerid in set(self.default_workerids):
            worker_name = workerid
        else:
            worker_name = workerid + "*"
        if np:
            self.log("worker {0} is running (pid {1})", worker_name, np.pid)
            return True
        else:
            self.log("worker {0} is down", worker_name)
            return False

    def command_worker_status_bool(self, workerid, **kwargs):
        np = self.get_worker_process(workerid)
        print(int(bool(np)))
        return True

    def command_worker_restart(self, **kwargs):
        self.command_worker_stop(**kwargs)
        return self.command_worker_start(**kwargs)

    def command_worker_info(self, workerid, all=False, nowait=False, **kwargs):

        self.log_debug("show process info for worker {0}", workerid)
        np = self.worker_process(workerid)

        if not np:
            self.log("worker {0} is down", workerid)
            self.log("")
            return False

        mi = np.memory_info()
        ct = np.cpu_times()
        mp = np.memory_percent()
        cp = np.cpu_percent(None if nowait else 0.2)
        tc = np.num_threads()
        info = (
            "cpu {cpu_p:>5.1%} - {cpu_tot:g}s usr / {cpu_sys:g}s sys,\t"
            "mem rss {mem_p:>3.1%} ({mem_rss:g}M),  vms {mem_vms:g}M,\t"
            "threads {th_cnt}"
        ).format(
            cpu_tot=round(ct.user + ct.system),
            cpu_sys=round(ct.system),
            cpu_p=(cp / 100),
            mem_rss=round(mi.rss / 2 ** 20),
            mem_vms=round(mi.vms / 2 ** 20),
            mem_p=(mp / 100),
            th_cnt=tc,
        )

        if not all:
            self.log("worker {0}: \t{1}", workerid, info)
            return True
        else:
            self.log("worker {0}:", workerid)
            self.log("{0}", info)

        connections = np.connections('all')
        self.log("connections {0}", len(connections))
        for c in connections:
            ctype = {socket.SOCK_STREAM: "STREAM", socket.SOCK_DGRAM: "DGRAM"}[c.type]
            cfamily = {socket.AF_INET: "INET",
                       socket.AF_INET6: "INET6",
                       socket.AF_UNIX: "UNIX"}[c.family]
            self.log(
                "\t{cfamily}/{ctype}\t{status}\tlocal {laddr} - remote {raddr}",
                status=c.status, cfamily=cfamily, ctype=ctype,
                laddr=c.laddr, raddr=c.raddr,
            )

        openfiles = np.open_files()
        self.log("open files {0}", len(openfiles))
        for of in np.open_files():
            self.log("\t{0!r}", of.path)

        self.log("")
        return True

    def command_restart_worker(self, workerid, **kwargs):
        np = self.worker_process(workerid)
        if np:
            self.command_worker_stop(workerid, **kwargs)
        return self.command_worker_start(workerid, **kwargs)

    def command_status(self, **kwargs):
        x = True
        active_workerids = set(self.load_workerids_from_pidfiles())
        for workerid in natural_sorted(active_workerids | set(self.default_workerids)):
            x = self.command_worker_status(workerid=workerid, **kwargs) and x
        return x

    def command_status_bool(self, **kwargs):
        active_workerids = set(self.load_workerids_from_pidfiles())
        status = all(
            self.get_worker_process(workerid)
            for workerid in (active_workerids | set(self.default_workerids))
        )
        print(int(bool(status)))
        return True

    def command_stop(self, wait=30, kill=False, **kwargs):

        all_workes = set(self.load_workerids_from_pidfiles()) | set(self.default_workerids)
        res = True

        for workerid in natural_sorted(all_workes):
            res = self.command_worker_stop(
                workerid=workerid, wait=None, kill=kill, **kwargs) and res
        if wait and all_workes:
            nps = list(filter(None, map(self.worker_process, all_workes)))
            self.wait_for_processes(nps, wait)

        if kill:
            # no wait
            for workerid in natural_sorted(all_workes):
                self._maybe_kill_worker(workerid, wait=0)
            res = self.wait_for_processes(nps, 0.1) and res

            # final shot (check if process is still alive)
            for workerid in natural_sorted(all_workes):
                self._maybe_kill_worker(workerid, wait=0.01)

        return res

    def command_start(self, **kwargs):
        if self.disabled:
            self.log_error("app {0} is disabled, can't run workers", self.appname)
            return False
        for workerid in self.default_workerids:
            self.command_worker_start(workerid=workerid, **kwargs)
        return self.command_status()

    def command_restart(self, **kwargs):
        workerids = set(self.load_workerids_from_pidfiles()) | set(self.default_workerids)
        for workerid in natural_sorted(workerids):
            self.command_worker_restart(workerid=workerid, **kwargs)
        time.sleep(0.2)
        return self.command_status()

    def command_info(self, **kwargs):
        x = True
        active_workerids = set(self.load_workerids_from_pidfiles())
        workerids = natural_sorted(active_workerids)

        # preinitialize cpu_percent counter
        for workerid in workerids:
            self.worker_process(workerid).cpu_percent(None)
        time.sleep(0.2)

        for workerid in workerids:
            x = self.command_worker_info(workerid=workerid, nowait=True, **kwargs) and x
        return x

    def command_add_worker(self, **kwargs):
        self.log("add new worker")
        workerids = set(self.load_workerids_from_pidfiles())
        for workerid in self.all_possible_workerids:
            if workerid in workerids:
                self.log_debug("worker {0} already running", workerid)
                continue
            return self.command_worker_start(workerid=workerid, **kwargs)

    def command_remove_worker(self, **kwargs):
        self.log("remove worker")
        workers = list(natural_sorted(self.load_workerids_from_pidfiles()))
        if not workers:
            self.log_error("no workers running")
            return False
        workerid = workers[-1]
        self.command_worker_stop(workerid=workerid, **kwargs)

    def create_parser(self):
        parser = argparse.ArgumentParser()

        verbose_group = parser.add_mutually_exclusive_group()
        verbose_group.add_argument('--verbose', '-v', action='store_true', default=False,
                                   help="_verbose output", dest='_verbose')
        verbose_group.add_argument('--quiet', '-q', action='store_true', default=False,
                                   help="suppress output", dest='_quiet')

        parser.add_argument('--workers', type=int, help="number of workers")
        parser.add_argument('--logs', dest='log_dir', help="logs directory")
        parser.add_argument('--remove-stale-pidfiles', action='store_true',
                            help="remove stale pidfiles", dest='_remove_stale_pidfiles')

        self.create_subparsers(parser)
        return parser

    def command_num_workers(self, **kwargs):
        nps = self.load_workerids_from_pidfiles()
        self.log("{0}", len(list(nps)))

    def coerce_workerid(self, s):
        s = str(s)
        if self.is_workerid(s):
            return s
        elif not self.singletone:
            return self.appname + "-" + s
        else:
            raise ValueError("invalid workerid %r" % s)

    def is_workerid(self, s):
        s = str(s)
        return s == self.appname or s.startswith(self.appname + "-")

    def create_subparsers(self, parser):

        sp = parser.add_subparsers(dest='command')

        # parent parsers
        p_all_workers = argparse.ArgumentParser(add_help=False)

        p_start = argparse.ArgumentParser(add_help=False)

        p_stop = argparse.ArgumentParser(add_help=False)
        p_stop.add_argument('--kill', '-k', action='store_true', help="kill with SIGKILL")
        p_stop.add_argument('--wait', '-w', type=float, dest='wait',
                            default=10, help="wait process termination")

        p_status = argparse.ArgumentParser(add_help=False)

        p_info = argparse.ArgumentParser(add_help=False)
        p_info.add_argument('--all', '-a', action='store_true',
                            help="show open files and ports")

        p_worker = argparse.ArgumentParser(add_help=False)
        p_worker.add_argument('workerid', type=self.coerce_workerid, help="worker id")

        # subparsers
        sp.add_parser('start', parents=[p_start, p_all_workers])
        sp.add_parser('stop', parents=[p_stop, p_all_workers])
        sp.add_parser('status', parents=[p_status, p_all_workers])
        sp.add_parser('status_bool', parents=[p_status, p_all_workers])
        sp.add_parser('restart', parents=[p_start, p_stop, p_all_workers, p_status])
        sp.add_parser('info', parents=[p_info, p_all_workers])

        if not self.singletone:
            sp.add_parser('worker_start', parents=[p_worker, p_start])
            sp.add_parser('worker_stop', parents=[p_worker, p_stop])
            sp.add_parser('worker_status', parents=[p_worker, p_status])
            sp.add_parser('worker_status_bool', parents=[p_worker, p_status])
            sp.add_parser('worker_restart', parents=[p_worker, p_start, p_stop, p_status])
            sp.add_parser('worker_info', parents=[p_worker, p_info])
            sp.add_parser('add_worker', parents=[p_start])
            sp.add_parser('remove_worker', parents=[p_stop])
            sp.add_parser('num_workers')

        return sp

    def process_command_args(self, parsed_args):
        for f in ['_verbose', '_quiet', 'log_dir', '_remove_stale_pidfiles', 'workers']:
            if hasattr(parsed_args, f):
                v = getattr(parsed_args, f)
                if v is None:
                    continue
                self.log_debug("set option `{0}` to {1!r}", f, v)
                setattr(self, f, v)

    def _create_dirs(self):
        mkdir_p(self.log_dir)
        mkdir_p(self.pid_dir)

    def _make_flock(self):
        fn = os.path.join(self.pid_dir, self.appname + ".lock")
        return lockfile.FilesystemLock(fn)

    def _acquire_flock(self, lock):

        if lock.lock():
            self.log_debug("flock has been acquired")
            return

        if not self._quiet:
            print("acquire lock...", end="")
            sys.stdout.flush()

        end_time = time.time() + self.flock_timeout
        step = 0.2
        last_dot_time = time.time()

        while time.time() <= end_time:

            if lock.lock():
                self.log_debug("flock has been acquired")
                return

            time.sleep(step)
            if not self._quiet and last_dot_time + 1 < time.time():
                print(".", end="")
                last_dot_time = time.time()
                sys.stdout.flush()

        self.log_error("there is another running geninit process")

    def run(self, args=None):

        self._create_dirs()
        parser = self.create_parser()
        lock = self._make_flock()

        parsed_args = parser.parse_args(args)
        self.process_command_args(parsed_args)

        command_name = parsed_args.command
        self.log_debug("run command {0!r}", command_name)
        command = getattr(self, 'command_' + command_name)

        self._acquire_flock(lock)
        try:
            x = command(**vars(parsed_args))
        finally:
            lock.unlock()

        self.log_debug("exit with code {0}", int(not x))
        sys.exit(not x)


# -- integration with twisted

class TwistedGenInit(GenInit):

    tac_file = required_attr
    twistd_log_file = "/dev/null"
    twistd_args = ()

    def worker_pidfile(self, workerid):
        return os.path.join(self.pid_dir, workerid + ".pid")

    def create_worker_command_line(self, workerid):
        return [
            'twistd',
            '--logfile=' + self.twistd_log_file,
            '--pidfile=' + self.worker_pidfile(workerid),
            '-y',
            self.tac_file,
        ] + list(self.twistd_args or [])

    def workerid_of_process(self, p):
        # optimization - don't scan all processes, only `twistd`
        try:
            p_name = p.name()
        except psutil.NoSuchProcess:
            p_name = None
        return p_name == 'twistd' and GenInit.workerid_of_process(self, p)


class Worker(TwistedGenInit):

    tac_file = os.path.join(os.path.dirname(__file__), "_geninit_worker_tac.py")

    def create_worker_environ(self, workerid):
        env = TwistedGenInit.create_worker_environ(self, workerid)
        env['GENINIT_CTOR'] = reflect.qual(type(self))
        return env

    def create_twisted_application(self):
        workerid = os.environ['WORKERID']
        return self.create_app(workerid)

    def create_app(self, workerid):
        raise NotImplementedError


def main(argv=None, gi_ctor=None):

    args = argv or sys.argv[1:]

    if gi_ctor is None:
        if not args:
            print("Please, provide full name of worker/geninit-class", file=sys.stderr)
            sys.exit(2)
        gi_ctor_name = args[0]
        gi_ctor = reflect.namedAny(gi_ctor_name)
        if not gi_ctor:
            print("No function/class found: %r" % gi_ctor, file=sys.stderr)
            sys.exit(2)

    gi_ctor().run(args[1:])


if __name__ == '__main__':
    main()
