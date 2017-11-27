#!/usr/bin/env python2
# coding: utf-8

import os
import sys
import argparse
import logging
import json

from pykit import logutil
from pykit import jobq
from pykit import humannum

import mysqlbackup

logger = logging.getLogger(__name__)

if __name__ == "__main__":

    rootlogger = logutil.make_logger(base_dir='/tmp',
                                     log_fn=logutil.get_root_log_fn(),
                                     level=logging.DEBUG)
    logutil.add_std_handler(rootlogger, stream=sys.stdout)
    rootlogger.handlers[1].setLevel(logging.WARN)

    parser = argparse.ArgumentParser(description='run commands for one or more ports concurrenty')

    parser.add_argument('--conf-base', type=str, required=False,  help='base path to config file')
    parser.add_argument('--jobs',      type=int, required=False, default=1, help='nr of threads to run')
    parser.add_argument('--cmd',       type=str, required=True,  choices=[
        'backup',
        'catchup',
        'make_mycnf',
        'optimize',
        'replication_diff',
        'restore',
        'restore_from_backup',
        'setup_replication',
        'table_size',
    ], help='command to run')
    parser.add_argument('--ports',     type=int, required=False, nargs='+', help='ports to run "cmd" on')
    parser.add_argument('--db',        type=str, required=False, help='specifies db name to run command on')
    parser.add_argument('--human',     action='store_true', required=False,  help='print result for human')
    parser.add_argument('--full',      action='store_true', required=False,  help='do not reduce any info when display')
    parser.add_argument('--size',      type=str, required=False,  help='specify size filter expression e.g.: ">10M"')
    parser.add_argument('--sortby',    type=str, required=False,  choices=['free', 'total', 'used'], help='sort by')

    parser.add_argument('--date-str',            action='store', help='date in form 2017_01_01. It is used in backup file name, or to specify which backup to use for restore. when absent, use date of today')
    parser.add_argument('--clean-after-restore', action='store_true', help='clean backup files after restore')

    args = parser.parse_args()
    logger.info('command:' + str(args))

    if args.conf_base is None:
        args.conf_base = '/s2/mysql/backup_conf'

    ports = args.ports

    if ports is None:
        ports = os.listdir(args.conf_base)

        ports = [int(x) 
                 for x in ports
                 if x.isdigit()]
        ports.sort()

    cmd = args.cmd
    date_str = mysqlbackup.backup_date_str()
    rsts = {}

    def setdef(dic, key, v):
        if v is not None:
            dic[key] = v

    def worker(port):

        try:
            rst = _worker(port)
            rsts[port] = True
            return rst
        except Exception as e:
            logger.exception(repr(e))
            return jobq.EmptyRst

    def _worker(port):

        conf_path = '{conf_base}/{port}/backup_conf.yaml'.format(
                conf_base=args.conf_base, port=port)

        conf = mysqlbackup.load_conf_from_file(conf_path)

        setdef(conf, 'date_str', args.date_str)
        setdef(conf, 'clean_after_restore', args.clean_after_restore)

        conf.setdefault('date_str', date_str)

        mb = mysqlbackup.MysqlBackup(conf)

        if cmd == 'backup':
            mb.backup()
        elif cmd == 'setup_replication':
            mb.setup_replication()

        elif cmd == 'restore':
            if mb.has_data_dir():
                logger.info('data-dir presents, skip restore_from_backup')
                pass
            else:
                mb.restore_from_backup()
            mb.catchup()

        elif cmd == 'restore_from_backup':
            if mb.has_data_dir():
                logger.info('data-dir presents, skip restore_from_backup')
                pass
            else:
                mb.restore_from_backup()
        elif cmd == 'catchup':
            mb.catchup()

        elif cmd == 'make_mycnf':
            mb.make_runtime_my_cnf()

        elif cmd == 'optimize':
            mb.optimize_tables(args.db)

        elif cmd == 'replication_diff':
            rst = mb.diff_replication()
            if not args.full:
                for k, diff in rst.items():
                    if isinstance(diff, basestring):
                        continue

                    for side in ('onlyleft', 'onlyright'):
                        if diff[side]['length'] == 0:
                            del diff[side]

            if args.human:
                mapping = {
                        'onlyleft': 'OnlyIHave',
                        'onlyright': 'IDontHave'
                }
                hm = []
                for k, diff in rst.items():
                    if isinstance(diff, basestring):
                        hm.append('{k:>24}: {desc}'.format(k=k, desc=diff))
                        continue
                    for side in ('onlyleft', 'onlyright'):
                        if side not in diff:
                            continue
                        d = diff[side]
                        line = '{k:>24}: {side:>10}: {length:>10}: {rs}'.format(
                                k=k,
                                side=mapping[side],
                                length=d['length'],
                                rs=str(d['gtidset']))
                        hm.append(line)

                def _out():
                    for line in hm:
                        print line
                rst = _out
            return rst
        elif cmd == 'table_size':
            rsts = mb.table_sizes(args.db, args.sortby)
            def _out():
                print port, args.db
                for _repr, tbl_stat in rsts:
                    if args.size is not None:
                        op = args.size[0]
                        num = humannum.parseint(args.size[1:])

                        if op == '>' and tbl_stat['Data_length'] < num:
                            continue
                        if op == '<' and tbl_stat['Data_length'] > num:
                            continue
                    print _repr
            return _out
        else:
            raise ValueError('unsupported command: ' + repr(cmd))

        return jobq.EmptyRst

    def output(rst):
        if callable(rst):
            rst()
        else:
            print json.dumps(rst, indent=2)

    jm = jobq.JobManager([(worker, args.jobs),
                          (output, 1)])

    for port in ports:
        jm.put(port)

    jm.join()

    if len(rsts) == len(ports):
        sys.exit(0)
    else:
        sys.exit(1)


