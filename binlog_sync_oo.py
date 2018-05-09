# /usr/bin/env python2
# coding: utf-8

import binascii
import heapq
import logging
import os
import Queue
import random
import sys
import time

from pykit import fsutil
from pykit import logutil
from pykit import mysqlconnpool
from pykit import mysqlutil
from pykit import shell
from pykit import strutil
from pykit import threadutil
from pykit import utfyaml

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.row_event import DeleteRowsEvent
from pymysqlreplication.row_event import RowsEvent
from pymysqlreplication.row_event import UpdateRowsEvent
from pymysqlreplication.row_event import WriteRowsEvent


logger = logging.getLogger(__name__)


class ConnectionError(Exception):
    pass


class BinlogSync(object):

    def __init__(self, conf, n_writers=3, n_ticks=1000, n_rounds=10, n_rows=10):

        self.source_conn = conf.get('source_conn')
        self.database = conf.get('database')
        self.source_table = conf.get('source_table')

        self.tb_index = conf.get('table_index')
        self.tb_fields = conf.get('table_fields')
        self.shard_fields = conf.get('shard_fields')

        self.new_conns = conf.get('new_conns')
        self.shards = conf.get('shards')

        self.binlog_pos_file = conf.get('position_file')
        self.binlog_pos = conf.get('init_position')

        self.start = conf.get('start')
        self.end = conf.get('end')

        self.n_ticks = n_ticks

        self.n_writers = n_writers
        self.writers = []

        self.time = time.time()
        self.last_stat = {}
        self.stat = {
            'n_records': 0,
            'n_out_range': 0,
            'log_pos': 0,
        }

        self.db_pool = {}

    def _is_in_range(self, row):

        shard = [row.get(k) for k in self.shard_fields]
        if self.end is not None:
            return shard >= self.start and shard < self.end
        else:
            return shard >= self.start

    def find_shard(self, idx_shard):

        lo = 0
        hi = len(idx_shard)

        while lo < hi:

            i = (lo + hi) / 2

            shard = self.shards[i].get('from')

            if shard == idx_shard:
                return self.shards[i]

            if shard < idx_shard:
                lo = i + 1
            else:
                hi = i

        return self.shards[hi - 1]

    def get_conn(self, row):

        idx_shard = [row.get(x) for x in self.tb_index]

        shard = self.find_shard(idx_shard)

        db = shard.get('db')
        table = shard.get('table')

        pool = self.db_pool.get(db)
        if pool is None:
            conn = self.new_conns.get(db)
            pool = mysqlconnpool.make(conn)
            self.db_pool[db] = pool

        return pool, db, table

    def prepare_delete(self, before, after):

        pool, db, table = self.get_conn(before)

        index_values = [before.get(x) for x in self.tb_index]

        sql = mysqlutil.make_delete_sql(
            (str(db), table), self.tb_index, index_values, limit=1)

        return pool, sql

    def prepare_insert(self, before, after):

        pool, db, table = self.get_conn(after)

        values = [after.get(x) for x in self.tb_fields]
        fields = self.tb_fields

        sql = mysqlutil.make_insert_sql((str(db), table), values, fields)

        return pool, sql

    def prepare_update(self, before, after):

        pool, db, table = self.get_conn(before)

        index_values = [before.get(x) for x in self.tb_index]

        sql = mysqlutil.make_update_sql(
            (str(db), table), after, self.tb_index, index_values, limit=1)

        return pool, sql

    def write_to_db(self, th_idx, in_q, out_q):

        while True:

            try:
                # blocking Queue.get does not respond killing signal by <C-c>
                idx, ev, get_conn, former, current = in_q.get(
                    block=True, timeout=1)
            except Queue.Empty as e:
                time.sleep(1)
                continue

            try:
                pool, sql = get_conn(former, current)

                with pool() as conn:
                    conn.query(sql, retry=3)

                out_q.put((idx, ev, former, current, th_idx))

            except Exception as e:
                logger.exception(repr(e))
                self.exit('query database error')

            in_q.task_done()

    def collecter(self):

        prev_idx = 0

        # heapq.merge use the first element idx to make sure first-in first-out.
        for args in heapq.merge(*[x['out_q_iter'] for x in self.writers]):

            idx, ev, former, current, writer_idx = args

            assert prev_idx < idx, 'idx must be incremental'
            prev_idx += 1

            if isinstance(ev, RotateEvent):
                self.log_file = ev.next_binlog
                self.log_pos = ev.position
                continue

            self.stat['log_pos'] = ev.packet.log_pos

            self.stat['n_records'] += 1
            self.writers[writer_idx]['n_wrote'] += 1

            if self.stat['n_records'] % self.n_ticks == 0:
                self.logger_stat(ev)

    def logger_stat(self, event):

        now = time.time()

        logger.info('======================= stat =======================')
        for wt in self.writers:
            logger.info('writer-{idx}: in-q: {iq}, out-q: {oq}, n_write: {n_wrote}'.format(
                iq=wt['in_q'].qsize(),
                oq=wt['out_q'].qsize(),
                **wt
            ))

        logger.info('Last event loc: {log_file:>10}{log_pos:>10}'.format(
            log_file=self.binlog_pos_file,
            log_pos=self.binlog_pos))

        logger.info('Last event table: {table}'.format(table=event.table))
        logger.info('Last event rows: {n_rows}'.format(n_rows=len(event.rows)))

        for k, v in self.stat:
            duration = now - self.time
            n_add = v - self.last_stat.get(k, 0)

            nps = round(n_add / duration, 3)

        logger.info('{k:<24}: {n_k:>10} {nps:>10.3f}/sec'.format(
            k=k, n_k=v, nps=nps))

        self.last_stat = self.stat.copy()

    def sync(self):

        logger.info("Start catching up from {log_pos}".format(
            log_pos=self.binlog_pos))

        for i in xrange(self.n_writers):
            in_q = Queue.Queue(10240)
            out_q = Queue.Queue(10240)

            th = threadutil.start_daemon_thread(
                self.write_to_db, args=(i, in_q, out_q, ))

            self.writers.append({
                'idx': i,
                'in_q': in_q,
                'out_q': out_q,
                'out_q_iter': self._queue_iter(out_q),
                'thread': th,
                'n_wrote': 0,
            })

        threadutil.start_daemon_thread(self.collecter, args=())

        self.make_binlog_reader()

        idx = 0
        for ev in self.binlog_reader:

            idx += 1
            if isinstance(ev, RotateEvent):
                self.writers[0]['in_q'].put((idx, ev, 'rotate', None, None))
                continue

            if not isinstance(ev, RowsEvent):
                continue

            elts = self.make_ev_elts(ev)

            for ev, get_conn, former, current in elts:
                record = former or current
                # We need to guarantee same index event in same thread,
                # because only operations on a same index are needed to keep
                # in order.
                index = [record.get(x) for x in self.tb_index]

                hsh = binascii.crc32(repr(index))

                writer = self.writers[hsh % self.n_writers]
                writer['in_q'].put((idx, ev, get_conn, former, current))

    def check(self):

        src_pool = mysqlconnpool.make(self.source_conn)

        sql = 'SELECT COUNT(*) FROM `{db}`.`{table}`'.format(
            db=self.database, table=self.source_table)

        rsts = src_pool.query(sql, use_dict=False)
        rst = rsts[0]

        total = int(rst[0])
        logger.info(
            'start checking, total number is: {total}'.format(total=total))

        for i in xrange(self.nround):
            offset = random.randint(0, total - 1)

            sql = 'SELECT * FROM `{db}`.`{table}` LIMIT {offset}, {limit}'.format(
                db=self.database, table=self.source_table,
                offset=offset, limit=self.nrow)

            rows = src_pool.query(sql)

            for row in rows:

                if self.is_row_exsist(row):

                    logger.info('{progress:>5.1%} check {nrow} rows > {start_id} found: {row}'.format(
                        progress=float(i) / self.nround,
                        nrow=self.nrow,
                        start_id=offset,
                        row=row,
                    )
                    )

                    continue

                msg = 'NOT found: {row} in new table'.format(row=row)
                logger.warn(msg)
                os.write(1, '\n')
                os.write(1, str(strutil.yellow(msg) + '\n'))

    def is_row_exsist(self, row):

        shard = [row.get(x) for x in self.shard_fields]
        pool, db, table = self.get_conn(shard)

        index_values = [row.get(x) for x in self.tb_index]
        sql = mysqlutil.make_select_sql(
            (str(db), table), None, self.tb_index, index_values)

        with pool() as conn:
            rst = conn.query(sql, retry=3)

        if len(rst) > 0:
            return True

        return False

    def make_binlog_reader(self):

        self.binlog_reader = BinLogStreamReader(
            connection_settings=self.source_conn,
            server_id=99999,
            blocking=True,
            resume_stream=True,
            log_file=self.binlog_pos_file,
            log_pos=self.binlog_pos,
            only_schemas=self.database,
            only_tables=self.source_tableb,
            only_events=[DeleteRowsEvent,
                         UpdateRowsEvent,
                         WriteRowsEvent,
                         RotateEvent],
        )

    def make_ev_elts(self, ev):

        elts = []

        def present(row): return row.get('values')

        def absent(row): return None

        def get_before(row): return row.get('before_values')

        def get_after(row): return row.get('after_values')

        if isinstance(ev, WriteRowsEvent):
            get_conn = self.prepare_insert
            former_mod = absent
            current_mod = present

        elif isinstance(ev, DeleteRowsEvent):
            get_conn = self.prepare_delete
            former_mod = present
            current_mod = absent

        elif isinstance(ev, UpdateRowsEvent):
            get_conn = self.prepare_update
            former_mod = get_before
            current_mod = get_after

        else:
            pass

        for row in ev.rows:
            current = current_mod(row)
            former = former_mod(row)

            record = former or current

            if self._is_in_range(record):
                elts.append((ev, get_conn, former, current))
            else:
                self.stat['n_out_range'] += 1

        return elts

    def _queue_iter(q):

        while True:
            try:
                # blocking Queue.get does not respond killing signal by <C-c>
                x = q.get(block=True, timeout=1)
            except Queue.Empty:
                time.sleep(1)
            else:
                yield x

    def exit(self, msg=''):

        fsutil.write_file('binlog_process',
                          utfyaml.dump(
                              {'log_file': self.binlog_pos_file, 'log_pos': self.binlog_pos}),
                          atomic=True)

        logger.info(msg)

        sys.exit(1)


def sync(conf_path, n_writers, n_ticks):

    conf = utfyaml.load(conf_path)

    binlog_sync = BinlogSync(conf, n_writers, n_ticks)

    binlog_sync.sync()


def check(conf_path, nrounds, nrows):

    conf = utfyaml.load(conf_path)

    binlog_sync = BinlogSync(conf, nrounds, nrows)

    binlog_sync.check()


if __name__ == "__main__":

    logutil.make_logger(level=logging.INFO)

    shell.command(**{

        '__description__': 'use to synchronize mysql binlog.',

        'sync': (sync,
                  ('conf_path', {'type': str,
                                 'help': 'path of the yaml config'}),
                  ('n_writers', {'type': int,
                                 'help': 'how many threads writing to db'}),
                  ('n_ticks',   {'type': int,
                                 'help': 'how many records to report state'}),
                  ),

        'check': (check,
                  ('conf_path', {'type': str,
                                 'help': 'path of the yaml config'}),
                  ('nrounds',    {'type': int,
                                'help': 'number of round of consistency check'}),
                  ('nrows',      {'type': int,
                                 'help': 'number of row to check in each round'}),
                  ),

        '__add_help__': {
            ('sync',): 'use to synchronize records from binlog.',
            ('check',): 'use to check consistency after synchronize.',
        },
    })
