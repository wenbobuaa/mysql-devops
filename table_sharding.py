import logging
import os
import urllib

from pykit import dictutil
from pykit import fsutil
from pykit import logutil
from pykit import mysqlutil
from pykit import proc
from pykit import shell
from pykit import utfyaml

logger = logging.getLogger(__name__)

MYSQL_TABLE_NAME_LENGTH = 64
EXEC_DUMP = ['mysqldump']
TMP_DIR = '/tmp'

result_file = os.path.join(TMP_DIR, '{tb}.result.yaml')
shards_file = os.path.join(TMP_DIR, '{tb}.shards.yaml')
dump_cmd_file = os.path.join(TMP_DIR, '{tb}.dump.yaml')

sql_file = os.path.join(TMP_DIR, '{tb}.sql')


def run(conf_path):

    conf = read_conf(conf_path)

    shards = make_sharding(conf)

    db = conf['db']
    src_table = conf['table']
    base_table = conf['basetable']
    shard_flds = conf['shard_fields']

    shard_db = map_shard_db(shards, conf['new_ports'])

    db_tables = make_db_table(base_table, shard_db)

    dump_commands = make_dump_commands(
        shard_flds, shards, conf['source_conn'], db, src_table, base_table)

    exec_dump(dump_commands)

    exec_restore(db_tables, conf['new_conn'], src_table, db)

    shard_filename = write_shards(shard_db, shard_flds, src_table)

    out_message('new dbshard message is on {0}.'.format(shard_filename))


# conf:
# {
#      "table": string,
#      "basetable": string,
#      "database": string,
#      "source_conn": {"host": string, "port": number, "user": string, "passwd": number},
#      "shard_fields": [],
#      "start_shard": [],
#      "number_per_shard": number,
#      "tolerance_of_shard": number,
#
#      "new_conn": {"host": string, "user": string, "passwd": string},
#      "ports": [[4491, 2], [4492, 2]]
# }
def read_conf(conf_path):

    if os.path.isfile(conf_path):
        conf = utfyaml.load(fsutil.read_file(conf_path))

        shards_file_result = shards_file.format(tb=conf['table'])

        if os.path.isfile(shards_file_result):
            shards = utfyaml.load(fsutil.read_file(shards_file_result))

            conf['start_shard'] = shards['shard'][-1]

        return conf

    return {}


def make_sharding(conf):

    def sharding_generator(shard):

        shard_fields = conf['shard_fields']
        basetable = conf['basetable']

        # use basetable name and shard fileds join with '_' to be the new table name
        len_remaining = MYSQL_TABLE_NAME_LENGTH - len(basetable) - 1

        new_shard = []
        for s in shard:

            s = str(s)

            if len(s) >= len_remaining:
                s = s[:len_remaining]
                len_remaining = 0
            else:
                len_remaining -= len(s) + 1

            new_shard.append(s)

        len_shard = len(shard_fields)
        len_new_shard = len(new_shard)
        if len_new_shard < len_shard:
            new_shard += [''] * (len_shard - len_new_shard)

        return tuple(new_shard)

    conf_flds = ['db', 'table', 'shard_fields', 'start_shard', 'number_per_shard','tolerance_of_shard']

    sharding_conf = dictutil.subdict(conf, conf_flds)
    sharding_conf['conn'] = conf.get('source_conn', {})
    sharding_conf['sharding_generator'] = sharding_generator

    shard_filename = shards_file.format(tb=urllib.quote_plus(conf['table']))

    rst_stat = {
        'shard': [],
        'num': [],
        'total': 0,
    }
    if os.path.isfile(shard_filename):
        rst_stat = utfyaml.load(fsutil.read_file(shard_filename))

        sharding_conf['start_shard'] = rst_stat['shard'].pop(-1)

        rst_stat['total'] -= rst_stat['num'][-1]
        rst_stat['num'].pop(-1)

    shard_result = mysqlutil.make_sharding(sharding_conf)

    for k in ('shard', 'num', 'total'):
        rst_stat[k] += shard_result[k]

    fsutil.write_file(shard_filename, utfyaml.dump(rst_stat), atomic=True)

    out_message('get shard successfully on {path}'.format(path=shard_filename))

    return rst_stat['shard']


def map_shard_db(shards, db_info):

    ports = []
    for port, n_shard in db_info:
        ports += [port] * n_shard

    num_shards = len(shards)
    num_ports = len(ports)
    if num_shards > num_ports:
        ports += [ports[-1]] * (num_shards - num_ports)

    return zip(shards, ports)


def make_db_table(basetable, shard_db):

    result = {}

    for shard, port in shard_db:

        if result.get(port) is None:
            result[port] = []

        table_name = new_table_name(basetable, shard)

        result[port].append(table_name)

    return result


def new_table_name(base, parts, joiner='_'):

    table_name = base + joiner + joiner.join(p for p in parts)

    return table_name


def make_dump_commands(shard_fields, shards, conn, db, src_tb, base_tb):

    result = []

    num_shards = len(shards)
    for i in xrange(num_shards):

        start = shards[i]
        if i < num_shards - 1:
            end = shards[i + 1]
        else:
            end = None

        table_name = new_table_name(base_tb, start)

        sql_dump = mysqlutil.make_mysqldump_in_range(
            shard_fields,
            conn,
            db,
            src_tb,
            sql_file.format(tb=urllib.quote_plus(table_name)),
            EXEC_DUMP,
            start,
            end)

        result.append(sql_dump)

    dump_filename = dump_cmd_file.format(tb=urllib.quote_plus(src_tb))

    fsutil.write_file(dump_filename, '\n'.join(result), atomic=True)

    out_message('make dump databases command successfully on: {file}'.format(file=dump_filename))

    return result


def exec_dump(dump_commands):

    for cmd in dump_commands:
        return_code, out, err = proc.shell_script(cmd)

        if return_code != 0:
            logger.error('dump error, return_code: {return_code}, out: {out}, err: {err}, cmd: {cmd}'.format(
                return_code=return_code, out=out, err=err, cmd=cmd))


def exec_restore(db_tables, conn, src_tb, db):

    for port, tables in db_tables.items():
        for table_new in tables:

            sql_path = sql_file.format(tb=urllib.quote_plus(table_new))

            rename_table = (
                "DROP TABLE IF EXISTS `{table_new}`;\n" +
                "ALTER TABLE `{table_old}` RENAME TO `{table_new}`;").format(
                table_old=src_tb,
                table_new=table_new)

            return_code, out, err = proc.shell_script(
                "echo '" + rename_table + "' >> {sql}".format(sql=sql_path))

            if return_code != 0:
                logger.error('add rename table statement to sql failed. cmd: {rename_table}'.format(
                    rename_table=rename_table))
                return

            return_code, out, err = proc.shell_script(
                    'cat {sql} | mysql '
                    '--host={host} '
                    '--port={port} '
                    '--user={user} '
                    '--password={passwd} '
                    '{db}'.format(sql=sql_path, port=port, db=db, **conn)
            )

            if return_code != 0:
                logger.error(('create table {table} error, out: {out}, err: {err}, err_code: ' +
                              '{err_code}').format(table=table_new, out=out, err=err, err_code=return_code))
                out_message('restore_table_by_sql error')


def write_shards(shards_port, shard_fields, table, base_table):

    out_fmt = {}
    result = []
    for shard, port in shards_port:
        out_fmt['from'] = list(shard)
        out_fmt['table'] = new_table_name(base_table, shard)
        out_fmt['db'] = port

        result.append(out_fmt.copy())

    rst_path = result_file.format(tb=urllib.quote_plus(table))

    fsutil.write_file(rst_path, utfyaml.dump(result), atomic=True)

    return rst_path


def out_message(msg):

    logger.info(msg)

    label = '========================='

    print label
    print msg


if __name__ == "__main__":
    logutil.make_logger()

    shell.command(**{

        '__description__': 'use to splite table to shards',

        'run': (run,
              ('conf_path', {'type': str, 'help': 'path of the yaml config.'}),
            ),

        '__add_help__': {
            ('run',) : 'to split a table.',
        },
    })
