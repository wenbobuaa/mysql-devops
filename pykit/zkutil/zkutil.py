
import os
import types

from pykit import net

# We assumes that ip does not change during process running.
# Display intra ip if presents, or display pub ip.
host_ip4 = net.ips_prefer(net.get_host_ip4(), net.INN)


def lock_data(node_id):
    """
    Embed lock holder information into the zk node data for the lock.

    `node_id` is a user defined identifier of a host.
    """

    ip = (host_ip4 + ['unknownip'])[0]
    seq = [node_id, ip, str(os.getpid())]

    return '-'.join(seq)


def parse_lock_data(data_str):
    """
    Parse string generated by lock_data()
    """

    node_id, ip, process_id = (data_str.split('-') + ([None] * 3))[:3]

    if type(process_id) in types.StringTypes and process_id.isdigit():
        process_id = int(process_id)
    else:
        process_id = None

    return {
        'node_id': node_id,
        'ip': ip,
        'process_id': process_id,
    }
