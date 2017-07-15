from pykit import logutil

logger = logutil.make_logger('/tmp', log_fn='stdlog', level='INFO',
                             fmt='message')


logutil.add_std_handler(logger, 'stdout', fmt='message')
logger.info('stdlog')
