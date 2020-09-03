#! -*- coding: utf-8 -*-

# o5/2o2o: 0.24.7              :: wro-guys


import logging


url_filetransfer = 'storage.ebay.com/FileTransferService'
url_bulkexchange = 'webservices.ebay.com/BulkDataExchangeService'

api_version = 1149
side_id = 77


def get_logger(name, levelname='info'):
    logger = logging.getLogger(name)
    formatter = logging.Formatter('%(name)s [%(levelname)s]  %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    level = logging.CRITICAL
    if levelname == 'debug':
        level = logging.DEBUG
    if levelname == 'info':
        level = logging.INFO
    if levelname == 'warning':
        level = logging.WARNING
    if levelname == 'error':
        level = logging.ERROR
    logger.setLevel(level)
    return logger
