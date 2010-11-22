#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103

"""
Config utilities
"""

import ConfigParser

def fm_config(iconfig=None):
    """
    FileMover configuration
    """
    dbs = iconfig.section_('dbs')
    fmws = iconfig.section_('fmws')
    file_manager = iconfig.section_('file_manager')
    file_lookup = iconfig.section_('file_lookup')
    transfer = iconfig.section_('transfer_wrapper')

    config = ConfigParser.ConfigParser()

    config.add_section('dbs')
    config.set('dbs', 'url', dbs.url)
    config.set('dbs', 'instance', dbs.instance)
    config.set('dbs', 'params', dbs.params)

    config.add_section('fmws')
    config.set('fmws', 'logger_dir', fmws.logger_dir)
    config.set('fmws', 'verbose', str(fmws.verbose))
    config.set('fmws', 'max_transfer', str(fmws.max_transfer))
    config.set('fmws', 'day_transfer', str(fmws.day_transfer))
    config.set('fmws', 'download_area', fmws.download_area)

    config.add_section('file_manager')
    config.set('file_manager', 'base_directory', file_manager.base_directory)
    config.set('file_manager', 'max_size_gb', str(file_manager.max_size_gb))
    config.set('file_manager', 'max_movers', str(file_manager.max_movers))

    config.add_section('transfer_wrapper')
    config.set('transfer_wrapper', 'transfer_command', transfer.transfer_command)

    config.add_section('file_lookup')
    config.set('file_lookup', 'priority_0', file_lookup.priority_0)
    config.set('file_lookup', 'priority_1', file_lookup.priority_1)
    config.set('file_lookup', 'priority_2', file_lookup.priority_2)

    return config

