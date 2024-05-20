#-------------------------------------------------------------------------#
# SyncNet                                                     by Bob Hood #
#                                                 Copyright 2024 Bob Hood #
#                                                                         #
# Module: main                                                            #
#                                                                         #
# Entry point for the SyncNet system                                      #
#-------------------------------------------------------------------------#

import os
import re
import sys
import random
import locale
import asyncio
import configparser

from Source     import Source
from Sink       import Sink
from Settings   import Settings

try:
    import textual
    from TUI import TUI
    HAVE_TUI = True
except ModuleNotFoundError:
    HAVE_TUI = False

from argparse   import ArgumentParser

from loguru     import logger   # https://pypi.org/project/loguru/

__author__     = "Bob Hood"
__copyright__  = "Copyright (C) 2024 Bob Hood"
__version__    = "1.0"
__maintainer__ = "Bob Hood"
__email__      = "Bob.Hood.2@gmail.com"
__status__     = "Prototype"

# https://packaging.python.org/en/latest/overview/
if __name__ == "__main__":
    # initialize global settings

    def warn_about_security():
        logger.warning('You are operating without security;')
        logger.warning('Data will be exchanged in the clear')

    locale.setlocale(locale.LC_ALL, '')

    parser = ArgumentParser(description="SyncNet")
    parser.add_argument("-s", "--source", metavar="ROOT", default=None, type=str, help="Function as a Source, monitoring the ROOT path")
    parser.add_argument("-S", "--sink", metavar="ROOT", default=None, type=str, help="Function as a Sink, updating the ROOT path")
    parser.add_argument("-a", "--address", metavar="ADDR", default=None, type=str, help="IP address to use for the multicast group")
    parser.add_argument("-p", "--port", metavar="PORT", default=None, type=int, help="Port to use for the multicast group")
    parser.add_argument("-c", "--certificate", metavar="FILE", default=None, type=str, help="Certificate file to use with a Source")
    parser.add_argument("-k", "--keyfile", metavar="FILE", default=None, type=str, help="Certificate key file to use with a Source")
    parser.add_argument("-P", "--passphrase", metavar="STR", default=None, type=str, help="Passphrase to use for endpoint encryption")
    parser.add_argument("-r", "--retry", metavar="SEC", default=None, type=int, help="Timeout before retrying a connection to the Source")
    parser.add_argument("-l", "--log", metavar="FILE", default=None, type=str, help="Path to log file")
    parser.add_argument("-L", "--level", metavar="{DEBUG, INFO, WARNING, ERROR, CRITICAL}", default="INFO", type=str, help="Log level to capture")
    parser.add_argument("-i", "--include", action="append", metavar="FILTER", default=None, help="Add items to the inlude filter list and exit")
    parser.add_argument("-e", "--exclude", action="append", metavar="FILTER", default=None, help="Add items to the exclude filter list and exit")
    parser.add_argument("--tui", action="store_true", default=False, help="Run in the console using Textual")
    parser.add_argument("--set", metavar="ID VALUE", default=None, type=str, help="Set a new, or replace an existing, value in settings file")
    parser.add_argument("--clear", metavar="ID", default=None, type=str, help="Clear an existing identified value in the settings file")
    parser.add_argument("--list", action="store_true", default=False, help="List all currently cached settings")
    parser.add_argument("--init", metavar="MODE", default=None, type=str, help="One-time initialization of the system with critical settings")

    options, args = parser.parse_known_args()

    if options.level.upper() not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
        logger.critical(f"Provided level '{options.level}' is invalid")
        sys.exit(-1)

    tui = None
    logger.remove(0)
    if HAVE_TUI and options.tui:
        tui = TUI()
        logger.add(tui, level=options.level.upper())
    else:
        logger.add(sys.stderr, level=options.level.upper())

    # if the format becomes more complicated, switch to JSON
    config = configparser.ConfigParser()
    if sys.platform == 'linux':
        config_folder = os.path.join(os.environ['HOME'], '.config', 'SyncNet')
        if not os.path.exists(config_folder):
            os.makedirs(config_folder)
        config_file = os.path.join(config_folder, 'SyncNet.ini')
        if os.path.exists(config_file):
            config.read(config_file)
        else:
            config['SyncNet'] = {}

    if options.init is not None:
        # generate a unique id for this instance
        id = str(random.getrandbits(32))  # 4 bytes
        match options.init:
            case 'source':
                config['SyncNet']['source_id'] = id
            case 'sink':
                config['SyncNet']['sink_id'] = id

        with open(config_file, 'w') as configfile:    # save
            config.write(configfile)

        sys.exit(0)

    # fix up include/exclude filters first
    if (options.include is not None) or (options.exclude is not None):
        if (options.include is not None) and ('excludes' in config['SyncNet']):
            # see if any of the includes in the CLI list are in the exclude list
            filters = config['SyncNet']['excludes'].split(',')
            filters_to_remove = []
            for filter in options.include:
                if filter in filters:
                    filters_to_remove.append(filter)

            options.exclude = [filter for filter in filters if filter not in filters_to_remove]
            config['SyncNet']['excludes'] = ','.join(options.exclude)
            options.include = [filter for filter in options.include if filter not in filters_to_remove]

        if (options.exclude is not None) and ('includes' in config['SyncNet']):
            # see if any of the excludes in the CLI list are in the include list
            filters = config['SyncNet']['includes'].split(',')
            filters_to_remove = []
            for filter in options.exclude:
                if filter in filters:
                    filters_to_remove.append(filter)

            options.include = [filter for filter in options.include if filter not in filters_to_remove]
            config['SyncNet']['includes'] = ','.join(options.include)
            options.exclude = [filter for filter in options.exclude if filter not in filters_to_remove]

        # anything left in either list gets added to the config file

        if (options.include is not None) and len(options.include) > 0:
            includes = set()
            if 'includes' in config['SyncNet']:
                includes = set(config['SyncNet']['includes'].split(','))
            for filter in options.include:
                includes.add(filter)
            config['SyncNet']['includes'] = ','.join(includes)

        if (options.exclude is not None) and len(options.exclude) > 0:
            excludes = set()
            if 'excludes' in config['SyncNet']:
                excludes = set(config['SyncNet']['excludes'].split(','))
            for filter in options.exclude:
                excludes.add(filter)
            config['SyncNet']['excludes'] = ','.join(excludes)

        with open(config_file, 'w') as configfile:    # save
            config.write(configfile)

        sys.exit(0)

    if options.list or \
       (options.set is not None) or \
       (options.clear is not None):
        if options.list:
            for key in config['SyncNet']:
                logger.info(f"{key}: {config['SyncNet'][key]}")

            if ('certificate' not in config['SyncNet']) and ('passphrase' not in config['SyncNet']):
                warn_about_security()
        else:
            if options.set is not None:
                config['SyncNet'][options.set] = args[0]
            elif options.clear is not None:
                assert options.clear in config['SyncNet'], f"Option {options.clear} does not exist in the configuration file"
                del config['SyncNet'][options.clear]

            with open(config_file, 'w') as configfile:    # save
                config.write(configfile)

        sys.exit(0)

    settings_dict = {}
    if sys.platform == 'linux':
        settings_dict['config_folder'] = config_folder

    if 'SyncNet' in config:
        for key in config['SyncNet']:
            if (key == 'includes') or (key == 'excludes'):
                settings_dict[key] = []
                for filter in config['SyncNet'][key].split(','):
                    if filter.startswith('r::'):
                        # this is a regular expression; make sure it is valid
                        try:
                            rx = re.compile(filter[3:])
                            settings_dict[key].append(rx)
                        except Exception:
                            logger.critical(f"Regex filter '{filter[3:]}' failed to compile")
                            sys.exit(1)
                    else:
                        settings_dict[key].append(filter)
            else:
                settings_dict[key] = config['SyncNet'][key]

    # do options last so command-line args override settings file values
    for key in options.__dict__:
        if options.__dict__[key] is not None:
            settings_dict[key] = options.__dict__[key]

    if 'address' not in settings_dict:
        # lock it to the loopback just for safety
        settings_dict['address'] = '127.0.0.1'
    if 'retry' not in settings_dict:
        settings_dict['retry'] = 60

    settings = Settings(settings_dict)

    assert (('includes' in settings) and ('excludes' not in settings)) or \
           (('includes' not in settings) and ('excludes' in settings)) or \
           (('includes' not in settings) and ('excludes' not in settings)), \
           "Includes and Excludes are mutually exclusive; only one may be defined"
    assert ('source' in settings) or ('sink' in settings), "You must select an endpoint mode"
    # https://www.computernetworkingnotes.com/networking-tutorials/ipv6-multicast-addresses-explained.html
    assert 'address' in settings, "You must specify a Source address"
    assert 'port' in settings, "You must specify a Source port"

    if ('certificate' in settings) and (not settings.keyfile):
        logger.critical('Certificate specified without key file')
        sys.exit(-1)
    elif ('keyfile' in settings) and (not settings.certificate):
        logger.critical('Key file specified without certificate')
        sys.exit(-1)

    if ('passphrase' not in settings) and ('certificate' not in settings):
        warn_about_security()

    endpoint = None

    if 'source' in settings:
        assert os.path.exists(settings.source), f"The path '{settings.source}' does not exist"
        if 'source_id' not in settings:
            logger.critical('Permanent Source id was not found; did you forget to run --init?')
            sys.exit(-1)
        endpoint = Source(settings)

    if 'sink' in settings:
        assert os.path.exists(settings.sink), f"The path '{settings.sink}' does not exist"
        if 'sink_id' not in settings:
            logger.critical('Permanent Sink id was not found; did you forget to run --init?')
            sys.exit(-1)
        endpoint = Sink(settings)

    if HAVE_TUI and options.tui:
        tui.set_task(endpoint)
        tui.set_mode(mode="Source" if 'source' in settings else "Sink")
        tui.run()
    else:
        try:
            asyncio.run(endpoint.execute()) # this won't return until processing is interrupted
        except KeyboardInterrupt:
            logger.info('SyncNet terminated')
