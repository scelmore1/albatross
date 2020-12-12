import logging

from Logging.MyLogger import MyLogger
from MongoDB.MongoInitialization import MongoInitialization
from SGRun import SGRun

if __name__ == '__main__':
    main_logger = MyLogger('Main', 'Main/logs/main.log', logging.INFO).getLogger()
    mongo_obj = MongoInitialization()
    sg_run = SGRun(mongo_obj, main_logger)
    res = sg_run.runSG()
    main_logger.info('{}'.format(res))