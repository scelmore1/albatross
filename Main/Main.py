import concurrent.futures
import logging

import pandas as pd

from DataScraping.TournamentRun import TournamentRun


def initLogger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    # remove existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()
    # create handlers
    file_handler = logging.FileHandler('Main/main.log', mode='w+')
    console_handler = logging.StreamHandler()
    # define custom formatter
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s:\t%(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    # assign handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


if __name__ == '__main__':
    main_logger = initLogger()
    tournament_details = pd.read_csv('tournaments/TournamentList.csv')
    tournaments = tournament_details.apply(lambda row: TournamentRun(row['Name'], row['Year']), axis=1).tolist()
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for i, tournament in enumerate(tournaments):
            futures.append(executor.submit(tournament.createMongoDBCollectionsFromScrape))
        for future in concurrent.futures.as_completed(futures):
            main_logger.info('{}'.format(future.result()))

    combined_collections = []
    for tournament in tournaments:
        tournament_dict = {'Tournament Name': tournament.name,
                           'Year': tournament.year,
                           'Player Rounds': tournament.mongo_collections[0],
                           'Player Metadata': tournament.mongo_collections[1],
                           'Course Metadata': tournament.mongo_collections[2],
                           'Tournament Details': tournament.mongo_collections[3]}
        combined_collections.append(tournament_dict)