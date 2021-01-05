import concurrent.futures
import itertools
import logging

import pandas as pd

from Logging.MyLogger import MyLogger
from MongoDB.MongoDownload import MongoDownload
from MongoDB.MongoInitialization import MongoInitialization
from TournamentRun import TournamentRun

# tournaments_path = 'tournaments/FailedTournamentList.csv'
tournaments_path = 'tournaments/TournamentList.csv'

if __name__ == '__main__':
    max_drivers = 2
    main_logger = MyLogger('Main', 'Main/logs/main.log', logging.INFO).getLogger()
    mongo_obj = MongoInitialization('scraper')
    tournament_df = pd.read_csv(tournaments_path, delimiter=',')
    tournament_df.columns = tournament_df.columns.str.strip()
    tournament_df['Name'] = tournament_df['Name'].str.strip()
    mongo_download = MongoDownload(mongo_obj)
    tournaments_scraped = mongo_download.getTournamentsScraped()
    filter_tournaments = tournament_df[~tournament_df[['Name', 'Year']].apply(tuple, 1).isin(tournaments_scraped)]
    tournaments = filter_tournaments.apply(lambda row: TournamentRun(row[0], row[1], mongo_obj, main_logger),
                                           axis=1).tolist()
    iter_tournaments = iter(tournaments)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_drivers) as executor:
        # Only schedule max_drivers amount of futures to start
        futures = {
            executor.submit(tournament.runTournament, None, True): tournament
            for tournament in itertools.islice(iter_tournaments, max_drivers)
        }

        while futures:
            # Wait for the next future to complete.
            finished, _ = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )

            for future in finished:
                # get the completed tournament
                completed_tournament = futures.pop(future)
                main_logger.info('{}'.format(future.result()))

            for tournament in itertools.islice(iter_tournaments, len(finished)):
                future = executor.submit(tournament.runTournament, None, True)
                futures[future] = tournament

    failed_scrape_df = pd.DataFrame(columns=['Name', 'Year'], data=tournaments[0].failed_scrape_list)
    failed_scrape_df.to_csv('tournaments/FailedTournamentList.csv', index=False, header=True)