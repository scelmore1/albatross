import concurrent.futures
import itertools
import logging

import pandas as pd

from DataScraping.TournamentRun import TournamentRun
from Logging.MyLogger import MyLogger

tournaments_path = 'tournaments/TournamentList.csv'

if __name__ == '__main__':
    max_drivers = 2
    main_logger = MyLogger(__name__, 'Main/logs/main.log', logging.INFO).getLogger()
    tournament_details = pd.read_csv(tournaments_path, skipinitialspace=True)
    tournament_details.columns = tournament_details.columns.str.strip()
    tournaments = tournament_details.apply(lambda row: TournamentRun(row['Name'].strip(), row['Year'], main_logger),
                                           axis=1).tolist()
    iter_tournaments = iter(tournaments)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_drivers) as executor:
        # Only schedule max_drivers amount of futures to start
        futures = {
            executor.submit(tournament.runTournament, None, False): tournament
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
                if tournaments.index(tournament) >= (len(tournaments) - max_drivers):
                    future = executor.submit(tournament.runTournament, completed_tournament.getDriverObj(), True)
                else:
                    future = executor.submit(tournament.runTournament, completed_tournament.getDriverObj(), False)
                futures[future] = tournament