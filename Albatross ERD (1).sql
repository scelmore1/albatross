CREATE TABLE `golfer` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `full_name` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL,
  `pga_id` int NOT NULL,
  `country` varchar(255)
);

CREATE TABLE `tournament` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `pga_id` int NOT NULL,
  `start_date` datetime,
  `end_date` datetime,
  `stats_tournament_id` int
);

CREATE TABLE `tournamentRound` (
  `tournament_id` int,
  `round_num` int,
  `time` datetime,
  `weather` varchar(255),
  `stats_round_id` int,
  PRIMARY KEY (`tournament_id`, `round_num`)
);

CREATE TABLE `golferTournament` (
  `golfer_id` int,
  `tournament_id` int,
  `stat_tournament_id` int,
  PRIMARY KEY (`golfer_id`, `tournament_id`)
);

CREATE TABLE `golferRound` (
  `golfer_id` int,
  `tournament_id` int,
  `round_num` int,
  `stat_round_id` int,
  PRIMARY KEY (`golfer_id`, `tournament_id`, `round_num`)
);

CREATE TABLE `FieldStatTournament` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `stats` varchar(255) NOT NULL
);

CREATE TABLE `FieldStatRound` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `stats` varchar(255) NOT NULL
);

CREATE TABLE `individualStatTournament` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `stats` varchar(255) NOT NULL
);

CREATE TABLE `individualStatRound` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `stats` varchar(255) NOT NULL
);

ALTER TABLE `tournament` ADD FOREIGN KEY (`stats_tournament_id`) REFERENCES `FieldStatTournament` (`id`);

ALTER TABLE `tournamentRound` ADD FOREIGN KEY (`tournament_id`) REFERENCES `tournament` (`id`);

ALTER TABLE `tournamentRound` ADD FOREIGN KEY (`stats_round_id`) REFERENCES `FieldStatRound` (`id`);

ALTER TABLE `golferTournament` ADD FOREIGN KEY (`golfer_id`) REFERENCES `golfer` (`id`);

ALTER TABLE `golferTournament` ADD FOREIGN KEY (`tournament_id`) REFERENCES `tournament` (`id`);

ALTER TABLE `golferTournament` ADD FOREIGN KEY (`stat_tournament_id`) REFERENCES `individualStatTournament` (`id`);

ALTER TABLE `golferRound` ADD FOREIGN KEY (`golfer_id`) REFERENCES `golferTournament` (`golfer_id`);

ALTER TABLE `golferRound` ADD FOREIGN KEY (`tournament_id`) REFERENCES `tournamentRound` (`tournament_id`);

ALTER TABLE `golferRound` ADD FOREIGN KEY (`round_num`) REFERENCES `tournamentRound` (`round_num`);

ALTER TABLE `golferRound` ADD FOREIGN KEY (`stat_round_id`) REFERENCES `individualStatRound` (`id`);
