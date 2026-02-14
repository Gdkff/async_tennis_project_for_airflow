CREATE DATABASE tennis;

\connect tennis;

CREATE USER tennis_user WITH ENCRYPTED PASSWORD 'tennis_user_strong_password';

GRANT ALL PRIVILEGES ON DATABASE tennis TO tennis_user;

CREATE TABLE IF NOT EXISTS dim_game_pbp ( 
server_points_line text NOT NULL,
points_total int2 NULL,
server_points_won int2 NULL,
server_win bool NULL,
receiver_points_won int2 NULL,
receiver_breakpoints int2 NULL,
receiver_breakpoints_converted int2 NULL,
record_created_at timestamptz NOT NULL,
record_updated_at timestamptz NOT NULL,
CONSTRAINT dim_game_pbp_pk PRIMARY KEY (server_points_line));
ALTER TABLE dim_game_pbp OWNER TO tennis_user;
GRANT INSERT, TRIGGER, SELECT, DELETE, REFERENCES, UPDATE, TRUNCATE ON TABLE dim_game_pbp TO tennis_user;


CREATE TABLE IF NOT EXISTS dim_tiebreak_pbp ( 
server_points_line text NOT NULL,
points_total int2 NULL,
first_server_win int2 NULL,
first_server_points_on_serve_line text NULL,
second_server_points_on_serve_line text NULL,
first_server_points_on_serve_won int2 NULL,
first_server_points_on_receive_won int2 NULL,
second_server_points_on_serve_won int2 NULL,
second_server_points_on_receive_won int2 NULL,
record_created_at timestamptz NOT NULL,
record_updated_at timestamptz NOT NULL,
CONSTRAINT dim_tiebreak_pbp_pk PRIMARY KEY (server_points_line));
ALTER TABLE dim_tiebreak_pbp OWNER TO tennis_user;
GRANT INSERT, TRIGGER, SELECT, DELETE, REFERENCES, UPDATE, TRUNCATE ON TABLE dim_tiebreak_pbp TO tennis_user;


CREATE TABLE IF NOT EXISTS t24_players (
t24_pl_id text NOT NULL,
url text NULL,
full_name text NULL,
country text NULL,
birthday date NULL,
all_data_loaded bool NULL,
record_created_at timestamptz NOT NULL,
record_updated_at timestamptz NOT NULL,
CONSTRAINT t24_players_pk PRIMARY KEY (t24_pl_id));
ALTER TABLE t24_players OWNER TO tennis_user;
GRANT INSERT, TRIGGER, SELECT, DELETE, REFERENCES, UPDATE, TRUNCATE ON TABLE t24_players TO tennis_user;


CREATE TABLE IF NOT EXISTS t24_tournaments (
id int4 NOT NULL,
trn_archive_full_url text NOT NULL,
trn_type text NULL,
trn_name text NULL,
trn_years_loaded bool NULL,
record_updated_at timestamptz NOT NULL,
record_created_at timestamptz NOT NULL,
CONSTRAINT t24_tournaments_pk PRIMARY KEY (id),
CONSTRAINT t24_tournaments_unique UNIQUE (trn_archive_full_url));
ALTER TABLE t24_tournaments OWNER TO airflow;
GRANT INSERT, TRIGGER, SELECT, DELETE, REFERENCES, UPDATE, TRUNCATE ON TABLE t24_tournaments TO airflow;
GRANT INSERT, TRIGGER, SELECT, DELETE, REFERENCES, UPDATE, TRUNCATE ON TABLE t24_tournaments TO tennis_user;


CREATE TABLE IF NOT EXISTS t24_tournaments_years ( 
id int4 NOT NULL,
trn_id int4 NULL,
trn_year int4 NULL,
first_draw_id text NULL,
qual_draw_id text NULL,
main_draw_id text NULL,
draws_id_loaded bool NULL,
record_updated_at timestamptz NULL,
record_created_at timestamptz NULL,
trn_results_loaded bool NULL,
CONSTRAINT t24_tournaments_years_pk PRIMARY KEY (id),
CONSTRAINT t24_tournaments_years_unique UNIQUE (trn_id, trn_year));
ALTER TABLE t24_tournaments_years OWNER TO airflow;
GRANT INSERT, TRIGGER, SELECT, DELETE, REFERENCES, UPDATE, TRUNCATE ON TABLE t24_tournaments_years TO airflow;
GRANT INSERT, TRIGGER, SELECT, DELETE, REFERENCES, UPDATE, TRUNCATE ON TABLE t24_tournaments_years TO tennis_user;
ALTER TABLE public.t24_tournaments_years ADD CONSTRAINT t24_tournaments_years_t24_tournaments_fk FOREIGN KEY (trn_id) REFERENCES t24_tournaments(id);


CREATE TABLE IF NOT EXISTS t24_matches ( 
t24_match_id text NOT NULL,
trn_year_id int4 NOT NULL,
is_qualification bool NULL,
match_start timestamptz NULL,
match_finish timestamptz NULL,
match_status_short text NULL,
match_status text NULL,
t1_pl1_id text NULL,
t1_pl2_id text NULL,
t2_pl1_id text NULL,
t2_pl2_id text NULL,
team_winner int2 NULL,
match_score text NULL,
t1_sets_won int2 NULL,
t2_sets_won int2 NULL,
t1_s1_score int2 NULL,
t1_s1_score_tiebreak int2 NULL,
t2_s1_score int2 NULL,
t2_s1_score_tiebreak int2 NULL,
t1_s2_score int2 NULL,
t1_s2_score_tiebreak int2 NULL,
t2_s2_score int2 NULL,
t2_s2_score_tiebreak int2 NULL,
t1_s3_score int2 NULL,
t1_s3_score_tiebreak int2 NULL,
t2_s3_score int2 NULL,
t2_s3_score_tiebreak int2 NULL,
t1_s4_score int2 NULL,
t1_s4_score_tiebreak int2 NULL,
t2_s4_score int2 NULL,
t2_s4_score_tiebreak int2 NULL,
t1_s5_score int2 NULL,
t1_s5_score_tiebreak int2 NULL,
t2_s5_score int2 NULL,
t2_s5_score_tiebreak int2 NULL,
match_status_short_code int2 NULL,
match_status_code int2 NULL,
match_url text NULL,
final_pbp_data_loaded bool NULL,
record_created_at timestamptz NOT NULL,
record_updated_at timestamptz NOT NULL,
final_statistics_loaded bool NULL,
CONSTRAINT t24_matches_pk PRIMARY KEY (t24_match_id));
ALTER TABLE t24_matches OWNER TO tennis_user;
GRANT INSERT, TRIGGER, SELECT, DELETE, REFERENCES, UPDATE, TRUNCATE ON TABLE t24_matches TO tennis_user;
ALTER TABLE public.t24_matches ADD CONSTRAINT t24_matches_t24_players_fk FOREIGN KEY (t1_pl1_id) REFERENCES t24_players(t24_pl_id);
ALTER TABLE public.t24_matches ADD CONSTRAINT t24_matches_t24_players_fk_1 FOREIGN KEY (t1_pl2_id) REFERENCES t24_players(t24_pl_id);
ALTER TABLE public.t24_matches ADD CONSTRAINT t24_matches_t24_players_fk_2 FOREIGN KEY (t2_pl1_id) REFERENCES t24_players(t24_pl_id);
ALTER TABLE public.t24_matches ADD CONSTRAINT t24_matches_t24_players_fk_3 FOREIGN KEY (t2_pl2_id) REFERENCES t24_players(t24_pl_id);
ALTER TABLE public.t24_matches ADD CONSTRAINT t24_matches_t24_tournaments_years_fk FOREIGN KEY (trn_year_id) REFERENCES t24_tournaments_years(id);


CREATE TABLE IF NOT EXISTS t24_matches_defective ( 
t24_match_id text NOT NULL,
match_url text NULL,
trn_year_id int4 NULL,
is_qualification bool NULL,
match_start timestamptz NULL,
match_finish timestamptz NULL,
match_status_short text NULL,
match_status text NULL,
t1_pl1_id text NULL,
t1_pl2_id text NULL,
t2_pl1_id text NULL,
t2_pl2_id text NULL,
team_winner int2 NULL,
match_score text NULL,
t1_sets_won int2 NULL,
t2_sets_won int2 NULL,
t1_s1_score int2 NULL,
t1_s1_score_tiebreak int2 NULL,
t2_s1_score int2 NULL,
t2_s1_score_tiebreak int2 NULL,
t1_s2_score int2 NULL,
t1_s2_score_tiebreak int2 NULL,
t2_s2_score int2 NULL,
t2_s2_score_tiebreak int2 NULL,
t1_s3_score int2 NULL,
t1_s3_score_tiebreak int2 NULL,
t2_s3_score int2 NULL,
t2_s3_score_tiebreak int2 NULL,
t1_s4_score int2 NULL,
t1_s4_score_tiebreak int2 NULL,
t2_s4_score int2 NULL,
t2_s4_score_tiebreak int2 NULL,
t1_s5_score int2 NULL,
t1_s5_score_tiebreak int2 NULL,
t2_s5_score int2 NULL,
t2_s5_score_tiebreak int2 NULL,
match_status_short_code int2 NULL,
match_status_code int2 NULL,
final_pbp_data_loaded bool NULL,
record_created_at timestamptz NOT NULL,
record_updated_at timestamptz NOT NULL,
CONSTRAINT t24_matches_defective_pk PRIMARY KEY (t24_match_id));
ALTER TABLE t24_matches_defective OWNER TO tennis_user;
GRANT INSERT, TRIGGER, SELECT, DELETE, REFERENCES, UPDATE, TRUNCATE ON TABLE t24_matches_defective TO tennis_user;
ALTER TABLE public.t24_matches_defective ADD CONSTRAINT t24_matches_defective_t24_players_fk FOREIGN KEY (t1_pl1_id) REFERENCES t24_players(t24_pl_id);
ALTER TABLE public.t24_matches_defective ADD CONSTRAINT t24_matches_defective_t24_players_fk_1 FOREIGN KEY (t1_pl2_id) REFERENCES t24_players(t24_pl_id);
ALTER TABLE public.t24_matches_defective ADD CONSTRAINT t24_matches_defective_t24_players_fk_2 FOREIGN KEY (t2_pl1_id) REFERENCES t24_players(t24_pl_id);
ALTER TABLE public.t24_matches_defective ADD CONSTRAINT t24_matches_defective_t24_players_fk_3 FOREIGN KEY (t2_pl2_id) REFERENCES t24_players(t24_pl_id);
ALTER TABLE public.t24_matches_defective ADD CONSTRAINT t24_matches_defective_t24_tournaments_years_fk FOREIGN KEY (trn_year_id) REFERENCES t24_tournaments_years(id);


CREATE TABLE IF NOT EXISTS t24_game_pbp ( 
t24_match_id text NOT NULL,
"set" int2 NOT NULL,
game int2 NOT NULL,
"server" int2 NOT NULL,
server_game_points_line text NULL,
server_tiebreak_points_line text NULL,
record_created_at timestamptz NOT NULL,
record_updated_at timestamptz NOT NULL,
CONSTRAINT t24_game_pbp_unique UNIQUE (t24_match_id, set, game));
ALTER TABLE t24_game_pbp OWNER TO tennis_user;
GRANT INSERT, TRIGGER, SELECT, DELETE, REFERENCES, UPDATE, TRUNCATE ON TABLE t24_game_pbp TO tennis_user;
ALTER TABLE public.t24_game_pbp ADD CONSTRAINT t24_game_pbp_dim_game_pbp_fk FOREIGN KEY (server_game_points_line) REFERENCES dim_game_pbp(server_points_line);
ALTER TABLE public.t24_game_pbp ADD CONSTRAINT t24_game_pbp_dim_tiebreak_pbp_fk FOREIGN KEY (server_tiebreak_points_line) REFERENCES dim_tiebreak_pbp(server_points_line);
ALTER TABLE public.t24_game_pbp ADD CONSTRAINT t24_game_pbp_t24_matches_fk FOREIGN KEY (t24_match_id) REFERENCES t24_matches(t24_match_id);


CREATE TABLE IF NOT EXISTS t24_set_statistics ( 
t24_match_id text NOT NULL,
team_num int2 NOT NULL,
"set" int2 NOT NULL,
aces int2 NULL,
double_faults int2 NULL,
service_points int2 NULL,
first_serves_won int2 NULL,
first_serves_success int2 NULL,
second_serves_won int2 NULL,
break_points_created int2 NULL,
break_points_converted int2 NULL,
average_first_serve_speed_km_h float8 NULL,
average_second_serve_speed_km_h float8 NULL,
winners int2 NULL,
unforced_errors int2 NULL,
net_approaches int2 NULL,
net_points_won int2 NULL,
record_created_at timestamptz NOT NULL,
record_updated_at timestamptz NOT NULL,
CONSTRAINT t24_set_statistics_unique UNIQUE (t24_match_id, team_num, set));
ALTER TABLE t24_set_statistics OWNER TO tennis_user;
GRANT INSERT, TRIGGER, SELECT, DELETE, REFERENCES, UPDATE, TRUNCATE ON TABLE t24_set_statistics TO tennis_user;
ALTER TABLE public.t24_set_statistics ADD CONSTRAINT t24_set_statistics_t24_matches_fk FOREIGN KEY (t24_match_id) REFERENCES t24_matches(t24_match_id);


CREATE TABLE IF NOT EXISTS public.atp_players (
atp_pl_id text not null,
last_name text null,
first_name text null,
country text null,
birthday date null,
birthplace text null,
weight numeric(9,2) null,
height numeric(9,2) null,
play_hand text null,
back_hand text null,
coach text null,
pl_data_loaded bool null,
record_created_at timestamp not null,
record_updated_at timestamp not null,
CONSTRAINT atp_players_pk PRIMARY KEY (atp_pl_id)
);
ALTER TABLE atp_players OWNER TO tennis_user;
GRANT SELECT, TRUNCATE, INSERT, REFERENCES, DELETE, UPDATE, TRIGGER ON TABLE atp_players TO tennis_user;


CREATE TABLE IF NOT EXISTS public.atp_ranking_dates (
ranking_type CHAR not null,
week_date DATE not null,
url TEXT not null,
count_rows_on_page INT null,
count_rows_loaded INT null,
loaded BOOL null,
record_created_at timestamp not null,
record_updated_at timestamp not null
);
ALTER TABLE atp_ranking_dates OWNER TO tennis_user;
GRANT SELECT, TRUNCATE, INSERT, REFERENCES, DELETE, UPDATE, TRIGGER ON TABLE atp_ranking_dates TO tennis_user;
ALTER TABLE public.atp_ranking_dates ADD CONSTRAINT atp_ranking_dates_unique UNIQUE (week_date,ranking_type);


CREATE TABLE IF NOT EXISTS public.atp_ranking_singles (
week_date DATE not null,
rank INT not null,
atp_pl_id TEXT not null,
points INT not null,
trn_played INT null,
record_created_at timestamp not null,
record_updated_at timestamp not null
);
ALTER TABLE atp_ranking_singles OWNER TO tennis_user;
GRANT SELECT, TRUNCATE, INSERT, REFERENCES, DELETE, UPDATE, TRIGGER ON TABLE atp_ranking_singles TO tennis_user;
ALTER TABLE public.atp_ranking_singles ADD CONSTRAINT atp_ranking_singles_atp_players_fk FOREIGN KEY (atp_pl_id) REFERENCES public.atp_players(atp_pl_id);
ALTER TABLE public.atp_ranking_singles ADD CONSTRAINT atp_ranking_singles_unique UNIQUE (week_date,atp_pl_id);


CREATE TABLE IF NOT EXISTS public.atp_ranking_doubles (
week_date DATE not null,
rank INT not null,
atp_pl_id TEXT not null,
points INT not null,
trn_played INT null,
record_created_at timestamp not null,
record_updated_at timestamp not null
);
ALTER TABLE atp_ranking_doubles OWNER TO tennis_user;
GRANT SELECT, TRUNCATE, INSERT, REFERENCES, DELETE, UPDATE, TRIGGER ON TABLE atp_ranking_doubles TO tennis_user;
ALTER TABLE public.atp_ranking_doubles ADD CONSTRAINT atp_ranking_doubles_atp_players_fk FOREIGN KEY (atp_pl_id) REFERENCES public.atp_players(atp_pl_id);
ALTER TABLE public.atp_ranking_doubles ADD CONSTRAINT atp_ranking_doubles_unique UNIQUE (week_date,atp_pl_id);


CREATE TABLE IF NOT EXISTS public.atp_tournaments ( 
atp_trn_id text NOT NULL,
trn_year int4 NOT NULL,
tour_type text NOT NULL,
trn_name text NULL,
trn_start_date date NOT NULL,
trn_end_date date NULL,
trn_city text NULL,
trn_country text NULL,
singles_main_draw_loaded bool NULL,
singles_qualification_draw_loaded bool NULL,
doubles_main_draw_loaded bool NULL,
doubles_qualification_draw_loaded bool NULL,
results_loaded bool NULL, draws_count int2 NULL,
record_created_at timestamp NOT NULL,
record_updated_at timestamp NOT NULL,
CONSTRAINT atp_tournaments_pk PRIMARY KEY (atp_trn_id, trn_year, trn_start_date)
);
ALTER TABLE atp_tournaments OWNER TO tennis_user;
GRANT TRIGGER, TRUNCATE, REFERENCES, SELECT, INSERT, DELETE, UPDATE ON TABLE atp_tournaments TO tennis_user;