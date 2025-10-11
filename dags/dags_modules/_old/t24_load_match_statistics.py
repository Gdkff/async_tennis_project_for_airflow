import urllib.request
import json
import datetime


def t24_get_match_statistic(t24_match_id: str) -> list:
    response = urllib.request.Request(f'https://global.flashscore.ninja/107/x/feed/df_st_2_{t24_match_id}',
                                      headers={'x-fsign': 'SW9D1eZo'})
    data = urllib.request.urlopen(response).read().decode('utf-8')
    stats_keys_dict = {'Aces': ('aces',),
                       'Double Faults': ('double_faults',),
                       'Service Points Won': (None, None, 'service_points'),
                       '1st Serve Points Won': (None, 'first_serves_won', 'first_serves_success'),
                       '2nd Serve Points Won': (None, 'second_serves_won', None),
                       'Break Points Converted': (None, 'break_points_converted', 'break_points_created'),
                       'Average 1st Serve Speed': ('average_first_serve_speed_km_h',),
                       'Average 2nd Serve Speed': ('average_second_serve_speed_km_h',),
                       'Winners': ('winners',),
                       'Unforced Errors': ('unforced_errors',),
                       'Net Points Won': (None, 'net_points_won', 'net_approaches')
                       }
    delimiters_dict = {'% (': ('% (', '/', ')'),
                       ' km/h': ' km/h',
                       '': None}
    data = data.split('¬~')
    match_statistic = []
    section, period, key_out, t1_val, t2_val, period_last, statistic = None, None, None, None, None, None, None
    for part in data:
        parts_of_part = part.split('¬')
        period_last = period
        if part[:2] == 'SE':
            period = parts_of_part[0].split('÷')[1]
            period = 0 if period == 'Match' else int(period.replace('Set ', ''))
            statistic = {'t1': {'t24_match_id': t24_match_id,
                                'team_num': 1,
                                'set': period},
                         't2': {'t24_match_id': t24_match_id,
                                'team_num': 2,
                                'set': period}}
        # if part[:2] == 'SF':
        #     section = parts_of_part[0].split('÷')[1]
        if part[:2] == 'SG':
            for part_of_part in parts_of_part:
                key, value = part_of_part.split('÷')
                if key == 'SG':
                    key_out = value
                if key == 'SH':
                    t1_val = value
                if key == 'SI':
                    t2_val = value
            if key_out in stats_keys_dict:
                db_keys = stats_keys_dict[key_out]
                for team_num, team_value in enumerate((t1_val, t2_val), 1):
                    team_line_vals = []
                    for check_val in delimiters_dict:
                        if check_val in team_value:
                            delimiters = delimiters_dict[check_val]
                            if not delimiters:
                                team_line_vals = [int(team_value)]
                                break
                            for delimiter in delimiters:
                                team_value = team_value.replace(delimiter, '#')
                            t = team_value.strip('#')
                            t = t.split('#')
                            team_line_vals = [int(x) for x in t]
                            break
                    result = {k: v for k, v in zip(db_keys, team_line_vals) if k is not None}
                    statistic[f't{team_num}'].update(result)
        if period != period_last or part[:2] == 'A1':
            match_statistic.append(statistic['t1'])
            match_statistic.append(statistic['t2'])
    return match_statistic


t24_get_match_statistic('zXV1Q0qd')
