class T24matchPBPparser:
    @staticmethod
    def __parse_game_points(points: list, server: int, winner: int) -> list:
        game_data = []
        server -= 1
        winner -= 1
        receiver = 0 if server == 1 else 1
        for point_num, point in enumerate(points):
            if point_num == 0:
                game_data.append(1 if point[server] > point[receiver] else 0)
            elif point[receiver] == 50:
                game_data.append(0)
            elif point[server] == 40 and point[receiver] == 40 and points[point_num - 1][receiver] == 50:
                game_data.append(1)
            else:
                game_data.append(1 if point[server] > points[point_num - 1][server] else 0)
        game_data.append(1 if winner == server else 0)
        return game_data

    @staticmethod
    def __parse_tiebreak(points: list, t24_match_id: str, set_num: int):
        # print(points)
        first_server_team = points[0][2] - 1
        tiebreak_points_list = []
        first_server_points = []
        second_server_points = []
        for point_num, point in enumerate(points):
            server = point[2] - 1
            receiver = 1 if server == 0 else 0
            if point_num == 0:
                server_point = 1 if point[server] > point[receiver] else 0
            else:
                server_point = 1 if point[server] > points[point_num - 1][server] else 0
            if first_server_team == server:
                first_server_points.append(server_point)
            else:
                second_server_points.append(server_point)
            tiebreak_points_list.append(server_point)
        # print(tiebreak_points_list)
        first_server_points_on_serve_won = sum(first_server_points)
        first_server_points_on_receive_won = sum([1 if x == 0 else 0 for x in second_server_points])
        second_server_points_on_serve_won = sum(second_server_points)
        second_server_points_on_receive_won = sum([1 if x == 0 else 0 for x in first_server_points])
        points_total = len(tiebreak_points_list)
        tiebreak_pbp_data = {
            't24_match_id': t24_match_id,
            'set': set_num,
            'game': 13,
            'server': first_server_team + 1,
            'server_game_points_line': ''.join([str(x) for x in tiebreak_points_list]),
            'points_total': points_total,
            'first_server_win': True if first_server_points[-1] == 1 else False,
            'first_server_points_on_serve_str': ''.join([str(x) for x in first_server_points]),
            'second_server_points_on_serve_str': ''.join([str(x) for x in second_server_points]),
            'first_server_points_on_serve_won': first_server_points_on_serve_won,
            'first_server_points_on_receive_won': first_server_points_on_receive_won,
            'second_server_points_on_serve_won': second_server_points_on_serve_won,
            'second_server_points_on_receive_won': second_server_points_on_receive_won
        }
        return tiebreak_pbp_data

    def __parse_pbp_game_line(self, t24_match_id: str, line_dict: dict,
                              set_num: int, game_num: int, t1_bp: list, t2_bp: list):
        # print('serve:', line_dict['HG'], 'winner:', line_dict['HK'], 'T1:', line_dict['HC'], 'T2', line_dict['HE'])
        server = int(line_dict['HG'])
        winner = int(line_dict['HK'])
        game_pbp = line_dict['HL']
        point_num = 1
        points = []
        receiver_breakpoints = 0
        for point in game_pbp.split(', '):
            game_points = point.split(':')
            if line_dict['HG'] == '1':
                if '|B1|' in point:
                    t2_bp.append((set_num, game_num, point_num))
                    receiver_breakpoints += 1
            elif line_dict['HG'] == '2':
                if '|B1|' in point:
                    t1_bp.append((set_num, game_num, point_num))
                    receiver_breakpoints += 1
            points.append((int(game_points[0].replace('A', '50')),
                           int(game_points[1].replace(' |B1|', '').replace(' |B2|', '').replace(' |B3|', '').replace('A', '50').strip())))
            # print(f'*{point}*')
            point_num += 1
        server_points_list = self.__parse_game_points(points, server, winner)
        server_points_str = ''.join([str(x) for x in server_points_list])
        server_points_won = sum(server_points_list)
        points_total = len(server_points_list)
        receiver_points_won = points_total - server_points_won
        server_win = server == winner
        receiver_breakpoints_converted = 1 if not server_win else 0
        game_pbp_data = {
            't24_match_id': t24_match_id,
            'set': set_num,
            'game': game_num,
            'server': server,
            'server_game_points_line': server_points_str,
            'points_total': points_total,
            'server_points_won': server_points_won,
            'server_win': server_win,
            'receiver_points_won': receiver_points_won,
            'receiver_breakpoints': receiver_breakpoints,
            'receiver_breakpoints_converted': receiver_breakpoints_converted
        }
        return game_pbp_data

    def parse_t24_pbp_string(self, t24_pbp: str, t24_match_id: str):
        pbp_split = t24_pbp.split('¬~')
        game_num = 1
        t1_bp = []
        t2_bp = []
        tiebreak_points = []
        set_num = 1
        match_pbp_data_out = []
        for line in pbp_split:
            # print(line)
            line_split = line.split('¬')
            line_dict = {}
            set_or_tb = 'set'
            for line_part in line_split:
                if '÷' in line_part:
                    key, value = line_part.split('÷')
                    line_dict[key] = value
            if line[:2] == 'HA':
                if tiebreak_points:
                    match_pbp_data_out.append(self.__parse_tiebreak(tiebreak_points, t24_match_id, set_num))
                    tiebreak_points = []
                set_or_tb = 'set'
                set_num = int(line_dict['HB'].split(' - ')[-1].split()[-1])
                # print('HA', set_or_tb)
                game_num = 1
            elif line[:2] == 'A1':
                if tiebreak_points:
                    match_pbp_data_out.append(self.__parse_tiebreak(tiebreak_points, t24_match_id, set_num))
                    tiebreak_points = []
            elif line[:2] == 'HB':
                set_or_tb = 'tiebreak'
                set_num = int(line_dict['HB'].split(' - ')[-1].split()[-1])
                # print('HB', set_or_tb)
                tiebreak_points = []
            elif line[:2] == 'HC':
                if not line_dict.get('HL'):
                    if line_dict.get('HD'):
                        continue
                    # print('HC', line_dict)
                    tiebreak_points.append((int(line_dict['HC']), int(line_dict['HE']), int(line_dict['HG'])))
                    continue
                # print(f'Game {game_num}')
                game_data = self.__parse_pbp_game_line(t24_match_id, line_dict, set_num, game_num, t1_bp, t2_bp)
                game_num += 1
                match_pbp_data_out.append(game_data)
        return match_pbp_data_out


if __name__ == '__main__':
    pars = T24matchPBPparser()
    pbp = 'HA÷Set 1¬HB÷Point by point - Set 1¬~HC÷1¬HE÷0¬HG÷1¬HK÷1¬HL÷0:15, 15:15, 30:15, 40:15, 40:30¬~HC÷1¬HE÷1¬HG÷2¬HK÷2¬HL÷15:0, 15:15, 15:30, 30:30, 30:40¬~HC÷2¬HE÷1¬HG÷1¬HK÷1¬HL÷15:0, 30:0, 30:15, 40:15, 40:30, 40:40, A:40¬~HC÷3¬HE÷1¬HG÷2¬HH÷2¬HK÷1¬HL÷0:15, 15:15, 30:15, 40:15 |B1|, 40:30 |B1|¬~HC÷4¬HE÷1¬HG÷1¬HK÷1¬HL÷0:15, 15:15, 30:15, 30:30, 40:30, 40:40, A:40¬~HC÷4¬HE÷2¬HG÷2¬HK÷2¬HL÷0:15, 0:30, 15:30, 30:30, 40:30 |B1|, 40:40, 40:A¬~HC÷5¬HE÷2¬HG÷1¬HK÷1¬HL÷15:0, 30:0, 40:0¬~HC÷5¬HE÷3¬HG÷2¬HK÷2¬HL÷0:15, 0:30, 0:40¬~HC÷6¬HE÷3¬HG÷1¬HK÷1¬HL÷15:0, 30:0, 40:0 |B2|, 40:15 |B2|¬~HA÷Set 2¬HB÷Point by point - Set 2¬~HC÷0¬HE÷1¬HG÷2¬HK÷2¬HL÷0:15, 0:30, 15:30, 15:40¬~HC÷1¬HE÷1¬HG÷1¬HK÷1¬HL÷15:0, 30:0, 40:0¬~HC÷1¬HE÷2¬HG÷2¬HK÷2¬HL÷0:15, 15:15, 15:30, 15:40¬~HC÷2¬HE÷2¬HG÷1¬HK÷1¬HL÷0:15, 15:15, 30:15, 40:15¬~HC÷2¬HE÷3¬HG÷2¬HK÷2¬HL÷0:15, 0:30, 0:40¬~HC÷3¬HE÷3¬HG÷1¬HK÷1¬HL÷15:0, 30:0, 30:15, 40:15, 40:30¬~HC÷3¬HE÷4¬HG÷2¬HK÷2¬HL÷0:15, 15:15, 15:30, 30:30, 30:40, 40:40, 40:A, 40:40, 40:A, 40:40, A:40 |B1|, 40:40, A:40 |B1|, 40:40, 40:A, 40:40, A:40 |B1|, 40:40, A:40 |B1|, 40:40, 40:A, 40:40, 40:A, 40:40, 40:A¬~HC÷4¬HE÷4¬HG÷1¬HK÷1¬HL÷0:15, 15:15, 15:30, 30:30, 40:30¬~HC÷4¬HE÷5¬HG÷2¬HK÷2¬HL÷0:15, 0:30, 0:40¬~HC÷5¬HE÷5¬HG÷1¬HK÷1¬HL÷0:15, 15:15, 30:15, 40:15¬~HC÷5¬HE÷6¬HG÷2¬HK÷2¬HL÷15:0, 30:0, 40:0 |B1|, 40:15 |B1|, 40:30 |B1|, 40:40, A:40 |B1|, 40:40, 40:A¬~HC÷6¬HE÷6¬HG÷1¬HK÷1¬HL÷0:15, 15:15, 30:15, 40:15¬~HC÷6¬HD÷2¬HE÷7¬HF÷7¬HK÷2¬~HB÷Tiebreak - Set 2¬~HC÷0¬HE÷1¬HG÷2¬HK÷2¬~HC÷0¬HE÷2¬HG÷1¬HH÷1¬HK÷2¬~HC÷1¬HE÷2¬HG÷1¬HK÷1¬~HC÷1¬HE÷3¬HG÷2¬HK÷2¬~HC÷1¬HE÷4¬HG÷2¬HK÷2¬~HC÷2¬HE÷4¬HG÷1¬HK÷1¬~HC÷2¬HE÷5¬HG÷1¬HH÷1¬HK÷2¬~HC÷2¬HE÷6¬HG÷2¬HK÷2¬HM÷2,2¬~HC÷2¬HE÷7¬HG÷2¬HK÷2¬~HA÷Set 3¬HB÷Point by point - Set 3¬~HC÷1¬HE÷0¬HG÷1¬HK÷1¬HL÷0:15, 0:30, 15:30, 30:30, 40:30, 40:40, 40:A |B1|, 40:40, A:40, 40:40, A:40, 40:40, A:40¬~HC÷1¬HE÷1¬HG÷2¬HK÷2¬HL÷15:0, 30:0, 30:15, 30:30, 30:40¬~HC÷1¬HE÷2¬HG÷1¬HH÷1¬HK÷2¬HL÷15:0, 30:0, 40:0, 40:15, 40:30, 40:40, A:40, 40:40, 40:A |B1|, 40:40, 40:A |B1|¬~HC÷2¬HE÷2¬HG÷2¬HH÷2¬HK÷1¬HL÷0:15, 0:30, 0:40, 15:40, 30:40, 40:40, A:40 |B1|¬~HC÷3¬HE÷2¬HG÷1¬HK÷1¬HL÷15:0, 30:0, 40:0, 40:15, 40:30, 40:40, A:40, 40:40, A:40¬~HC÷3¬HE÷3¬HG÷2¬HK÷2¬HL÷0:15, 0:30, 0:40¬~HC÷4¬HE÷3¬HG÷1¬HK÷1¬HL÷15:0, 15:15, 30:15, 40:15¬~HC÷4¬HE÷4¬HG÷2¬HK÷2¬HL÷0:15, 0:30, 0:40, 15:40¬~HC÷5¬HE÷4¬HG÷1¬HK÷1¬HL÷15:0, 15:15, 30:15, 40:15¬~HC÷5¬HE÷5¬HG÷2¬HK÷2¬HL÷15:0, 15:15, 15:30, 15:40, 30:40¬~HC÷6¬HE÷5¬HG÷1¬HK÷1¬HL÷0:15, 15:15, 30:15, 30:30, 40:30¬~HC÷6¬HE÷6¬HG÷2¬HK÷2¬HL÷0:15, 0:30, 0:40¬~HC÷7¬HD÷7¬HE÷6¬HF÷5¬HK÷1¬~HB÷Tiebreak - Set 3¬~HC÷1¬HE÷0¬HG÷1¬HK÷1¬~HC÷2¬HE÷0¬HG÷2¬HH÷2¬HK÷1¬~HC÷2¬HE÷1¬HG÷2¬HK÷2¬~HC÷2¬HE÷2¬HG÷1¬HH÷1¬HK÷2¬~HC÷3¬HE÷2¬HG÷1¬HK÷1¬~HC÷3¬HE÷3¬HG÷2¬HK÷2¬~HC÷3¬HE÷4¬HG÷2¬HK÷2¬~HC÷4¬HE÷4¬HG÷1¬HK÷1¬~HC÷5¬HE÷4¬HG÷1¬HK÷1¬~HC÷6¬HE÷4¬HG÷2¬HH÷2¬HK÷1¬HM÷3,1¬~HC÷6¬HE÷5¬HG÷2¬HK÷2¬HM÷3,1¬~HC÷7¬HE÷5¬HG÷1¬HK÷1¬~A1÷¬~'
    # pbp = 'HA÷Set 1¬HB÷Point by point - Set 1¬~HC÷1¬HE÷0¬HG÷1¬HK÷1¬HL÷15:0, 15:15, 15:30, 30:30, 40:30, 40:40, 40:A |B1|, 40:40, A:40¬~HC÷2¬HE÷0¬HG÷2¬HH÷2¬HK÷1¬HL÷0:15, 0:30, 15:30, 30:30, 40:30 |B1|¬~HC÷2¬HE÷1¬HG÷1¬HH÷1¬HK÷2¬HL÷15:0, 15:15, 30:15, 30:30, 30:40 |B1|, 40:40, 40:A |B1|¬~HC÷2¬HE÷2¬HG÷2¬HK÷2¬HL÷0:15, 0:30, 15:30, 15:40¬~HC÷3¬HE÷2¬HG÷1¬HK÷1¬HL÷0:15, 0:30, 15:30, 30:30, 40:30¬~HC÷3¬HE÷3¬HG÷2¬HK÷2¬HL÷0:15, 0:30, 0:40¬~HC÷4¬HE÷3¬HG÷1¬HK÷1¬HL÷15:0, 30:0, 40:0, 40:15, 40:30¬~HC÷5¬HE÷3¬HG÷2¬HH÷2¬HK÷1¬HL÷0:15, 15:15, 30:15, 40:15 |B1|, 40:30 |B1|¬~HC÷5¬HE÷4¬HG÷1¬HH÷1¬HK÷2¬HL÷0:15, 15:15, 15:30, 30:30, 40:30 |B2|, 40:40, A:40 |B2|, 40:40, 40:A |B1|¬~HC÷5¬HE÷5¬HG÷2¬HK÷2¬HL÷15:0, 15:15, 30:15, 30:30, 30:40¬~HC÷6¬HE÷5¬HG÷1¬HK÷1¬HL÷0:15, 15:15, 30:15, 30:30, 30:40 |B1|, 40:40, 40:A |B1|, 40:40, A:40¬~HC÷6¬HE÷6¬HG÷2¬HK÷2¬HL÷0:15, 15:15, 30:15, 30:30, 40:30 |B1| |B2|, 40:40, 40:A, 40:40, 40:A¬~HC÷6¬HD÷5¬HE÷7¬HF÷7¬HK÷2¬~HB÷Tiebreak - Set 1¬~HC÷1¬HE÷0¬HG÷1¬HK÷1¬~HC÷1¬HE÷1¬HG÷2¬HK÷2¬~HC÷1¬HE÷2¬HG÷2¬HK÷2¬~HC÷1¬HE÷3¬HG÷1¬HH÷1¬HK÷2¬~HC÷2¬HE÷3¬HG÷1¬HK÷1¬~HC÷2¬HE÷4¬HG÷2¬HK÷2¬~HC÷3¬HE÷4¬HG÷2¬HH÷2¬HK÷1¬~HC÷3¬HE÷5¬HG÷1¬HH÷1¬HK÷2¬~HC÷4¬HE÷5¬HG÷1¬HK÷1¬~HC÷4¬HE÷6¬HG÷2¬HK÷2¬HM÷2,2¬~HC÷5¬HE÷6¬HG÷2¬HH÷2¬HK÷1¬HM÷2,2¬~HC÷5¬HE÷7¬HG÷1¬HH÷1¬HK÷2¬~HA÷Set 2¬HB÷Point by point - Set 2¬~HC÷0¬HE÷1¬HG÷2¬HK÷2¬HL÷0:15, 0:30, 15:30, 15:40¬~HC÷1¬HE÷1¬HG÷1¬HK÷1¬HL÷15:0, 15:15, 30:15, 40:15¬~HC÷1¬HE÷2¬HG÷2¬HK÷2¬HL÷15:0, 30:0, 30:15, 40:15 |B1|, 40:30 |B1|, 40:40, 40:A¬~HC÷2¬HE÷2¬HG÷1¬HK÷1¬HL÷15:0, 15:15, 30:15, 30:30, 40:30¬~HC÷2¬HE÷3¬HG÷2¬HK÷2¬HL÷0:15, 15:15, 30:15, 30:30, 30:40¬~HC÷2¬HE÷4¬HG÷1¬HH÷1¬HK÷2¬HL÷15:0, 15:15, 15:30, 15:40 |B1|¬~HC÷2¬HE÷5¬HG÷2¬HK÷2¬HL÷0:15, 0:30, 0:40¬~HC÷2¬HE÷6¬HG÷1¬HH÷1¬HK÷2¬HL÷0:15, 0:30, 15:30, 30:30, 30:40 |B1| |B3|, 40:40, 40:A |B1| |B3|¬~A1÷f888b7be5f1fdbd4392724978492c320¬~'
    pars.parse_t24_pbp_string(pbp, 'AodtjydU')
