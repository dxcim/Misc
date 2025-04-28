import requests
import os
import unicodedata
import time

API_TOKEN = os.getenv('FOOTBALL_DATA_API_TOKEN', 'YOUR_API_TOKEN_HERE')
HEADERS = {'X-Auth-Token': API_TOKEN}
BASE_URL = 'https://api.football-data.org/v4'
COMPETITION_CODE = 'PL'
SEASON_YEAR = 2024


OUTPUT_FILE = 'insert_premier_league.sql'


def remove_accents(text):
    if not isinstance(text, str):
        return text

    return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')


def clean_data(obj):
    if isinstance(obj, dict):
        new_dict = {}
        for k, v in obj.items():
            new_key = remove_accents(k)
            new_value = clean_data(v)
            new_dict[new_key] = new_value
        return new_dict
    elif isinstance(obj, list):
        return [clean_data(item) for item in obj]
    elif isinstance(obj, str):
        return remove_accents(obj)
    else:
        return obj


def fetch_team_stats():
    time.sleep(6.5)
    url = f"{BASE_URL}/competitions/{COMPETITION_CODE}/standings"
    raw_data = requests.get(url, headers=HEADERS).json()
    data = clean_data(raw_data)
    table = data['standings'][0]['table']  # Overall table

    teams = []
    for entry in table:
        t = entry['team']
        team_id = t['id']
        time.sleep(6.5)
        raw_details = requests.get(f"{BASE_URL}/teams/{team_id}", headers=HEADERS).json()
        details = clean_data(raw_details)

        name = t['name'].replace("'", "''")
        venue = details.get('venue', '').replace("'", "''")

        teams.append({
            'team_id': team_id,
            'team_name': name,
            'team_founded_year': details.get('founded') or 'NULL',
            'team_home_stadium': venue,
            'team_matches_played': entry['playedGames'],
            'team_matches_won': entry['won'],
            'team_matches_drawn': entry['draw'],
            'team_matches_lost': entry['lost'],
            'team_goals_scored': entry['goalsFor'],
            'team_goals_conceded': entry['goalsAgainst'],
            'team_goal_difference': entry['goalDifference'],
            'team_points': entry['points'],
        })
    return teams


def fetch_matches():
    time.sleep(6.5)
    url = f"{BASE_URL}/competitions/{COMPETITION_CODE}/matches?season={SEASON_YEAR}&status=FINISHED"
    raw_data = requests.get(url, headers=HEADERS).json()
    data = clean_data(raw_data)

    matches = []
    for m in data['matches']:
        matches.append({
            'match_id': m['id'],
            'home_team_id': m['homeTeam']['id'],
            'away_team_id': m['awayTeam']['id'],
            'match_date': m['utcDate'][:10],  # YYYY-MM-DD
            'home_team_score': m['score']['fullTime']['home'],
            'away_team_score': m['score']['fullTime']['away'],
        })
    return matches


def fetch_squad_and_manager(team_id):
    time.sleep(6.5)
    url = f"{BASE_URL}/teams/{team_id}"
    raw_data = requests.get(url, headers=HEADERS).json()
    data = clean_data(raw_data)

    players = []
    for member in data.get('squad', []):
        full_name = member.get('name', '')
        name_parts = full_name.split()
        first = name_parts[0].replace("'", "''")
        last = ' '.join(name_parts[1:]).replace("'", "''") if len(name_parts) > 1 else ''

        nationality = member.get('nationality', '').replace("'", "''")
        position = member.get('position', '').replace("'", "''")

        players.append({
            'player_id': member['id'],
            'first_name': first,
            'last_name': last,
            'player_dob': member.get('dateOfBirth', 'NULL'),
            'player_nationality': nationality,
            'position': position,
            'team_id': team_id
        })

    managers = []
    coach = data.get('coach')
    if coach:
        contract = coach.get('contract', {})
        start = contract.get('start')
        hire_date = f"{start}-01" if start else 'NULL'

        first = coach.get('firstName', '').replace("'", "''")
        last = coach.get('lastName', '').replace("'", "''")
        nationality = coach.get('nationality', '').replace("'", "''")

        managers.append({
            'manager_id': coach['id'],
            'first_name': first,
            'last_name': last,
            'manager_nationality': nationality,
            'manager_hire_date': hire_date,
            'team_id': team_id
        })

    return players, managers


if __name__ == '__main__':
    teams = fetch_team_stats()
    matches = fetch_matches()

    with open(OUTPUT_FILE, 'w') as f:
        # Teams
        for t in teams:
            f.write(
                f"INSERT INTO teams (team_id, team_name, team_founded_year, team_home_stadium, team_matches_played, "
                f"team_matches_won, team_matches_drawn, team_matches_lost, team_goals_scored, team_goals_conceded, "
                f"team_goal_difference, team_points) VALUES ({t['team_id']}, '{t['team_name']}', {t['team_founded_year']}, "
                f"'{t['team_home_stadium']}', {t['team_matches_played']}, {t['team_matches_won']}, {t['team_matches_drawn']}, "
                f"{t['team_matches_lost']}, {t['team_goals_scored']}, {t['team_goals_conceded']}, {t['team_goal_difference']}, "
                f"{t['team_points']});\n"
            )

        all_player_ids = set()
        for t in teams:
            players, managers = fetch_squad_and_manager(t['team_id'])
            for p in players:
                if p['player_id'] in all_player_ids:
                    continue
                all_player_ids.add(p['player_id'])
                f.write(
                    f"INSERT INTO players (player_id, first_name, last_name, player_dob, player_nationality, position, team_id) "
                    f"VALUES ({p['player_id']}, '{p['first_name']}', '{p['last_name']}', '{p['player_dob']}', "
                    f"'{p['player_nationality']}', '{p['position']}', {p['team_id']});\n"
                )
            for m in managers:
                f.write(
                    f"INSERT INTO managers (manager_id, first_name, last_name, manager_nationality, manager_hire_date, team_id) "
                    f"VALUES ({m['manager_id']}, '{m['first_name']}', '{m['last_name']}', '{m['manager_nationality']}', "
                    f"'{m['manager_hire_date']}', {m['team_id']});\n"
                )

        for m in matches:
            f.write(
                f"INSERT INTO matches (match_id, home_team_id, away_team_id, match_date, home_team_score, away_team_score) "
                f"VALUES ({m['match_id']}, {m['home_team_id']}, {m['away_team_id']}, '{m['match_date']}', "
                f"{m['home_team_score']}, {m['away_team_score']});\n"
            )

    print(f"SQL insert statements written to {OUTPUT_FILE}")
